"""Bedrock fact adapter tests.

Fake Converse client only. No network. The model proposes facts; the
validator, Decide, and Guard keep authority.
"""

from __future__ import annotations

import json

import pytest

from pipeline_reliability.agent import Agent, decide, guard
from pipeline_reliability.bedrock import (
    BedrockFactIntelligence,
    TaskLog,
    fact_proposal_output_config,
    record_fact_evidence,
    validate_fact_proposal,
)
from pipeline_reliability.coordination import LeaseStore
from pipeline_reliability.model import (
    Action,
    CommitStatus,
    Decision,
    EffectStatus,
    IncidentState,
)


_LOG = TaskLog(
    error_type="VendorTimeout",
    message="Worker hung for 47 minutes and the task was marked failed.",
    detail="No stable source marker. Polling stopped before a terminal warehouse state.",
)

_TIMEOUT_PAYLOAD: dict[str, object] = {
    "failure_type": "TIMEOUT",
    "facts": {
        "file_present": None,
        "expected_object": None,
        "observed_rows": None,
        "volume_status": None,
        "expected_delimiter": None,
        "observed_delimiter": None,
        "error": "timeout",
    },
    "confidence": 0.91,
    "evidence": "worker hung for 47 minutes",
}


class _FakeBedrockRuntime:
    def __init__(
        self,
        payload: dict[str, object] | None = None,
        *,
        text: str | None = None,
        error: Exception | None = None,
    ) -> None:
        self._error = error
        if text is None and payload is not None:
            text = json.dumps(payload)
        self._text = text
        self.calls: list[dict[str, object]] = []

    def converse(self, **kwargs: object) -> dict[str, object]:
        self.calls.append(kwargs)
        if self._error is not None:
            raise self._error
        return {
            "output": {
                "message": {
                    "role": "assistant",
                    "content": [{"text": self._text}],
                }
            },
            "stopReason": "end_turn",
        }


class _RetryAdapter:
    def __init__(self, *, commit_on_retry: bool) -> None:
        self.commit_on_retry = commit_on_retry
        self.committed = False
        self.retry_calls = 0

    def inspect_commit(self, state: IncidentState) -> CommitStatus:
        return CommitStatus.COMMITTED if self.committed else CommitStatus.EMPTY

    def retry(self, state: IncidentState) -> None:
        self.retry_calls += 1
        if self.commit_on_retry:
            self.committed = True


def _intelligence(
    payload: dict[str, object] | None = None,
    **fake_kwargs: object,
) -> tuple[BedrockFactIntelligence, _FakeBedrockRuntime]:
    client = _FakeBedrockRuntime(payload, **fake_kwargs)  # type: ignore[arg-type]
    return BedrockFactIntelligence(client=client, model="example.bedrock-model"), client


def _blank() -> IncidentState:
    return IncidentState("incident", "daily_orders", "run")


def test_valid_converse_response_becomes_fact_proposal() -> None:
    intelligence, client = _intelligence(_TIMEOUT_PAYLOAD)

    proposal = intelligence.propose_facts(_LOG)
    validation = validate_fact_proposal(proposal)

    assert validation.accepted
    assert proposal.failure_type == "TIMEOUT"
    assert proposal.facts == {"error": "timeout"}
    assert proposal.confidence == 0.91
    assert intelligence.provider == "bedrock"
    assert len(client.calls) == 1
    call = client.calls[0]
    assert call["modelId"] == "example.bedrock-model"
    assert call["outputConfig"] == fact_proposal_output_config()
    assert "toolConfig" not in call
    assert "tools" not in call


def test_malformed_or_failed_response_does_not_write_state() -> None:
    broken, _client = _intelligence(text="not-json {RETRY")
    state = _blank()
    with pytest.raises(ValueError, match="failed to produce a FactProposal"):
        broken.propose_facts(_LOG)
    assert state.evidence == []
    assert state.outcome is None
    assert state.retry_effect == EffectStatus.NONE

    timed_out, _client = _intelligence(error=TimeoutError("bedrock read timed out"))
    with pytest.raises(ValueError, match="failed to produce a FactProposal"):
        timed_out.propose_facts(_LOG)
    assert state.retry_count == 0


def test_retry_and_backfill_are_rejected_by_the_validator() -> None:
    payload = {
        "failure_type": "TIMEOUT",
        "facts": {"error": "timeout", "suggested_action": "RETRY"},
        "confidence": 0.95,
        "evidence": "worker hung",
        "RETRY": "execute now",
        "BACKFILL": "partition dt=2026-09-25",
        "authorization": "approved",
    }
    intelligence, _client = _intelligence(payload)
    proposal = intelligence.propose_facts(_LOG)
    validation = validate_fact_proposal(proposal)

    assert validation.accepted is False
    assert any(token in validation.reason for token in ("RETRY", "BACKFILL", "suggested_action"))

    state = _blank()
    with pytest.raises(ValueError):
        record_fact_evidence(state, proposal)
    assert state.evidence == []
    assert state.outcome is None

    leases = LeaseStore()
    claim = leases.claim(state.incident_id, "worker", now=0, ttl=30)
    assert claim is not None
    denied = guard(
        state,
        Decision(Action.RETRY, "model tried to choose RETRY"),
        claim=claim,
        leases=leases,
        now=0,
    )
    assert denied.allowed is False


def test_accepted_facts_then_guard_allows_one_deterministic_retry() -> None:
    intelligence, _client = _intelligence(_TIMEOUT_PAYLOAD)
    proposal = intelligence.propose_facts(_LOG)
    state = _blank()
    record_fact_evidence(state, proposal)
    assert state.retry_effect == EffectStatus.NONE
    assert state.outcome is None

    adapter = _RetryAdapter(commit_on_retry=True)
    leases = LeaseStore()
    claim = leases.claim(state.incident_id, "worker", now=0, ttl=30)
    assert claim is not None
    trace = Agent(adapter, leases).run(state, claim, now=0)

    retry_guards = [
        event
        for event in trace
        if event.phase == "Guard" and event.action == Action.RETRY
    ]
    assert len(retry_guards) == 1
    assert retry_guards[0].fields["allowed"] is True
    assert adapter.retry_calls == 1
    assert state.outcome == "STOP_SAFE"


def test_unknown_side_effect_blocks_a_second_retry_and_asks_human() -> None:
    intelligence, _client = _intelligence(_TIMEOUT_PAYLOAD)
    proposal = intelligence.propose_facts(_LOG)
    assert validate_fact_proposal(proposal).accepted

    state = IncidentState(
        "incident",
        "daily_orders",
        "run",
        retry_effect=EffectStatus.UNKNOWN,
        retry_count=1,
        reconcile_required=True,
    )
    record_fact_evidence(state, proposal)
    assert state.retry_effect == EffectStatus.UNKNOWN

    leases = LeaseStore()
    claim = leases.claim("incident", "worker", now=0, ttl=30)
    assert claim is not None
    assert decide(state).action == Action.RECONCILE
    denied = guard(
        state,
        Decision(Action.RETRY, "model tried to choose RETRY"),
        claim=claim,
        leases=leases,
        now=0,
    )
    assert denied.allowed is False
    assert "unreconciled" in denied.reason

    adapter = _RetryAdapter(commit_on_retry=False)
    trace = Agent(adapter, leases).run(state, claim, now=0)
    decisions = [event.action for event in trace if event.phase == "Decide"]

    assert Action.RETRY not in decisions
    assert Action.ASK_HUMAN in decisions
    assert adapter.retry_calls == 0
    assert state.outcome == "HITL_REQUIRED"


def test_tool_use_response_fails_safely() -> None:
    class _ToolClient:
        def converse(self, **kwargs: object) -> dict[str, object]:
            assert "toolConfig" not in kwargs
            return {
                "output": {
                    "message": {
                        "content": [
                            {
                                "toolUse": {
                                    "name": "RETRY",
                                    "input": {"task_id": "load_orders"},
                                }
                            }
                        ]
                    }
                }
            }

    intelligence = BedrockFactIntelligence(client=_ToolClient(), model="example.bedrock-model")
    state = _blank()
    with pytest.raises(ValueError, match="failed to produce a FactProposal"):
        intelligence.propose_facts(_LOG)
    assert state.evidence == []
    assert state.retry_count == 0
    assert state.outcome is None
