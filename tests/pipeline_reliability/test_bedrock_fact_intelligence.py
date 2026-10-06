"""Bedrock FactIntelligence tests.

Fake Converse client only. No network. Bedrock proposes facts; validator,
Decide, and Guard keep authority.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field

import pytest

from pipeline_reliability.adapters import (
    DownstreamImpactResult,
    OrchestratorRunResult,
    RetryResult,
    TaskLogResult,
    WarehouseJobResult,
)
from pipeline_reliability.apply import apply
from pipeline_reliability.bedrock_intelligence import (
    BedrockFactIntelligence,
    fact_proposal_output_config,
)
from pipeline_reliability.decide import GET_TASK_LOG, RETRY
from pipeline_reliability.facts import TIMEOUT, validate_fact_proposal
from pipeline_reliability.guard import guard
from pipeline_reliability.intelligence import OpenAIFactIntelligence
from pipeline_reliability.llm_provider import (
    LlmProviderConfigError,
    fact_intelligence_from_environ,
)
from pipeline_reliability.observability import RunTrace
from pipeline_reliability.runner import run_agent
from pipeline_reliability.state import PipelineReliabilityState


_UNSTRUCTURED_LOG = TaskLogResult(
    error_type="VendorTimeout",
    message="Worker hung for 47 minutes and the task was marked failed.",
    detail="No stable source or schema marker. Polling stopped before a terminal warehouse state.",
)

_TIMEOUT_PAYLOAD: dict[str, object] = {
    "failure_type": TIMEOUT,
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
    """SDK-shaped fake: client.converse(**kwargs). No network."""

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


def _intelligence(
    payload: dict[str, object] | None = None,
    **fake_kwargs: object,
) -> tuple[BedrockFactIntelligence, _FakeBedrockRuntime]:
    client = _FakeBedrockRuntime(payload, **fake_kwargs)  # type: ignore[arg-type]
    return BedrockFactIntelligence(client=client, model="anthropic.example-v1"), client


def _blank_state(**overrides: object) -> PipelineReliabilityState:
    state = PipelineReliabilityState(pipeline="mdp_daily_orders", error="")
    for key, value in overrides.items():
        setattr(state, key, value)
    return state


def _assert_untouched(state: PipelineReliabilityState) -> None:
    assert state.error == ""
    assert state.outcome is None
    assert state.retries == 0
    assert state.approved_action is None
    assert state.suggested_action is None
    assert state.awaiting_human is False


@dataclass
class _CountingAdapter:
    retry_calls: list[str] = field(default_factory=list)
    log_calls: int = 0

    def check_orchestrator_run(self, state: PipelineReliabilityState) -> OrchestratorRunResult:
        return OrchestratorRunResult(
            status="FAILED",
            failed_task_id="load_orders",
            detail="orchestrator reported the task failed",
        )

    def get_task_log(self, state: PipelineReliabilityState) -> TaskLogResult:
        self.log_calls += 1
        return _UNSTRUCTURED_LOG

    def check_warehouse_job(self, state: PipelineReliabilityState) -> WarehouseJobResult:
        return WarehouseJobResult(
            status="FAILED",
            rows_written=0,
            detail="warehouse load failed before any rows were committed",
        )

    def check_downstream_impact(self, state: PipelineReliabilityState) -> DownstreamImpactResult:
        return DownstreamImpactResult(
            impact="low",
            affected_assets=["orders_daily"],
            detail="no critical dashboard depends on this run",
        )

    def retry_orchestrator_task(self, state: PipelineReliabilityState) -> RetryResult:
        self.retry_calls.append(state.task_id or "load_orders")
        return RetryResult(
            accepted=True,
            pipeline_id=state.pipeline,
            task_id=state.task_id or "load_orders",
            detail="retry accepted by the fake orchestrator",
            orchestrator_status="RUNNING",
        )


def test_valid_converse_response_becomes_fact_proposal() -> None:
    intelligence, client = _intelligence(_TIMEOUT_PAYLOAD)

    proposal = intelligence.propose_facts(_UNSTRUCTURED_LOG)

    validation = validate_fact_proposal(proposal)
    assert validation.accepted is True
    assert proposal.failure_type == TIMEOUT
    assert proposal.facts == {"error": "timeout"}
    assert proposal.confidence == 0.91
    assert proposal.evidence == "worker hung for 47 minutes"
    assert intelligence.provider == "bedrock"
    assert intelligence.model == "anthropic.example-v1"
    assert len(client.calls) == 1
    call = client.calls[0]
    assert call["modelId"] == "anthropic.example-v1"
    assert call["outputConfig"] == fact_proposal_output_config()
    assert "toolConfig" not in call
    assert "tools" not in call


def test_malformed_json_fails_safely_and_does_not_write_state() -> None:
    intelligence, _client = _intelligence(text="not-json {RETRY")
    state = _blank_state()

    with pytest.raises(ValueError, match="failed to produce a FactProposal"):
        intelligence.propose_facts(_UNSTRUCTURED_LOG)

    apply(state, GET_TASK_LOG, _UNSTRUCTURED_LOG, intelligence=intelligence)
    _assert_untouched(state)

    missing, _missing_client = _intelligence(
        text=json.dumps({"failure_type": TIMEOUT, "confidence": 0.9})
    )
    missing_state = _blank_state()
    apply(missing_state, GET_TASK_LOG, _UNSTRUCTURED_LOG, intelligence=missing)
    _assert_untouched(missing_state)


def test_bedrock_exception_fails_safely_and_does_not_retry() -> None:
    intelligence, _client = _intelligence(error=TimeoutError("bedrock read timed out"))
    state = _blank_state(run_id="demo-timeout", task_id="load_orders")
    adapter = _CountingAdapter()

    with pytest.raises(ValueError, match="failed to produce a FactProposal"):
        intelligence.propose_facts(_UNSTRUCTURED_LOG)

    result = run_agent(state, adapter=adapter, intelligence=intelligence)

    assert adapter.retry_calls == []
    assert all(step.action != RETRY for step in result.trace.steps)
    assert result.state.outcome == "ASK_HUMAN"
    assert result.state.error == ""
    assert result.state.retries == 0


def test_retry_and_suggested_action_are_rejected_by_existing_validator() -> None:
    payload = {
        "failure_type": TIMEOUT,
        "facts": {
            "error": "timeout",
            "suggested_action": "RETRY",
        },
        "confidence": 0.95,
        "evidence": "worker hung",
        "RETRY": "execute now",
        "authorization": "approved",
    }
    intelligence, _client = _intelligence(payload)
    proposal = intelligence.propose_facts(_UNSTRUCTURED_LOG)

    validation = validate_fact_proposal(proposal)
    assert validation.accepted is False
    assert "suggested_action" in validation.reason or "RETRY" in validation.reason

    state = _blank_state()
    apply(state, GET_TASK_LOG, _UNSTRUCTURED_LOG, intelligence=intelligence)
    _assert_untouched(state)
    denied = guard(state, RETRY)
    assert denied.allowed is False


def test_unknown_side_effect_blocks_retry_after_accepted_timeout_facts() -> None:
    intelligence, _client = _intelligence(_TIMEOUT_PAYLOAD)
    state = _blank_state(
        orchestrator_status="FAILED",
        warehouse_status="FAILED",
        rows_written=0,
        downstream_impact="low",
        retry_side_effect="UNKNOWN",
        task_id="load_orders",
    )
    trace = RunTrace()
    apply(state, GET_TASK_LOG, _UNSTRUCTURED_LOG, intelligence=intelligence, trace=trace)

    assert state.error == "timeout"
    assert state.retry_side_effect == "UNKNOWN"
    assert trace.intelligence_events[0].event_type == "LLM_PROPOSAL"
    assert trace.intelligence_events[1].accepted is True

    denied = guard(state, RETRY)
    assert denied.allowed is False
    assert "UNKNOWN" in denied.reason

    adapter = _CountingAdapter()
    result = run_agent(state, adapter=adapter, intelligence=intelligence, trace=trace)

    assert result.state.outcome == "ASK_HUMAN"
    assert adapter.retry_calls == []
    assert adapter.log_calls == 0
    assert all(step.action != RETRY for step in result.trace.steps)


def test_llm_provider_none_keeps_the_deterministic_path() -> None:
    assert fact_intelligence_from_environ({}) is None
    assert fact_intelligence_from_environ({"LLM_PROVIDER": "none"}) is None
    assert fact_intelligence_from_environ({"LLM_PROVIDER": "NONE"}) is None

    state = _blank_state(run_id="no-llm", task_id="load_orders")
    adapter = _CountingAdapter()
    result = run_agent(state, adapter=adapter, intelligence=fact_intelligence_from_environ({}))

    assert result.trace.intelligence_events == []
    assert adapter.retry_calls == []
    assert result.state.error == ""
    assert result.state.outcome == "ASK_HUMAN"


def test_openai_provider_still_uses_injected_openai_intelligence() -> None:
    class _FakeCompletions:
        def __init__(self) -> None:
            self.calls = 0

        def create(self, **kwargs: object) -> object:
            self.calls += 1
            content = json.dumps(_TIMEOUT_PAYLOAD)
            message = type("Message", (), {"content": content})()
            choice = type("Choice", (), {"message": message})()
            return type("Response", (), {"choices": [choice]})()

    completions = _FakeCompletions()
    client = type(
        "Client",
        (),
        {"chat": type("Chat", (), {"completions": completions})()},
    )()
    intelligence = fact_intelligence_from_environ(
        {"LLM_PROVIDER": "openai", "OPENAI_MODEL": "gpt-4o-mini"},
        openai_client=client,
    )
    assert isinstance(intelligence, OpenAIFactIntelligence)
    proposal = intelligence.propose_facts(_UNSTRUCTURED_LOG)
    assert validate_fact_proposal(proposal).accepted is True
    assert completions.calls == 1


def test_bedrock_factory_does_not_read_secret_env_and_requires_model() -> None:
    created: list[str] = []

    def _factory(region: str) -> _FakeBedrockRuntime:
        created.append(region)
        return _FakeBedrockRuntime(_TIMEOUT_PAYLOAD)

    with pytest.raises(LlmProviderConfigError, match="BEDROCK_MODEL_ID"):
        fact_intelligence_from_environ(
            {
                "LLM_PROVIDER": "bedrock",
                "AWS_REGION": "us-west-2",
                "AWS_SECRET_ACCESS_KEY": "do-not-read",
            },
            bedrock_client_factory=_factory,
        )
    assert created == []

    intelligence = fact_intelligence_from_environ(
        {
            "LLM_PROVIDER": "bedrock",
            "BEDROCK_MODEL_ID": "anthropic.example-v1",
            "BEDROCK_REGION": "us-west-2",
            "AWS_REGION": "us-east-1",
            "AWS_ACCESS_KEY_ID": "do-not-read",
            "AWS_SECRET_ACCESS_KEY": "do-not-read",
            "AWS_SESSION_TOKEN": "do-not-read",
        },
        bedrock_client_factory=_factory,
    )
    assert isinstance(intelligence, BedrockFactIntelligence)
    assert created == ["us-west-2"]
    assert intelligence.model == "anthropic.example-v1"
    proposal = intelligence.propose_facts(_UNSTRUCTURED_LOG)
    assert proposal.facts == {"error": "timeout"}


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

    intelligence = BedrockFactIntelligence(client=_ToolClient(), model="anthropic.example-v1")
    state = _blank_state()
    apply(state, GET_TASK_LOG, _UNSTRUCTURED_LOG, intelligence=intelligence)
    _assert_untouched(state)
