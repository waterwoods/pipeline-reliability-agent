"""Two-scenario Bedrock demo. Fake Converse client. No AWS calls.

    python -m pipeline_reliability.bedrock_demo
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field

from pipeline_reliability.agent import Agent, decide, guard
from pipeline_reliability.bedrock import (
    BedrockFactIntelligence,
    TaskLog,
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
    TraceEvent,
)

_LOG = TaskLog(
    error_type="VendorTimeout",
    message="Worker hung for 47 minutes and the task was marked failed.",
    detail="No stable source marker. Polling stopped before a terminal warehouse state.",
)

_TIMEOUT_FACTS: dict[str, object] = {
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
    "evidence": "worker hung for 47 minutes; this text is not an action",
}

_MODEL_ID = "demo-bedrock-model"


class _FakeBedrockRuntime:
    def __init__(self, payload: dict[str, object]) -> None:
        self._text = json.dumps(payload)
        self.calls: list[dict[str, object]] = []

    def converse(self, **kwargs: object) -> dict[str, object]:
        if "toolConfig" in kwargs or "tools" in kwargs:
            raise RuntimeError("demo client refuses tool configuration")
        self.calls.append(kwargs)
        return {
            "output": {
                "message": {
                    "role": "assistant",
                    "content": [{"text": self._text}],
                }
            },
            "stopReason": "end_turn",
        }


@dataclass
class _DemoAdapter:
    committed_after_retry: bool = True
    committed: bool = False
    retry_calls: list[str] = field(default_factory=list)

    def inspect_commit(self, state: IncidentState) -> CommitStatus:
        return CommitStatus.COMMITTED if self.committed else CommitStatus.EMPTY

    def retry(self, state: IncidentState) -> None:
        self.retry_calls.append("RETRY")
        if self.committed_after_retry:
            self.committed = True


def _intelligence() -> tuple[BedrockFactIntelligence, _FakeBedrockRuntime]:
    client = _FakeBedrockRuntime(_TIMEOUT_FACTS)
    return BedrockFactIntelligence(client=client, model=_MODEL_ID), client


def _print_trace(trace: list[TraceEvent]) -> None:
    print("trace:")
    for event in trace:
        if event.phase not in {"Decide", "Guard", "Execute"}:
            continue
        extra = ""
        if event.phase == "Guard":
            extra = f" allowed={event.fields.get('allowed')}"
        print(f"  {event.phase} {event.action}{extra} reason={event.detail}")


def scenario_a() -> None:
    print("SCENARIO A — Bedrock facts, then deterministic Decide/Guard")
    intelligence, client = _intelligence()
    proposal = intelligence.propose_facts(_LOG)
    validation = validate_fact_proposal(proposal)
    state = IncidentState("incident-a", "daily_orders", "run-a")
    record_fact_evidence(state, proposal)

    adapter = _DemoAdapter()
    leases = LeaseStore()
    claim = leases.claim(state.incident_id, "demo-worker", now=0, ttl=30)
    assert claim is not None
    trace = Agent(adapter, leases).run(state, claim, now=0)
    retry_steps = [
        event
        for event in trace
        if event.phase == "Guard" and event.action == Action.RETRY
    ]

    print(f"model_provider={intelligence.provider}")
    print(f"model_id={intelligence.model}")
    print(f"bedrock_converse_calls={len(client.calls)}")
    print(f"proposed_failure_type={proposal.failure_type}")
    print(f"proposed_facts={proposal.facts}")
    print(
        f"validation={'accepted' if validation.accepted else 'rejected'} "
        f"reason={validation.reason}"
    )
    print(f"retry_tool_executions={len(adapter.retry_calls)}")
    print(f"final_outcome={state.outcome}")
    _print_trace(trace)
    print("note=Bedrock wrote facts. Decide proposed RETRY. Guard allowed one retry.")
    print()

    assert validation.accepted
    assert proposal.facts == {"error": "timeout"}
    assert "toolConfig" not in client.calls[0]
    assert len(retry_steps) == 1 and retry_steps[0].fields.get("allowed") is True
    assert adapter.retry_calls == ["RETRY"]
    assert state.outcome == "STOP_SAFE"


def scenario_b() -> None:
    print("SCENARIO B — UNKNOWN side effect, no second RETRY")
    intelligence, client = _intelligence()
    proposal = intelligence.propose_facts(_LOG)
    validation = validate_fact_proposal(proposal)
    state = IncidentState(
        "incident-b",
        "daily_orders",
        "run-b",
        retry_effect=EffectStatus.UNKNOWN,
        retry_count=1,
        reconcile_required=True,
    )
    record_fact_evidence(state, proposal)
    assert state.retry_effect == EffectStatus.UNKNOWN

    leases = LeaseStore()
    claim = leases.claim(state.incident_id, "demo-worker", now=0, ttl=30)
    assert claim is not None
    proposed = decide(state)
    forced = Decision(Action.RETRY, "model tried to choose RETRY")
    denied = guard(state, forced, claim=claim, leases=leases, now=0)

    adapter = _DemoAdapter(committed_after_retry=False)
    trace = Agent(adapter, leases).run(state, claim, now=0)
    actions = [event.action for event in trace if event.phase == "Decide"]

    print(f"model_provider={intelligence.provider}")
    print(f"model_id={intelligence.model}")
    print(f"bedrock_converse_calls={len(client.calls)}")
    print(f"proposed_failure_type={proposal.failure_type}")
    print(f"proposed_facts={proposal.facts}")
    print(
        f"validation={'accepted' if validation.accepted else 'rejected'} "
        f"reason={validation.reason}"
    )
    print(f"retry_effect_before_loop=UNKNOWN")
    print(f"decide_before_reconcile={proposed.action}")
    verdict = "ALLOW" if denied.allowed else "DENY"
    print(f"guard_if_RETRY_were_proposed={verdict} reason={denied.reason}")
    print(f"decide_actions={[action.value for action in actions]}")
    print(f"retry_tool_executions={len(adapter.retry_calls)}")
    print(f"final_outcome={state.outcome}")
    _print_trace(trace)
    print(
        "note=Timeout facts were accepted. UNKNOWN is not NOT_EXECUTED. "
        "No second RETRY. The run asked a human."
    )
    print()

    assert validation.accepted
    assert proposed.action == Action.RECONCILE
    assert denied.allowed is False
    assert "unreconciled" in denied.reason
    assert Action.RETRY not in actions
    assert Action.ASK_HUMAN in actions
    assert adapter.retry_calls == []
    assert state.outcome == "HITL_REQUIRED"


def main() -> None:
    print("Pipeline Reliability Agent — Bedrock fact intelligence demo")
    print("client=fake  network=no  tools=none")
    print("path=Bedrock → FactProposal → Validator → Decide → Guard → Execute")
    print("Bedrock provides intelligence, not authority.")
    print()
    scenario_a()
    scenario_b()


if __name__ == "__main__":
    main()
