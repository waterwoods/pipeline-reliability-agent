"""Two-minute Bedrock reliability demo. Fake Converse client. No AWS calls.

Bedrock proposes facts. The existing validator, Decide, and Guard keep authority.
The model is not given tools, and this script never calls a mutation itself.

    python scripts/bedrock_reliability_demo.py
"""

from __future__ import annotations

import contextlib
import io
import json
from dataclasses import dataclass, field

from pipeline_reliability.adapters import (
    DownstreamImpactResult,
    OrchestratorRunResult,
    RetryResult,
    TaskLogResult,
    WarehouseJobResult,
)
from pipeline_reliability.apply import apply
from pipeline_reliability.bedrock_intelligence import BedrockFactIntelligence
from pipeline_reliability.decide import GET_TASK_LOG, RETRY
from pipeline_reliability.facts import TIMEOUT, validate_fact_proposal
from pipeline_reliability.guard import guard
from pipeline_reliability.observability import RunTrace
from pipeline_reliability.runner import AgentRunResult, run_agent
from pipeline_reliability.state import PipelineReliabilityState


_LOG = TaskLogResult(
    error_type="VendorTimeout",
    message="Worker hung for 47 minutes and the task was marked failed.",
    detail="No stable source or schema marker. Polling stopped before a terminal warehouse state.",
)

_TIMEOUT_FACTS: dict[str, object] = {
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
    "evidence": "worker hung for 47 minutes; this text is not an action",
}

_MODEL_ID = "demo-bedrock-model"


class _FakeBedrockRuntime:
    def __init__(self, payload: dict[str, object]) -> None:
        self._text = json.dumps(payload)
        self.calls = 0

    def converse(self, **kwargs: object) -> dict[str, object]:
        if "toolConfig" in kwargs or "tools" in kwargs:
            raise RuntimeError("demo client refuses tool configuration")
        self.calls += 1
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
    retry_calls: list[str] = field(default_factory=list)

    def check_orchestrator_run(self, state: PipelineReliabilityState) -> OrchestratorRunResult:
        return OrchestratorRunResult(
            status="FAILED",
            failed_task_id="load_orders",
            detail="orchestrator reported the task failed",
        )

    def get_task_log(self, state: PipelineReliabilityState) -> TaskLogResult:
        return _LOG

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
        self.retry_calls.append("RETRY")
        return RetryResult(
            accepted=True,
            pipeline_id=state.pipeline,
            task_id="load_orders",
            detail="fake orchestrator accepted one retry",
            orchestrator_status="RUNNING",
        )


def _intelligence() -> tuple[BedrockFactIntelligence, _FakeBedrockRuntime]:
    client = _FakeBedrockRuntime(_TIMEOUT_FACTS)
    return BedrockFactIntelligence(client=client, model=_MODEL_ID), client


def _run(
    state: PipelineReliabilityState,
    adapter: _DemoAdapter,
    intelligence: BedrockFactIntelligence,
    trace: RunTrace | None = None,
) -> AgentRunResult:
    sink = io.StringIO()
    with contextlib.redirect_stdout(sink):
        return run_agent(state, adapter=adapter, intelligence=intelligence, trace=trace)


def _print_trace(result: AgentRunResult) -> None:
    print("intelligence events:")
    if not result.trace.intelligence_events:
        print("  (none)")
    for event in result.trace.intelligence_events:
        facts = event.proposal.facts if event.proposal is not None else {}
        accepted = "" if event.accepted is None else f" accepted={event.accepted}"
        print(f"  {event.event_type}{accepted} facts={facts}")
    print("trace:")
    for step in result.trace.steps:
        verdict = "ALLOW" if step.guard_allowed else "DENY"
        print(f"  {step.action} guard={verdict} reason={step.guard_reason}")
    print(f"final_outcome={result.state.outcome}")


def scenario_a() -> None:
    print("SCENARIO A — INTELLIGENCE HELPS")
    intelligence, client = _intelligence()
    adapter = _DemoAdapter()
    state = PipelineReliabilityState(
        pipeline="mdp_daily_orders",
        error="",
        run_id="demo-a",
        task_id="load_orders",
    )
    result = _run(state, adapter, intelligence)
    proposal = result.trace.intelligence_events[0].proposal
    assert proposal is not None
    validation = validate_fact_proposal(proposal)
    retry_steps = [step for step in result.trace.steps if step.action == RETRY]
    print(f"model_provider={intelligence.provider}")
    print(f"model_id={intelligence.model}")
    print(f"bedrock_converse_calls={client.calls}")
    print(f"proposed_failure_type={proposal.failure_type}")
    print(f"proposed_facts={proposal.facts}")
    print(
        f"validation={'accepted' if validation.accepted else 'rejected'} "
        f"reason={validation.reason}"
    )
    print(f"decide_retry_proposals={len(retry_steps)}")
    if retry_steps:
        step = retry_steps[0]
        verdict = "ALLOW" if step.guard_allowed else "DENY"
        print(f"guard_on_retry={verdict} reason={step.guard_reason}")
    print(f"retry_tool_executions={len(adapter.retry_calls)}")
    _print_trace(result)
    print("note=Bedrock wrote facts. Decide proposed RETRY. Guard allowed the one safe retry.")
    print()


def scenario_b() -> None:
    print("SCENARIO B — MODEL CANNOT OVERRIDE SAFETY")
    intelligence, client = _intelligence()
    state = PipelineReliabilityState(
        pipeline="mdp_daily_orders",
        error="",
        orchestrator_status="FAILED",
        warehouse_status="FAILED",
        rows_written=0,
        downstream_impact="low",
        retry_side_effect="UNKNOWN",
        run_id="demo-b",
        task_id="load_orders",
    )
    trace = RunTrace()
    sink = io.StringIO()
    with contextlib.redirect_stdout(sink):
        apply(state, GET_TASK_LOG, _LOG, intelligence=intelligence, trace=trace)
    proposal = trace.intelligence_events[0].proposal
    assert proposal is not None
    validation = validate_fact_proposal(proposal)
    denied = guard(state, RETRY)
    adapter = _DemoAdapter()
    result = _run(state, adapter, intelligence, trace)
    print(f"model_provider={intelligence.provider}")
    print(f"model_id={intelligence.model}")
    print(f"bedrock_converse_calls={client.calls}")
    print(f"proposed_failure_type={proposal.failure_type}")
    print(f"proposed_facts={proposal.facts}")
    print(
        f"validation={'accepted' if validation.accepted else 'rejected'} "
        f"reason={validation.reason}"
    )
    print(f"state.retry_side_effect={result.state.retry_side_effect}")
    print("decide_proposed_RETRY=no")
    verdict = "ALLOW" if denied.allowed else "DENY"
    print(f"guard_if_RETRY_were_proposed={verdict} reason={denied.reason}")
    print(f"retry_tool_executions={len(adapter.retry_calls)}")
    _print_trace(result)
    print("note=Timeout facts were accepted. UNKNOWN still blocked a second RETRY.")
    print()


def main() -> None:
    print("Pipeline Reliability Agent — Bedrock fact intelligence demo")
    print("client=fake  network=no  tools=none")
    print("path=Bedrock → FactProposal → Validator → Decide → Guard → Execute")
    print()
    scenario_a()
    scenario_b()


if __name__ == "__main__":
    main()
