"""Focused proofs: Decide proposes RETRY, existing Guard Rule 2b blocks it.

Incident shape (existing rules only — no new Guard/Decide policy):

    error == timeout, retries < 1, Airflow FAILED, downstream not critical
        → Decide proposes RETRY  (timeout first-retry playbook)

    rows_written > 0 and warehouse_status != SUCCEEDED
        → Guard rejects RETRY    (Rule 2b: possible partial write)

Runner must skip Execute. Human approval must not bypass Guard.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from pipeline_reliability.adapters import (
    OrchestratorRunResult,
    WarehouseJobResult,
    DownstreamImpactResult,
    RetryResult,
    TaskLogResult,
)
from pipeline_reliability.apply import approve
from pipeline_reliability.decide import (
    CHECK_ORCHESTRATOR_RUN,
    CHECK_WAREHOUSE_JOB,
    CHECK_DOWNSTREAM_IMPACT,
    GET_TASK_LOG,
    RETRY,
    STOP_SAFE,
    decide,
)
from pipeline_reliability.guard import guard
from pipeline_reliability.observability import RunTrace
from pipeline_reliability.runner import run_agent
from pipeline_reliability.state import PipelineReliabilityState


PARTIAL_WRITE_GUARD_REASON = (
    "RETRY rejected: rows_written > 0 while warehouse status is "
    "'FAILED' (possible partial write)."
)


def _timeout_partial_write_state() -> PipelineReliabilityState:
    """Fully gathered State: Decide says RETRY, Guard Rule 2b says no."""
    return PipelineReliabilityState(
        pipeline="mdp_reliability_probe",
        error="timeout",
        orchestrator_status="FAILED",
        warehouse_status="FAILED",
        rows_written=3000,
        downstream_impact="low",
        retries=0,
        task_id="fail_with_timeout",
        run_id="manual__guard_block_demo",
    )


@dataclass
class CountingPartialWriteAdapter:
    """Timeout log + FAILED BQ with rows already written. Counts RETRY mutations."""

    retry_calls: list[str] = field(default_factory=list)

    def check_orchestrator_run(self, state: PipelineReliabilityState) -> OrchestratorRunResult:
        return OrchestratorRunResult(
            status="FAILED",
            failed_task_id="fail_with_timeout",
            detail="Airflow dag_run failed. Failed task: fail_with_timeout.",
        )

    def get_task_log(self, state: PipelineReliabilityState) -> TaskLogResult:
        return TaskLogResult(
            error_type="TimeoutError",
            message="Task exceeded execution_timeout while polling job status.",
            detail="TimeoutError: simulated execution_timeout for agent validation",
        )

    def check_warehouse_job(self, state: PipelineReliabilityState) -> WarehouseJobResult:
        return WarehouseJobResult(
            status="FAILED",
            rows_written=3000,
            detail=(
                "BigQuery job did not SUCCEED; 3000 rows already present in "
                "demo.daily_orders_candidate. The final table remains untouched."
            ),
        )

    def check_downstream_impact(
        self, state: PipelineReliabilityState
    ) -> DownstreamImpactResult:
        return DownstreamImpactResult(
            impact="low",
            affected_assets=["orders_daily_dashboard"],
            detail="Downstream dashboard can tolerate a same-day delay.",
        )

    def retry_orchestrator_task(self, state: PipelineReliabilityState) -> RetryResult:
        self.retry_calls.append(state.task_id or "fail_with_timeout")
        raise AssertionError("RETRY tool must not execute after Guard rejection")


def test_decide_proposes_retry_for_timeout_with_partial_write() -> None:
    state = _timeout_partial_write_state()

    action = decide(state)

    assert action == RETRY
    assert action != STOP_SAFE


def test_guard_rejects_retry_because_rows_already_written() -> None:
    state = _timeout_partial_write_state()

    result = guard(state, RETRY)

    assert result.allowed is False
    assert result.reason == PARTIAL_WRITE_GUARD_REASON


def test_runner_does_not_execute_retry_after_guard_block() -> None:
    adapter = CountingPartialWriteAdapter()
    state = PipelineReliabilityState(
        pipeline="mdp_reliability_probe",
        error="",
        run_id="manual__guard_block_demo",
    )
    trace = RunTrace()

    result = run_agent(state, adapter=adapter, trace=trace)
    actions = [step.action for step in trace.steps]
    retry_steps = [step for step in trace.steps if step.action == RETRY]

    assert actions == [
        CHECK_ORCHESTRATOR_RUN,
        GET_TASK_LOG,
        CHECK_WAREHOUSE_JOB,
        CHECK_DOWNSTREAM_IMPACT,
        RETRY,
    ]
    assert adapter.retry_calls == []
    assert result.state.retries == 0
    assert result.state.outcome == STOP_SAFE
    assert result.state.rows_written == 3000
    assert result.state.warehouse_status == "FAILED"
    assert result.state.error == "timeout"

    assert len(retry_steps) == 1
    assert retry_steps[0].guard_allowed is False
    assert retry_steps[0].guard_reason == PARTIAL_WRITE_GUARD_REASON
    assert retry_steps[0].observation == STOP_SAFE
    assert retry_steps[0].outcome == STOP_SAFE

    assert result.metrics.guard_block_count == 1
    assert result.metrics.guard_block_count == sum(
        1 for step in trace.steps if not step.guard_allowed
    )


def test_human_approval_cannot_bypass_partial_write_guard() -> None:
    adapter = CountingPartialWriteAdapter()
    paused = _timeout_partial_write_state()
    paused.outcome = "ASK_HUMAN"
    paused.awaiting_human = True
    approved = approve(paused, RETRY)
    assert approved.approved_action == RETRY
    trace = RunTrace()

    result = run_agent(approved, adapter=adapter, trace=trace)
    retry_steps = [step for step in trace.steps if step.action == RETRY]

    assert adapter.retry_calls == []
    assert result.state.retries == 0
    assert result.state.outcome == STOP_SAFE
    assert result.state.approved_action is None
    assert retry_steps
    assert retry_steps[0].guard_allowed is False
    assert retry_steps[0].guard_reason == PARTIAL_WRITE_GUARD_REASON
    assert result.metrics.guard_block_count >= 1
