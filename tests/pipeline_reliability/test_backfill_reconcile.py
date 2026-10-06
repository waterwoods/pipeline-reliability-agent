"""Crash/resume for BACKFILL: UNKNOWN intent is reconciled, never replayed.

Public loop only: run_agent() → Decide → Guard → Execute → Apply.
Resume is not replay. First ask reality what happened.
"""

from __future__ import annotations

from dataclasses import dataclass, field

import pytest

import pipeline_reliability.tools as tools_mod
from pipeline_reliability.adapters import BackfillReconcileResult, MockPipelineAdapter
from pipeline_reliability.completion import PASS, evaluate_completion
from pipeline_reliability.decide import (
    ASK_HUMAN,
    BACKFILL_PARTITION,
    FINISH,
    RECONCILE_BACKFILL,
    RETRY,
    VALIDATE_PARTITION,
    decide,
)
from pipeline_reliability.runner import run_agent
from pipeline_reliability.state import ArrivalItem, PipelineReliabilityState


@pytest.fixture(autouse=True)
def _no_wait_sleep(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(tools_mod, "WAIT_SECONDS", 0)
    monkeypatch.setattr(tools_mod, "_sleep_fn", lambda _s: None)


_ASSET = "stg_orders"
_PARTITION = "2026-09-17"


def _unknown_intent_state() -> PipelineReliabilityState:
    """Crash window: a BACKFILL may already have written; result is not durable."""
    return PipelineReliabilityState(
        pipeline="daily_orders",
        error="",
        orchestrator_status="FAILED",
        warehouse_status="FAILED",
        rows_written=0,
        partial_write=False,
        retry_may_duplicate=False,
        downstream_impact="low",
        sla_breached=False,
        load_mode="PARTITIONED",
        task_id="load_orders_to_bigquery",
        run_id="manual__backfill_crash_reconcile",
        backfill_side_effect="UNKNOWN",
        backfill_asset=_ASSET,
        backfill_partition=_PARTITION,
        arrival_items=[
            ArrivalItem(
                asset=_ASSET,
                expected_partition=_PARTITION,
                status="ARRIVED",
                target_partition_empty=None,
            )
        ],
    )


@dataclass
class _ReconcileAdapter(MockPipelineAdapter):
    """Read-only reconcile of the recorded asset+partition. Write policy varies."""

    reconcile_status: str
    allow_backfill: bool = False
    reconcile_calls: list[tuple[str, str]] = field(default_factory=list)
    backfill_calls: int = 0

    def reconcile_backfill(
        self,
        state: PipelineReliabilityState,
        asset: str,
        expected_partition: str,
    ) -> BackfillReconcileResult:
        self.reconcile_calls.append((asset, expected_partition))
        return BackfillReconcileResult(
            asset=asset,
            expected_partition=expected_partition,
            status=self.reconcile_status,
            detail=f"Mock reconcile: {self.reconcile_status}.",
        )

    def backfill_partition(
        self,
        state: PipelineReliabilityState,
        asset: str,
        expected_partition: str,
    ):
        self.backfill_calls += 1
        if not self.allow_backfill:
            raise AssertionError(
                f"BACKFILL_PARTITION must not run after reconcile {self.reconcile_status}"
            )
        return super().backfill_partition(state, asset, expected_partition)

    def retry_orchestrator_task(self, state: PipelineReliabilityState):
        raise AssertionError("RETRY must not run while reconciling an UNKNOWN backfill")


def _assert_unknown_does_not_replay(state: PipelineReliabilityState) -> None:
    """The most important safety fact: UNKNOWN never jumps to a second write."""
    assert decide(state) == RECONCILE_BACKFILL
    assert decide(state) != BACKFILL_PARTITION


def test_unknown_backfill_landed_reconciles_then_validates() -> None:
    state = _unknown_intent_state()
    adapter = _ReconcileAdapter(reconcile_status="LANDED")
    _assert_unknown_does_not_replay(state)

    result = run_agent(state, adapter=adapter)
    actions = [step.action for step in result.trace.steps]
    item = result.state.arrival_items[0]

    assert actions[0] == RECONCILE_BACKFILL
    assert BACKFILL_PARTITION not in actions
    assert actions == [RECONCILE_BACKFILL, VALIDATE_PARTITION, FINISH]
    assert adapter.reconcile_calls == [(_ASSET, _PARTITION)]
    assert adapter.backfill_calls == 0
    assert item.status == "VALIDATED"
    assert result.state.backfill_side_effect == ""
    assert result.state.backfill_asset == ""
    assert result.state.backfill_partition == ""
    assert evaluate_completion(result.state) == PASS
    assert result.state.outcome == FINISH


def test_unknown_backfill_not_landed_may_backfill_once_after_guard() -> None:
    state = _unknown_intent_state()
    adapter = _ReconcileAdapter(reconcile_status="NOT_LANDED", allow_backfill=True)
    _assert_unknown_does_not_replay(state)

    result = run_agent(state, adapter=adapter)
    actions = [step.action for step in result.trace.steps]
    backfill_steps = [
        step for step in result.trace.steps if step.action == BACKFILL_PARTITION
    ]
    item = result.state.arrival_items[0]

    assert actions[0] == RECONCILE_BACKFILL
    assert BACKFILL_PARTITION in actions
    assert actions.index(RECONCILE_BACKFILL) < actions.index(BACKFILL_PARTITION)
    assert adapter.reconcile_calls == [(_ASSET, _PARTITION)]
    assert adapter.backfill_calls == 1
    assert actions.count(BACKFILL_PARTITION) == 1
    assert len(backfill_steps) == 1
    assert backfill_steps[0].guard_allowed is True
    assert item.status == "VALIDATED"
    assert result.state.backfill_side_effect == ""
    assert result.state.outcome == FINISH


def test_unknown_backfill_unproven_asks_human_without_replay() -> None:
    state = _unknown_intent_state()
    adapter = _ReconcileAdapter(reconcile_status="UNKNOWN")
    _assert_unknown_does_not_replay(state)

    result = run_agent(state, adapter=adapter)
    actions = [step.action for step in result.trace.steps]
    item = result.state.arrival_items[0]

    assert actions[0] == RECONCILE_BACKFILL
    assert BACKFILL_PARTITION not in actions
    assert RETRY not in actions
    assert FINISH not in actions
    assert adapter.reconcile_calls == [(_ASSET, _PARTITION)]
    assert adapter.backfill_calls == 0
    assert item.status == "ARRIVED"
    assert item.status != "BACKFILLED"
    assert item.status != "VALIDATED"
    assert result.state.backfill_side_effect == "UNKNOWN"
    assert result.state.backfill_asset == _ASSET
    assert result.state.backfill_partition == _PARTITION
    assert evaluate_completion(result.state) != PASS
    assert result.state.outcome == ASK_HUMAN
    assert result.state.awaiting_human is True
