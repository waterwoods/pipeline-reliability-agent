"""Crash/Resume production-style proof — one dangerous boundary.

Worker A records RETRY intent, Airflow accepts, then dies before the retry
result is checkpointed. Worker B is a fresh ``run_agent()`` on the same
durable files. Airflow is a file-backed ledger (external to both workers),
not in-memory State.retries.

RESUME != REPLAY. No special recovery engine.
"""

from __future__ import annotations

import json
from copy import deepcopy
from dataclasses import dataclass
from pathlib import Path

import pytest

import pipeline_reliability.runner as runner_mod
import pipeline_reliability.tools as tools_mod
from pipeline_reliability.adapters import (
    OrchestratorRunResult,
    MockPipelineAdapter,
    RetryResult,
)
from pipeline_reliability.apply import apply as real_apply
from pipeline_reliability.checkpoint import (
    RETRY_INTENT_NOTE,
    load_checkpoint,
    save_checkpoint,
)
from pipeline_reliability.decide import CHECK_ORCHESTRATOR_RUN, RETRY, STOP_SAFE, WAIT, decide
from pipeline_reliability.guard import guard
from pipeline_reliability.runner import run_agent
from pipeline_reliability.state import PipelineReliabilityState
from pipeline_reliability.tools import TRANSIENT_TIMEOUT


CRASH_AFTER_ACCEPT = "crash after Airflow accepted retry, before result checkpoint"


class CrashAfterRetryAccepted(RuntimeError):
    """Worker death in the INTENT → SIDE EFFECT → (missing) RESULT window."""


@dataclass
class FileBackedAirflowAdapter(MockPipelineAdapter):
    """Airflow that survives Worker A dying: retry count and status on disk.

    Worker B constructs a *new* adapter on the same ledger. Duplicate RETRY
    is visible as ledger retry_count > 1, not as a shared Python object.
    """

    ledger_path: Path
    crash_after_accept: bool = False

    def __post_init__(self) -> None:
        if not self.ledger_path.exists():
            self._write({"retry_count": 0, "check_count": 0, "status": "FAILED"})

    def _read(self) -> dict:
        return json.loads(self.ledger_path.read_text(encoding="utf-8"))

    def _write(self, payload: dict) -> None:
        tmp = self.ledger_path.with_suffix(".tmp")
        tmp.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
        tmp.replace(self.ledger_path)

    def check_orchestrator_run(self, state: PipelineReliabilityState) -> OrchestratorRunResult:
        data = self._read()
        if data["retry_count"] < 1:
            return super().check_orchestrator_run(state)
        data["check_count"] = int(data.get("check_count") or 0) + 1
        # First reconcile sees the in-flight retry; later checks recover.
        data["status"] = "RUNNING" if data["check_count"] == 1 else "SUCCESS"
        self._write(data)
        status = data["status"]
        return OrchestratorRunResult(
            status=status,
            failed_task_id="" if status in {"RUNNING", "SUCCESS"} else "load_orders_to_bigquery",
            detail=f"External Airflow run is {status} after accepted retry.",
        )

    def retry_orchestrator_task(self, state: PipelineReliabilityState) -> RetryResult:
        data = self._read()
        data["retry_count"] = int(data.get("retry_count") or 0) + 1
        data["status"] = "RUNNING"
        self._write(data)
        if self.crash_after_accept:
            raise CrashAfterRetryAccepted(CRASH_AFTER_ACCEPT)
        return RetryResult(
            accepted=True,
            pipeline_id=state.pipeline,
            run_id=state.run_id,
            task_id=state.task_id or "load_orders_to_bigquery",
            detail="Airflow accepted clear/retry (recovery not claimed).",
            orchestrator_status="queued",
        )


def _incident() -> PipelineReliabilityState:
    """Facts already gathered so Worker A's first Decide is RETRY."""
    return PipelineReliabilityState(
        pipeline="daily_orders",
        error="timeout",
        orchestrator_status="FAILED",
        warehouse_status="FAILED",
        rows_written=0,
        downstream_impact="low",
        task_id="load_orders_to_bigquery",
        run_id="manual__crash_resume_proof",
        scenario=TRANSIENT_TIMEOUT,
        retries=0,
    )


def _ledger_retry_count(ledger: Path) -> int:
    return int(json.loads(ledger.read_text(encoding="utf-8"))["retry_count"])


# The accepted crash proof records one Airflow retry, then Worker B reconciles.
_ACCEPTED_CRASH_RETRIES = 1


def crash_duplicate_external_writes(work_dir: Path) -> int:
    """Extra Airflow retries after crash and reconcile. Zero is the proof.

    One accepted retry is the crash itself. Every later retry is a duplicate.
    A missing retry is not reported as a clean zero.
    """
    previous_wait = tools_mod.WAIT_SECONDS
    previous_sleep = tools_mod._sleep_fn
    tools_mod.WAIT_SECONDS = 0
    tools_mod._sleep_fn = lambda _seconds: None
    try:
        checkpoint = _run_worker_a(work_dir)
        ledger = work_dir / "airflow_ledger.json"
        resumed = load_checkpoint(checkpoint)
        if resumed is None:
            raise RuntimeError("crash checkpoint was not written")
        run_agent(
            resumed,
            adapter=FileBackedAirflowAdapter(ledger_path=ledger, crash_after_accept=False),
            checkpoint_path=checkpoint,
            agent_run_id="worker-b",
        )
        retries = _ledger_retry_count(ledger)
    finally:
        tools_mod.WAIT_SECONDS = previous_wait
        tools_mod._sleep_fn = previous_sleep
    if retries < _ACCEPTED_CRASH_RETRIES:
        return _ACCEPTED_CRASH_RETRIES
    return retries - _ACCEPTED_CRASH_RETRIES


def _no_wait_sleep(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(tools_mod, "WAIT_SECONDS", 0)
    monkeypatch.setattr(tools_mod, "_sleep_fn", lambda _s: None)


def _run_worker_a(work_dir: Path) -> Path:
    """Decide RETRY → Guard allow → persist UNKNOWN → Airflow accept → crash."""
    checkpoint = work_dir / "checkpoint.json"
    ledger = work_dir / "airflow_ledger.json"
    state = _incident()
    save_checkpoint(state, checkpoint)
    adapter = FileBackedAirflowAdapter(ledger_path=ledger, crash_after_accept=True)
    with pytest.raises(CrashAfterRetryAccepted, match="before result checkpoint"):
        run_agent(
            state,
            adapter=adapter,
            checkpoint_path=checkpoint,
            agent_run_id="worker-a",
        )
    return checkpoint


def test_worker_b_reconciles_running_and_does_not_retry_again(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _no_wait_sleep(monkeypatch)
    checkpoint = _run_worker_a(tmp_path)
    ledger = tmp_path / "airflow_ledger.json"

    # Worker A is gone: in-memory State and adapter are not reused.
    assert _ledger_retry_count(ledger) == 1

    resumed = load_checkpoint(checkpoint)
    assert resumed is not None
    assert resumed.pipeline == "daily_orders"
    assert resumed.run_id == "manual__crash_resume_proof"
    assert resumed.task_id == "load_orders_to_bigquery"
    assert resumed.retries == 0
    assert resumed.retry_side_effect == "UNKNOWN"
    assert resumed.orchestrator_status == ""
    assert RETRY_INTENT_NOTE in resumed.evidence
    assert decide(resumed) == CHECK_ORCHESTRATOR_RUN
    assert guard(resumed, RETRY).allowed is False

    reconciled: list[PipelineReliabilityState] = []

    def spy_apply(state, action, observation, **kwargs):
        updated = real_apply(state, action, observation, **kwargs)
        if action == CHECK_ORCHESTRATOR_RUN and not reconciled:
            reconciled.append(deepcopy(state))
        return updated

    monkeypatch.setattr(runner_mod, "apply", spy_apply)

    adapter_b = FileBackedAirflowAdapter(ledger_path=ledger, crash_after_accept=False)
    result_b = run_agent(
        resumed,
        adapter=adapter_b,
        checkpoint_path=checkpoint,
        agent_run_id="worker-b",
    )
    actions = [step.action for step in result_b.trace.steps]

    assert result_b.agent_run_id == "worker-b"
    assert _ledger_retry_count(ledger) == 1
    assert actions[0] == CHECK_ORCHESTRATOR_RUN
    assert RETRY not in actions
    assert actions[1] == WAIT
    assert CHECK_ORCHESTRATOR_RUN in actions[2:]

    assert reconciled, "Worker B never applied CHECK_ORCHESTRATOR_RUN"
    after_check = reconciled[0]
    assert after_check.orchestrator_status == "RUNNING"
    assert after_check.retry_side_effect == ""
    assert after_check.retries == 1
    assert guard(after_check, RETRY).allowed is False

    assert "RUNNING" in result_b.trace.steps[0].observation
    assert result_b.state.orchestrator_status == "SUCCESS"
    assert result_b.state.retry_side_effect == ""
    assert result_b.state.outcome == STOP_SAFE


def test_guard_blocks_incorrect_retry_on_resume_without_calling_airflow(
    tmp_path: Path,
) -> None:
    checkpoint = _run_worker_a(tmp_path)
    ledger = tmp_path / "airflow_ledger.json"
    assert _ledger_retry_count(ledger) == 1

    resumed = load_checkpoint(checkpoint)
    resumed.approved_action = RETRY
    assert decide(resumed) == CHECK_ORCHESTRATOR_RUN
    assert guard(resumed, RETRY).allowed is False

    adapter_b = FileBackedAirflowAdapter(ledger_path=ledger, crash_after_accept=False)
    result_b = run_agent(
        resumed,
        adapter=adapter_b,
        checkpoint_path=checkpoint,
        agent_run_id="worker-b-forced-retry",
    )
    actions = [step.action for step in result_b.trace.steps]

    assert _ledger_retry_count(ledger) == 1
    assert actions[0] == RETRY
    assert result_b.trace.steps[0].guard_allowed is False
    assert result_b.state.outcome == STOP_SAFE


def test_crash_reconcile_duplicate_external_writes_are_zero(tmp_path: Path) -> None:
    assert crash_duplicate_external_writes(tmp_path) == 0
