#!/usr/bin/env python3
"""Local crash / UNKNOWN / reconcile proof.

Reuses the existing crash-resume helpers. Worker A accepts one RETRY and
dies before the result checkpoint. Worker B loads that checkpoint, reconciles
the file-backed Airflow ledger, and does not clear again.

    python scripts/recovery_demo.py
"""

from __future__ import annotations

import contextlib
import importlib.util
import io
import sys
import tempfile
from dataclasses import dataclass
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
for _path in (REPO_ROOT, REPO_ROOT / "src"):
    if str(_path) not in sys.path:
        sys.path.insert(0, str(_path))

import pipeline_reliability.tools as tools_mod  # noqa: E402
from pipeline_reliability.checkpoint import load_checkpoint  # noqa: E402
from pipeline_reliability.decide import CHECK_ORCHESTRATOR_RUN, RETRY  # noqa: E402
from pipeline_reliability.runner import run_agent  # noqa: E402

_CRASH_MODULE = (
    REPO_ROOT / "tests" / "pipeline_reliability" / "test_crash_resume_production_proof.py"
)
_ACCEPTED_CRASH_RETRIES = 1


@dataclass(frozen=True)
class RecoveryDemo:
    retry_side_effect_after_crash: str
    ledger_retries_after_crash: int
    worker_b_actions: tuple[str, ...]
    ledger_retries_after_takeover: int
    duplicate_external_writes: int
    final_outcome: str

    @property
    def passed(self) -> bool:
        return (
            self.retry_side_effect_after_crash == "UNKNOWN"
            and self.ledger_retries_after_crash == _ACCEPTED_CRASH_RETRIES
            and bool(self.worker_b_actions)
            and self.worker_b_actions[0] == CHECK_ORCHESTRATOR_RUN
            and RETRY not in self.worker_b_actions
            and self.ledger_retries_after_takeover == _ACCEPTED_CRASH_RETRIES
            and self.duplicate_external_writes == 0
            and self.final_outcome == "STOP_SAFE"
        )


def _crash_module():
    spec = importlib.util.spec_from_file_location(
        "crash_resume_production_proof_demo",
        _CRASH_MODULE,
    )
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load {_CRASH_MODULE}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def _duplicates(retries: int) -> int:
    """Same rule as the crash proof: one accepted clear, every later clear is extra."""
    if retries < _ACCEPTED_CRASH_RETRIES:
        return _ACCEPTED_CRASH_RETRIES
    return retries - _ACCEPTED_CRASH_RETRIES


def run_recovery_demo(work_dir: Path) -> RecoveryDemo:
    """Run Worker A crash and Worker B reconcile in ``work_dir``."""
    crash = _crash_module()
    previous_wait = tools_mod.WAIT_SECONDS
    previous_sleep = tools_mod._sleep_fn
    tools_mod.WAIT_SECONDS = 0
    tools_mod._sleep_fn = lambda _seconds: None
    try:
        with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(
            io.StringIO()
        ):
            checkpoint = crash._run_worker_a(work_dir)
            ledger = work_dir / "airflow_ledger.json"
            resumed = load_checkpoint(checkpoint)
            if resumed is None:
                raise RuntimeError("crash checkpoint was not written")
            retries_after_crash = crash._ledger_retry_count(ledger)
            side_effect = resumed.retry_side_effect
            result = run_agent(
                resumed,
                adapter=crash.FileBackedAirflowAdapter(
                    ledger_path=ledger,
                    crash_after_accept=False,
                ),
                checkpoint_path=checkpoint,
                agent_run_id="worker-b",
            )
            retries_after = crash._ledger_retry_count(ledger)
    finally:
        tools_mod.WAIT_SECONDS = previous_wait
        tools_mod._sleep_fn = previous_sleep
    actions = tuple(step.action for step in result.trace.steps)
    return RecoveryDemo(
        retry_side_effect_after_crash=side_effect,
        ledger_retries_after_crash=retries_after_crash,
        worker_b_actions=actions,
        ledger_retries_after_takeover=retries_after,
        duplicate_external_writes=_duplicates(retries_after),
        final_outcome=result.state.outcome or "",
    )


def format_recovery_demo(demo: RecoveryDemo) -> str:
    actions = " ".join(demo.worker_b_actions) if demo.worker_b_actions else "(none)"
    lines = [
        "Recovery demo: crash during UNKNOWN retry, then reconcile",
        "-------------------------------------------------------",
        "Worker A accepted RETRY and crashed before the result checkpoint.",
        f"Checkpoint retry_side_effect: {demo.retry_side_effect_after_crash}",
        f"Ledger retries after crash: {demo.ledger_retries_after_crash}",
        f"Worker B actions: {actions}",
        f"Ledger retries after takeover: {demo.ledger_retries_after_takeover}",
        f"Duplicate external writes: {demo.duplicate_external_writes}",
        f"Final outcome: {demo.final_outcome or '(none)'}",
        f"Result: {'PASS' if demo.passed else 'FAIL'}",
    ]
    return "\n".join(lines)


def main() -> int:
    with tempfile.TemporaryDirectory() as directory:
        demo = run_recovery_demo(Path(directory))
    print(format_recovery_demo(demo))
    return 0 if demo.passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
