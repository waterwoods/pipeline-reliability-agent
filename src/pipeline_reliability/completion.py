"""Pipeline Reliability Agent — Completion Contract V1.

One deterministic question:

    Does the current pipeline state have enough evidence to be considered
    successfully complete?

State in → PASS / FAIL / UNKNOWN. No I/O, no Action, no State mutation.

Decide still owns "what Action should happen next?" This module does not
choose CHECK_WAREHOUSE_JOB, STOP_SAFE, or FINISH.

Orchestrator SUCCESS is not verified recovery. A green DAG can still leave
known-unhealthy data. V1 uses only facts already on State — no DQ scanner.
"""

from __future__ import annotations

from typing import Literal

from pipeline_reliability.state import PipelineReliabilityState

CompletionStatus = Literal["PASS", "FAIL", "UNKNOWN"]

PASS: CompletionStatus = "PASS"
FAIL: CompletionStatus = "FAIL"
UNKNOWN: CompletionStatus = "UNKNOWN"


def _known_pair_mismatch(expected: str | None, observed: str | None) -> bool:
    """True only when both sides are known and they disagree."""
    return (
        expected is not None
        and observed is not None
        and expected != observed
    )


def _known_unhealthy_data(state: PipelineReliabilityState) -> bool:
    """True when State already records a health defect.

    Unknown / unset facts are not defects. retry_may_duplicate is not a
    health signal: a legitimate successful write still carries retry-risk
    metadata so Guard can block a second load.
    """
    if state.partial_write is True:
        return True
    if state.volume_status == "too_low":
        return True
    if _known_pair_mismatch(
        state.expected_business_date, state.observed_business_date
    ):
        return True
    if _known_pair_mismatch(state.expected_partition, state.target_partition):
        return True
    if _known_pair_mismatch(state.expected_delimiter, state.observed_delimiter):
        return True
    if _known_pair_mismatch(state.expected_encoding, state.observed_encoding):
        return True
    if state.repair_applied is True and state.repair_validation_passed is not True:
        return True
    return False


def _warehouse_terminal_ok(state: PipelineReliabilityState) -> bool:
    """True when warehouse status is an acceptable terminal for V1."""
    if state.warehouse_status == "SUCCEEDED":
        return True
    # Schema-only disposable DAG: no warehouse job exists. A validated
    # staging repair plus orchestrator SUCCESS is enough to finish.
    return (
        state.warehouse_status == "NO_JOB"
        and state.rows_written == 0
        and state.repair_validation_passed is True
    )


def _arrival_recovery_completed(state: PipelineReliabilityState) -> bool:
    """True when a PARTITIONED late/missing recovery is fully validated and clean.

    All VALIDATED is not enough by itself. Uncertain writes and known
    unhealthy-data facts still block PASS. Empty arrival_items is not
    this incident class.
    """
    if not state.arrival_items:
        return False
    if state.load_mode != "PARTITIONED":
        return False
    if any(item.status != "VALIDATED" for item in state.arrival_items):
        return False
    if state.backfill_side_effect == "UNKNOWN":
        return False
    if state.partial_write is True:
        return False
    if state.retry_may_duplicate is True:
        return False
    if _known_unhealthy_data(state):
        return False
    return True


def evaluate_completion(state: PipelineReliabilityState) -> CompletionStatus:
    """Return whether State evidence counts as verified completion.

    Pure function. Same State in → same verdict out.
    PASS requires green jobs AND no known unhealthy data facts, except a
    fully validated PARTITIONED arrival recovery may PASS even when the
    original orchestrator status is still FAILED.
    """
    # Targeted backfill + validation is independent evidence. The original
    # DAG FAILED because the source was missing; that stale status must not
    # block close after every expected partition is VALIDATED and clean.
    if _arrival_recovery_completed(state):
        return PASS

    if state.orchestrator_status == "FAILED":
        return FAIL

    # Missing, RUNNING, or any other non-SUCCESS value is not conclusive.
    if state.orchestrator_status != "SUCCESS":
        return UNKNOWN

    if not state.warehouse_status:
        return UNKNOWN

    if state.warehouse_status == "FAILED":
        return FAIL

    if _warehouse_terminal_ok(state):
        if _known_unhealthy_data(state):
            return FAIL
        return PASS

    if (
        state.warehouse_status == "NO_JOB"
        and state.repair_applied is True
        and state.repair_validation_passed is not True
    ):
        return FAIL

    # Observed but not a V1 terminal (e.g. RUNNING) — not conclusive success.
    return UNKNOWN
