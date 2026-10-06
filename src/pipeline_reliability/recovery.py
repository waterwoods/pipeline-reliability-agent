"""Pipeline Reliability Agent — Recovery policy (Session 4).

Explicit, bounded rules for whether a production failure is SAFE_TO_RETRY.
Guard is the final authority: Decide may propose RETRY; this policy independently
rejects it when a retry could duplicate, corrupt, or run against unknown state.

Decision table (structured State only — not evidence-string scanning):

    Condition                                         Verdict         RETRY
    ------------------------------------------------  --------------  -----
    retry_side_effect == UNKNOWN                      ASK_HUMAN/WAIT  no
    orchestrator_status == UNKNOWN                    WAIT/ASK_HUMAN  no
    warehouse_status == UNKNOWN                       WAIT/ASK_HUMAN  no
    warehouse SUCCEEDED (any writes or none)          STOP_SAFE       no
    rows_written > 0 and warehouse != SUCCEEDED       STOP_SAFE       no
    partial_write is True                             STOP_SAFE       no
    retry_may_duplicate is True                       STOP_SAFE       no
    split stage (orch vs warehouse disagree)          STOP_SAFE       no
    retries >= MAX_RETRIES                            ASK_HUMAN       no
    error not in RETRYABLE_ERRORS                     STOP_SAFE/ASK   no
    downstream_impact == critical                     STOP_SAFE       no
    timeout + retries==0 + zero writes + FAILED/FAILED
      + non-critical downstream + known side effect   SAFE_TO_RETRY   yes
    anything else                                     ASK_HUMAN       no
                                                      (fail-closed)

WAIT vs ASK_HUMAN for UNKNOWN uses the existing poll_count / MAX_POLLS budget.
Backoff metadata is recorded on State; there is no scheduler.

Targeted BACKFILL_PARTITION is a separate fail-closed policy
(evaluate_backfill_safety). It does not reuse RETRY rules: recovering one
ARRIVED partition is not re-running a failed orchestrator task.
"""

from __future__ import annotations

from dataclasses import dataclass

from pipeline_reliability.decide import (
    ASK_HUMAN,
    MAX_POLLS,
    STOP_SAFE,
    WAIT,
    select_backfill_candidate,
)
from pipeline_reliability.state import ArrivalItem, PipelineReliabilityState

MAX_RETRIES = 1
SAFE_TO_RETRY = "SAFE_TO_RETRY"

RETRYABLE_ERRORS = frozenset({"timeout"})
NON_RETRYABLE_ERRORS = frozenset(
    {"data_conflict", "permission_denied", "missing_schema"}
)

# Last-resort backstop only. Structured fields (partial_write,
# retry_may_duplicate, rows_written, warehouse_status) are the primary signal.
_UNSAFE_EVIDENCE_KEYWORDS = (
    "partial",
    "duplicate",
    "conflict",
    "uncertain",
    "already succeeded",
    "already written",
    "unsafe",
    "data loss",
)


@dataclass(frozen=True)
class RetrySafety:
    """Independent RETRY authorization. Guard maps this to GuardResult."""

    allowed: bool
    reason: str
    verdict: str


@dataclass(frozen=True)
class BackfillSafety:
    """Independent BACKFILL_PARTITION authorization. Guard maps this later."""

    allowed: bool
    reason: str


def classify_write_risk(state: PipelineReliabilityState) -> None:
    """Set partial_write / retry_may_duplicate from warehouse facts.

    Mutates State in place. Apply owns when this runs. Does not choose an Action.
    True flags are sticky: a later UNKNOWN observation must not erase known risk.
    """
    rows = state.rows_written
    status = state.warehouse_status
    if rows is not None and rows > 0:
        state.retry_may_duplicate = True
        state.partial_write = status != "SUCCEEDED"
        return
    if rows == 0 and status and status != "UNKNOWN":
        state.retry_may_duplicate = False
        state.partial_write = False
        return
    if state.retry_may_duplicate is not True:
        state.retry_may_duplicate = None
    if state.partial_write is not True:
        state.partial_write = None


def next_wait_backoff_seconds(poll_count: int) -> float:
    """Linear wait metadata in seconds, capped at MAX_POLLS. Not a scheduler.

    ``poll_count`` is how many WAIT polls have already completed.
    """
    if poll_count <= 0:
        return 0.0
    return float(min(poll_count, MAX_POLLS))


def split_stage_failure(state: PipelineReliabilityState) -> bool:
    """True when orchestrator and warehouse terminal statuses disagree."""
    orch = state.orchestrator_status
    warehouse = state.warehouse_status
    orch_success = orch == "SUCCESS"
    orch_failed = orch == "FAILED"
    warehouse_success = warehouse == "SUCCEEDED"
    warehouse_failed = warehouse == "FAILED"
    return (orch_success and warehouse_failed) or (
        orch_failed and warehouse_success
    )


def _writes_confirmed_zero(state: PipelineReliabilityState) -> bool:
    return (
        state.rows_written == 0
        and state.partial_write is not True
        and state.retry_may_duplicate is not True
    )


def approved_repair_ready_to_rerun(state: PipelineReliabilityState) -> bool:
    """True when a validated human-approved repair may rerun the pipeline once."""
    return (
        state.error == "missing_schema"
        and state.repair_applied is True
        and state.repair_validation_passed is True
        and state.retries < MAX_RETRIES
        and state.retry_side_effect != "UNKNOWN"
        and state.orchestrator_status == "FAILED"
        and _writes_confirmed_zero(state)
        and state.downstream_impact not in {"", "critical"}
    )


def _unknown_verdict(state: PipelineReliabilityState) -> str:
    if state.poll_count < MAX_POLLS:
        return WAIT
    return ASK_HUMAN


def _non_retryable_verdict(error: str) -> str:
    if error in {"data_conflict", "permission_denied"}:
        return STOP_SAFE
    return ASK_HUMAN


def _evidence_suggests_unsafe_side_effect(state: PipelineReliabilityState) -> bool:
    """Last-resort string scan. Prefer structured fields above this."""
    notes: list[str] = list(state.evidence)
    if state.observation:
        notes.append(state.observation)
    combined = " ".join(notes).lower()
    return any(keyword in combined for keyword in _UNSAFE_EVIDENCE_KEYWORDS)


def evaluate_retry_safety(state: PipelineReliabilityState) -> RetrySafety:
    """Return whether RETRY is authorized. Fail-closed. Pure: no I/O, no mutation.

    Same State in -> same RetrySafety out. Guard is the caller with authority.
    """
    # --- Ambiguous external mutations / identities ---------------------------
    if state.retry_side_effect == "UNKNOWN":
        verdict = WAIT if (
            state.orchestrator_status == "RUNNING" and state.poll_count < MAX_POLLS
        ) else ASK_HUMAN
        return RetrySafety(
            allowed=False,
            reason=(
                "RETRY rejected: previous RETRY side effect is UNKNOWN; "
                "reconcile with orchestrator before another RETRY."
            ),
            verdict=verdict,
        )

    if state.orchestrator_status == "UNKNOWN":
        return RetrySafety(
            allowed=False,
            reason=(
                "RETRY rejected: orchestrator status is UNKNOWN; "
                "reconcile with orchestrator before RETRY."
            ),
            verdict=_unknown_verdict(state),
        )

    if state.warehouse_status == "UNKNOWN":
        return RetrySafety(
            allowed=False,
            reason=(
                "RETRY rejected: warehouse status is UNKNOWN; "
                "reconcile before RETRY."
            ),
            verdict=_unknown_verdict(state),
        )

    # --- Warehouse writes: full, partial, or duplicate risk ------------------
    if state.warehouse_status == "SUCCEEDED":
        if state.rows_written is not None and state.rows_written > 0:
            return RetrySafety(
                allowed=False,
                reason=(
                    "RETRY rejected: warehouse already SUCCEEDED with "
                    f"{state.rows_written} rows written."
                ),
                verdict=STOP_SAFE,
            )
        return RetrySafety(
            allowed=False,
            reason="RETRY rejected: warehouse already SUCCEEDED.",
            verdict=STOP_SAFE,
        )

    if (
        state.rows_written is not None
        and state.rows_written > 0
        and state.warehouse_status != "SUCCEEDED"
    ):
        status_label = state.warehouse_status or "unknown"
        return RetrySafety(
            allowed=False,
            reason=(
                "RETRY rejected: rows_written > 0 while warehouse status is "
                f"'{status_label}' (possible partial write)."
            ),
            verdict=STOP_SAFE,
        )

    if state.partial_write is True:
        return RetrySafety(
            allowed=False,
            reason="RETRY rejected: structured partial_write is True.",
            verdict=STOP_SAFE,
        )

    if state.retry_may_duplicate is True:
        return RetrySafety(
            allowed=False,
            reason="RETRY rejected: structured retry_may_duplicate is True.",
            verdict=STOP_SAFE,
        )

    if split_stage_failure(state):
        return RetrySafety(
            allowed=False,
            reason=(
                "RETRY rejected: orchestrator and warehouse statuses disagree "
                "(split-stage failure)."
            ),
            verdict=STOP_SAFE,
        )

    # --- Budgets and error class --------------------------------------------
    if state.retries >= MAX_RETRIES:
        return RetrySafety(
            allowed=False,
            reason="RETRY rejected: retries >= 1 (max automatic retry already used).",
            verdict=ASK_HUMAN,
        )

    # Validated human-approved schema repair may rerun the pipeline once.
    # This is not a blind missing_schema retry of the drifted file.
    if approved_repair_ready_to_rerun(state):
        return RetrySafety(
            allowed=True,
            reason=(
                "RETRY allowed: approved schema repair applied and validated; "
                "first pipeline rerun, zero writes, non-critical downstream."
            ),
            verdict=SAFE_TO_RETRY,
        )

    if state.error not in RETRYABLE_ERRORS:
        return RetrySafety(
            allowed=False,
            reason=f"RETRY rejected: error is '{state.error}', not 'timeout'.",
            verdict=_non_retryable_verdict(state.error),
        )

    if state.downstream_impact == "critical":
        return RetrySafety(
            allowed=False,
            reason="RETRY rejected: downstream_impact is 'critical'.",
            verdict=STOP_SAFE,
        )

    if _evidence_suggests_unsafe_side_effect(state):
        return RetrySafety(
            allowed=False,
            reason="RETRY rejected: evidence suggests an uncertain or unsafe side effect.",
            verdict=ASK_HUMAN,
        )

    # --- Explicit allow: every SAFE_TO_RETRY conjunct must hold -------------
    if (
        state.error in RETRYABLE_ERRORS
        and state.retries < MAX_RETRIES
        and state.retry_side_effect != "UNKNOWN"
        and state.orchestrator_status == "FAILED"
        and state.warehouse_status == "FAILED"
        and _writes_confirmed_zero(state)
        and state.downstream_impact not in {"", "critical"}
    ):
        return RetrySafety(
            allowed=True,
            reason=(
                "RETRY allowed: timeout, first retry, non-critical downstream, "
                "no unsafe evidence."
            ),
            verdict=SAFE_TO_RETRY,
        )

    return RetrySafety(
        allowed=False,
        reason=(
            "RETRY rejected: recovery policy fail-closed; "
            "case is not SAFE_TO_RETRY."
        ),
        verdict=ASK_HUMAN,
    )


def _next_arrived_item(state: PipelineReliabilityState) -> ArrivalItem | None:
    """Partition Guard evaluates for BACKFILL_PARTITION.

    The write candidate is the oldest confirmed-empty ARRIVED partition.
    If none is eligible, the first unchecked or occupied ARRIVED item is
    returned so the occupancy rules below can still reject it. That item
    is not a selected backfill.
    """
    chosen = select_backfill_candidate(state)
    if chosen is not None:
        return chosen
    for item in state.arrival_items:
        if item.status == "ARRIVED" and item.target_partition_empty is None:
            return item
    for item in state.arrival_items:
        if item.status == "ARRIVED":
            return item
    return None


def _has_clear_partition_identity(asset: str, expected_partition: str) -> bool:
    return bool(asset.strip()) and bool(expected_partition.strip())


def evaluate_backfill_safety(state: PipelineReliabilityState) -> BackfillSafety:
    """Return whether targeted partition backfill is authorized. Fail-closed.

    Pure: reads State only. No I/O, no mutation, no execution.
    Same State in -> same BackfillSafety out. Guard is not wired yet.
    V1 auto-backfills only a confirmed-empty target partition
    (ArrivalItem.target_partition_empty is True). Occupancy False or None
    is denied. This function does not discover occupancy.
    """
    if state.load_mode != "PARTITIONED":
        return BackfillSafety(
            allowed=False,
            reason="BACKFILL rejected: load_mode is not PARTITIONED.",
        )

    if not state.arrival_items:
        return BackfillSafety(
            allowed=False,
            reason="BACKFILL rejected: no arrival_items on State.",
        )

    arrived = _next_arrived_item(state)
    if arrived is None:
        return BackfillSafety(
            allowed=False,
            reason="BACKFILL rejected: no ARRIVED partition to recover.",
        )

    if arrived.target_partition_empty is None:
        return BackfillSafety(
            allowed=False,
            reason="BACKFILL rejected: target partition occupancy is unknown.",
        )
    if arrived.target_partition_empty is False:
        return BackfillSafety(
            allowed=False,
            reason="BACKFILL rejected: target partition already contains data.",
        )

    if not _has_clear_partition_identity(arrived.asset, arrived.expected_partition):
        return BackfillSafety(
            allowed=False,
            reason=(
                "BACKFILL rejected: ARRIVED item is missing a clear "
                "asset or expected_partition."
            ),
        )

    if state.partial_write is True:
        return BackfillSafety(
            allowed=False,
            reason="BACKFILL rejected: partial write already detected.",
        )
    if state.partial_write is not False:
        return BackfillSafety(
            allowed=False,
            reason="BACKFILL rejected: partial-write risk is unknown.",
        )

    if state.retry_may_duplicate is True:
        return BackfillSafety(
            allowed=False,
            reason="BACKFILL rejected: duplicate-write risk already detected.",
        )
    if state.retry_may_duplicate is not False:
        return BackfillSafety(
            allowed=False,
            reason="BACKFILL rejected: duplicate-write risk is unknown.",
        )

    if not state.downstream_impact:
        return BackfillSafety(
            allowed=False,
            reason="BACKFILL rejected: downstream_impact is unknown.",
        )
    if state.downstream_impact == "critical":
        return BackfillSafety(
            allowed=False,
            reason="BACKFILL rejected: downstream_impact is critical.",
        )

    return BackfillSafety(
        allowed=True,
        reason=(
            "BACKFILL allowed: PARTITIONED load, next ARRIVED item has "
            "clear identity, confirmed-empty target partition, "
            "no known write risk, non-critical downstream."
        ),
    )
