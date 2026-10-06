"""Pipeline Reliability Agent — Decide.

Three layers in the agent loop (only Decide lives in this file for now):

    State   = current world snapshot (read-only input here)
    Decide  = choose the next action string (pure function, no side effects)
    Execute = perform the action (tools, APIs — built later)

Decide *proposes* what should happen next. It has no authority to run tools,
mutate State, call APIs, or change the outside world. Guard (later) will
check the proposal; Execute (later) will carry it out.

Same State in -> same action out. No mutation, no I/O.
"""

from __future__ import annotations

from pipeline_reliability.completion import (
    FAIL as COMPLETION_FAIL,
    UNKNOWN as COMPLETION_UNKNOWN,
    evaluate_completion,
)
from pipeline_reliability.state import ArrivalItem, PipelineReliabilityState

# ---------------------------------------------------------------------------
# Action strings — vocabulary for what Execute may do later
# ---------------------------------------------------------------------------
# Keeping these as plain strings keeps the learning path simple. Guard and
# Execute will match on the same values Decide returns.

CHECK_ORCHESTRATOR_RUN = "CHECK_ORCHESTRATOR_RUN"
CHECK_WAREHOUSE_JOB = "CHECK_WAREHOUSE_JOB"
GET_TASK_LOG = "GET_TASK_LOG"
CHECK_DOWNSTREAM_IMPACT = "CHECK_DOWNSTREAM_IMPACT"
CHECK_LOG = "CHECK_LOG"
RETRY = "RETRY"
APPLY_APPROVED_REPAIR = "APPLY_APPROVED_REPAIR"
WAIT = "WAIT"
ASK_HUMAN = "ASK_HUMAN"
STOP_SAFE = "STOP_SAFE"
FINISH = "FINISH"
CHECK_EXPECTED_INPUTS = "CHECK_EXPECTED_INPUTS"
WAIT_FOR_SOURCE = "WAIT_FOR_SOURCE"
BACKFILL_PARTITION = "BACKFILL_PARTITION"
VALIDATE_PARTITION = "VALIDATE_PARTITION"
RECONCILE_BACKFILL = "RECONCILE_BACKFILL"

# ArrivalItem.status values Decide recognizes in V1. Anything else is fail-closed.
_ARRIVAL_STATUSES = frozenset(
    {"UNKNOWN", "MISSING", "ARRIVED", "BACKFILLED", "VALIDATED"}
)

# Bounded polls while the orchestrator reports RUNNING after a controlled retry.
# Distinct from retries (re-executions) and from runner MAX_STEPS.
MAX_POLLS = 3

# Bounded source-arrival rechecks while PARTITIONED inputs stay MISSING.
# Distinct from poll_count (orchestrator/warehouse WAIT) and from retries.
MAX_ARRIVAL_RECHECKS = 3


def decide(state: PipelineReliabilityState) -> str:
    """Return the next action for this incident snapshot.

    Pure function: reads State only, returns an action string, changes nothing.

    After a HITL REJECT, a second ASK_HUMAN on the same snapshot becomes
    STOP_SAFE so the Agent does not reopen an infinite human loop. REJECT
    is not an automatic incident close: WAIT / STOP_SAFE / investigation
    playbooks still run first.
    """
    action = _decide_playbook(state)
    if action == ASK_HUMAN and state.last_human_decision == "REJECT":
        return STOP_SAFE
    return action


def select_backfill_candidate(state: PipelineReliabilityState) -> ArrivalItem | None:
    """Oldest ARRIVED partition whose target is confirmed empty, or None.

    Eligible means status ARRIVED and target_partition_empty is True.
    Occupied targets and unknown occupancy are not candidates. Ordering is
    expected_partition, then asset, then existing list order. No partition
    value is special-cased.
    """
    ranked = [
        (index, item)
        for index, item in enumerate(state.arrival_items)
        if item.status == "ARRIVED" and item.target_partition_empty is True
    ]
    if not ranked:
        return None
    _, item = min(
        ranked,
        key=lambda pair: (
            (pair[1].expected_partition or "").strip(),
            (pair[1].asset or "").strip(),
            pair[0],
        ),
    )
    return item


def _unknown_occupancy_arrived(state: PipelineReliabilityState) -> bool:
    """True when an ARRIVED partition has not been checked for occupancy."""
    return any(
        item.status == "ARRIVED" and item.target_partition_empty is None
        for item in state.arrival_items
    )


def _decide_arrival_recovery(state: PipelineReliabilityState) -> str | None:
    """Next Late/Missing Partition action, or None to leave this playbook.

    None means either there are no arrival_items (not this incident class —
    keep the existing playbook) or every item is already VALIDATED (honor the
    existing completion contract; do not FINISH from VALIDATED alone).

    Mixed statuses recover what is already available: BACKFILLED before an
    eligible ARRIVED partition, then still-MISSING peers. An occupied ARRIVED
    target is not a backfill proposal. No I/O, no mutation.
    """
    if not state.arrival_items:
        return None

    if state.load_mode != "PARTITIONED":
        return ASK_HUMAN

    statuses = {item.status for item in state.arrival_items}
    if not statuses.issubset(_ARRIVAL_STATUSES):
        return ASK_HUMAN

    if "BACKFILLED" in statuses:
        return VALIDATE_PARTITION
    # Confirmed-empty ARRIVED partitions are the only write proposals.
    # Unknown occupancy still proposes so Guard can fail closed. A
    # confirmed-occupied target does not.
    if select_backfill_candidate(state) is not None or _unknown_occupancy_arrived(state):
        return BACKFILL_PARTITION
    if "UNKNOWN" in statuses:
        return CHECK_EXPECTED_INPUTS
    if "MISSING" in statuses:
        if state.sla_breached is True:
            return ASK_HUMAN
        if state.arrival_recheck_count >= MAX_ARRIVAL_RECHECKS:
            return ASK_HUMAN
        return WAIT_FOR_SOURCE

    # No eligible backfill and no source still to inspect. Occupied ARRIVED
    # targets land here. Caller uses completion; it does not invent a write.
    return None


def _action_from_completion(state: PipelineReliabilityState) -> str:
    """Map the completion contract to an Action. Decide still owns the Action."""
    completion = evaluate_completion(state)
    if completion == COMPLETION_UNKNOWN:
        return CHECK_WAREHOUSE_JOB
    if completion == COMPLETION_FAIL:
        writes_landed = (
            (state.rows_written is not None and state.rows_written > 0)
            or state.partial_write is True
            or state.warehouse_status == "FAILED"
        )
        return STOP_SAFE if writes_landed else ASK_HUMAN
    return FINISH


def _decide_playbook(state: PipelineReliabilityState) -> str:
    """Existing playbook. Do not call this from tests; use decide()."""
    # Uncertain BACKFILL mutation: inspect the recorded warehouse asset +
    # partition before any other recovery, including arrival BACKFILL /
    # VALIDATE. UNKNOWN is not FAILED and not SUCCEEDED.
    if state.backfill_side_effect == "UNKNOWN":
        return RECONCILE_BACKFILL

    # Already-classified errors with fixed playbooks — act before platform checks.
    # missing_schema is handled after evidence collection (see Rule 4 below).

    if state.error == "data_conflict":
        return STOP_SAFE

    if state.error == "permission_denied":
        return STOP_SAFE

    # Structured investigation — gather facts in order while fields are still empty.
    if not state.orchestrator_status:
        return CHECK_ORCHESTRATOR_RUN

    # Vendor/check timeout: not FAILED, not SUCCESS, not a license to RETRY.
    # Re-observe within the existing WAIT poll budget, then ASK_HUMAN.
    if state.orchestrator_status == "UNKNOWN":
        if state.poll_count < MAX_POLLS:
            return WAIT
        return ASK_HUMAN

    # Ambiguous RETRY mutation (timeout after dispatch, or crash after a
    # durable RETRY intent): empty orchestrator_status already forces CHECK.
    # Apply resolves UNKNOWN when identity shows a new attempt/repair, or
    # when no identity exists and status is SUCCESS/RUNNING. FAILED is not
    # proof the retry never happened. Do not RETRY or FINISH.
    if state.retry_side_effect == "UNKNOWN":
        if state.orchestrator_status == "RUNNING" and state.poll_count < MAX_POLLS:
            return WAIT
        return ASK_HUMAN

    # Severe volume drop is already classified on State (Tool/Adapter collected
    # baseline/observed; volume_status is the derived fact). Must run before the
    # generic orchestrator SUCCESS -> FINISH path so orchestrator success cannot
    # silently accept an incomplete file. Decide does not compute the 7-day
    # median or the 50% threshold here.
    if state.volume_status == "too_low":
        if state.rows_written is not None and state.rows_written > 0:
            return STOP_SAFE
        return ASK_HUMAN

    # Late / missing PARTITIONED source — already-collected arrival_items.
    # Must run before file_present WAIT and before timeout RETRY: a late
    # partition is not a missing object stub and not a transient task timeout.
    # Empty arrival_items leaves this path immediately (existing playbook).
    arrival_action = _decide_arrival_recovery(state)
    if arrival_action is not None:
        return arrival_action
    if state.arrival_items:
        # All items VALIDATED. Existing completion contract still owns FINISH.
        # Orchestrator FAILED stays FAIL in completion.py (limitation kept).
        return _action_from_completion(state)

    # Missing / late source file — already-collected observations.
    # Must run before SUCCESS -> FINISH and before timeout RETRY: an absent
    # object is not a recovered run and not a transient execution timeout.
    # Decide does not call GCS or compute the arrival SLA here.
    if state.file_present is False:
        if state.sla_breached:
            return ASK_HUMAN
        return WAIT

    # Wrong business date / partition — already-collected observations.
    # Compare only when both sides of a pair are known. Must run before
    # SUCCESS -> FINISH and before timeout RETRY: orchestrator success does
    # not mean the load wrote the intended date. Decide does not rewrite
    # dates or redirect partitions.
    date_mismatch = (
        state.expected_business_date is not None
        and state.observed_business_date is not None
        and state.expected_business_date != state.observed_business_date
    )
    partition_mismatch = (
        state.expected_partition is not None
        and state.target_partition is not None
        and state.expected_partition != state.target_partition
    )
    if date_mismatch or partition_mismatch:
        if state.rows_written is not None and state.rows_written > 0:
            return STOP_SAFE
        return ASK_HUMAN

    # Source format contract — already-collected expected vs observed.
    # Compare only when both sides of a pair are known. Must run before
    # SUCCESS -> FINISH and before timeout RETRY: a delimiter or encoding
    # mismatch is not a recovered run and not a transient execution timeout.
    # Decide does not sniff, guess, re-encode, or change parser config.
    delimiter_mismatch = (
        state.expected_delimiter is not None
        and state.observed_delimiter is not None
        and state.expected_delimiter != state.observed_delimiter
    )
    encoding_mismatch = (
        state.expected_encoding is not None
        and state.observed_encoding is not None
        and state.expected_encoding != state.observed_encoding
    )
    if delimiter_mismatch or encoding_mismatch:
        return ASK_HUMAN

    # Duplicate / re-delivered source file — already-collected identity + history.
    # prior_load_succeeded is a *previous* run's success for this business date,
    # not "the current run succeeded." Both checksums must be known; Decide
    # does not invent identity from filename, size, or row count.
    # Identical vs different checksums share the same policy: a second
    # delivery may be an accidental duplicate, an exact resend, a
    # restatement, or a legitimate replay. The Agent must not DELETE,
    # overwrite, reload, or deduplicate. Must run before SUCCESS -> FINISH
    # and before timeout RETRY.
    possible_redelivery = (
        state.prior_load_succeeded is True
        and state.expected_business_date is not None
        and state.source_checksum is not None
        and state.prior_source_checksum is not None
    )
    if possible_redelivery:
        if state.rows_written is not None and state.rows_written > 0:
            return STOP_SAFE
        return ASK_HUMAN

    # After a controlled retry, re-observation may show the run recovered.
    # Completion Contract owns "is this successfully complete?" Decide maps
    # that verdict to an Action. Never FINISH from retry acceptance.
    # Orchestrator SUCCESS is not warehouse confirmation. Orchestrator FAILED /
    # RUNNING / HITL never enter here.
    if state.orchestrator_status == "SUCCESS":
        # A warehouse fact from the attempt that was retried is not evidence
        # for this one. Re-read it before completion. UNKNOWN and RUNNING are
        # not terminal results for the new attempt.
        if state.warehouse_stale or state.warehouse_status in {"", "UNKNOWN", "RUNNING"}:
            return CHECK_WAREHOUSE_JOB
        # Reuse existing policy: writes / split-stage warehouse failure
        # already landed → STOP_SAFE. Known-unhealthy with no write →
        # ASK_HUMAN (same as too_low / mismatch playbooks above).
        return _action_from_completion(state)

    # Orchestrator still running after clear/retry — wait and re-check, bounded.
    # RUNNING is not SUCCESS and must not escalate immediately.
    if state.orchestrator_status == "RUNNING":
        if state.poll_count < MAX_POLLS:
            return WAIT
        return ASK_HUMAN

    # Intelligence already used its one repair and still produced no
    # acceptable facts. This is an investigation fact Apply recorded —
    # Intelligence does not choose ASK_HUMAN. Stop re-reading the same log.
    if state.intelligence_exhausted:
        return ASK_HUMAN

    if not state.error:
        # Unclassified after warehouse + downstream were already collected.
        # Re-GET_TASK_LOG would loop; FINISH would close without a human.
        if state.warehouse_status and state.downstream_impact:
            return ASK_HUMAN
        # After one log read, continue the existing investigation playbook.
        # Without this fall-through, empty error would re-GET_TASK_LOG forever
        # and never collect warehouse / downstream evidence.
        if not state.task_log_checked:
            return GET_TASK_LOG

    if not state.warehouse_status:
        return CHECK_WAREHOUSE_JOB

    # Warehouse check timed out or the load is still committing — not a
    # license to RETRY. Re-observe within the existing WAIT poll budget.
    if state.warehouse_status in {"UNKNOWN", "RUNNING"}:
        if state.poll_count < MAX_POLLS:
            return WAIT
        return ASK_HUMAN

    if not state.downstream_impact:
        return CHECK_DOWNSTREAM_IMPACT

    # Warehouse already succeeded with data — do not retry blindly (orchestrator
    # may have timed out while the load finished).
    if (
        state.warehouse_status == "SUCCEEDED"
        and state.rows_written is not None
        and state.rows_written > 0
    ):
        return STOP_SAFE

    # Cross-cutting rule — known critical downstream, do not propose RETRY
    # WHY: If dashboards, revenue pipelines, or SLAs depend on this job, an
    # unattended retry can make things worse (duplicate loads, partial writes).
    # Once impact is already known to be critical, stop safely instead of
    # proposing RETRY and relying on Guard to reject it.
    if state.downstream_impact == "critical" and state.error == "timeout":
        return STOP_SAFE

    # Rule 2 — transient timeout, first retry allowed
    # WHY: Timeouts are often flaky infra or queue backlog. One retry is cheap
    # and resolves many incidents without human involvement.
    if state.error == "timeout" and state.retries < 1:
        return RETRY

    # Rule 3 — timeout persisted after a retry
    # WHY: A second automatic retry on the same timeout usually wastes time.
    # Escalate to a human who can check upstream systems or extend the window.
    if state.error == "timeout" and state.retries >= 1:
        return ASK_HUMAN

    # Rule 4 — schema mismatch (after orchestrator, log, warehouse, downstream gathered)
    # WHY: Missing columns or tables need a code or contract change. The agent
    # cannot invent a repair. A human-approved exact recipe may be reused once;
    # otherwise escalate. RETRY here is the pipeline rerun after a validated
    # repair — not a blind retry of the drifted file.
    if state.error == "missing_schema":
        if (
            state.repair_recipe_found is True
            and state.repair_recipe_approved is True
            and state.repair_applied is None
        ):
            return APPLY_APPROVED_REPAIR
        if (
            state.repair_applied is True
            and state.repair_validation_passed is True
            and state.retries < 1
        ):
            return RETRY
        return ASK_HUMAN

    # Rule 5 — conflicting data
    # WHY: Blind retries can duplicate or corrupt facts. Stop safely and preserve
    # the current state for investigation instead of writing more bad data.
    # (Handled above before platform checks.)

    # Rule 6 — permission failure
    # WHY: Retrying will not grant IAM roles or warehouse grants. Continuing
    # automated recovery is pointless and noisy; stop and surface the auth issue.
    # (Handled above before platform checks.)

    # Rule 7 — unknown or unclassified error
    # WHY: No safe automated playbook. ASK_HUMAN so a human can review any
    # advisory analysis. FINISH would close an unclassified incident.
    return ASK_HUMAN
