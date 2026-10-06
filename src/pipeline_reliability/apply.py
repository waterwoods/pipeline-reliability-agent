"""Pipeline Reliability Agent — Apply.

Full loop:

    State -> Decide -> Guard -> Execute -> Tool -> Observation -> Apply -> New State

Execute/Tool performs work and returns a string report (the Observation).
Apply reads that report and updates State so the *next* Decide pass sees a
new world.

Why Apply is separate from Execute
---------------------------------
Execute talks to the outside world (or mocks it). Apply updates the agent's
memory. Mixing them causes trouble:

- Tools stay testable: pass State in, get observation out, State unchanged.
- Apply has one job: interpret observations with consistent rules.
- You can replay the same observation through Apply in tests without re-running
  a pipeline or API call.

Terminology
-----------
action      — what Decide proposed and Guard allowed (e.g. "RETRY").
observation — what the tool reported this turn (e.g. "still_timeout").
              Overwritten each loop turn: "what we just learned."
evidence    — append-only list of every observation; never overwritten.
              Accumulates so Guard and humans can audit the full story.
outcome     — terminal or phase status (e.g. "ASK_HUMAN", "FINISH").
              None means the incident is still open; the loop continues.

This version mutates the dataclass in place for clarity. Same object is returned.
"""

from __future__ import annotations

import re

from pipeline_reliability.checkpoint import freeze_retry_baseline
from pipeline_reliability.decide import STOP_SAFE
from pipeline_reliability.recovery import classify_write_risk, next_wait_backoff_seconds
from pipeline_reliability.facts import (
    ALLOWED_FACT_KEYS,
    FactProposal,
    validate_fact_proposal,
)
from pipeline_reliability.intelligence import FactIntelligence
from pipeline_reliability.observability import (
    INTELLIGENCE_EXHAUSTED,
    LLM_PROPOSAL,
    LLM_REPAIR,
    VALIDATION_ACCEPTED,
    VALIDATION_REJECTED,
    IntelligenceTraceEvent,
    RunTrace,
)
from pipeline_reliability.state import PipelineReliabilityState
from pipeline_reliability.repair_recipes import lookup_schema_drift_recipe
from pipeline_reliability.tools import (
    WAIT_FOR_SOURCE_OBSERVATION,
    WAIT_OBSERVATION,
    BackfillResult,
    BackfillReconcileResult,
    PartitionValidationResult,
    OrchestratorRunResult,
    WarehouseJobResult,
    DownstreamImpactResult,
    ExpectedInputsResult,
    RepairResult,
    RetryResult,
    TaskLogResult,
)

_LOG_ERROR_TYPE_TO_STATE: dict[str, str] = {
    "TimeoutError": "timeout",
    "SchemaError": "missing_schema",
    "PermissionDenied": "permission_denied",
    "DataConflict": "data_conflict",
}


def _default_escalation_reason(state: PipelineReliabilityState) -> str:
    """Best-effort why ASK_HUMAN fired, when Apply has not set a specific reason."""
    if state.retry_side_effect == "UNKNOWN":
        return (
            "Previous RETRY side effect is UNKNOWN; "
            "reconcile with orchestrator before another mutation."
        )
    if state.backfill_side_effect == "UNKNOWN":
        return (
            "Previous BACKFILL side effect is UNKNOWN; "
            "warehouse evidence could not prove the partition landed."
        )
    if state.orchestrator_status == "UNKNOWN":
        return "orchestrator status is UNKNOWN after the bounded poll budget."
    if state.volume_status == "too_low":
        return "Observed volume is too_low relative to baseline."
    if state.file_present is False and state.sla_breached:
        return "Expected source object is missing and the arrival SLA is breached."
    if state.intelligence_exhausted:
        return "Intelligence path exhausted; unstructured log still has no accepted facts."
    if (
        not state.error
        and state.warehouse_status
        and state.downstream_impact
    ):
        return (
            "Unclassified incident after investigation; "
            "advisory analysis is for a human, not automated recovery."
        )
    if state.error == "timeout" and state.retries >= 1:
        return "Timeout persisted after an automatic retry."
    if state.error == "missing_schema":
        return "Schema mismatch detected; automatic retry cannot repair missing columns."
    if state.orchestrator_status == "RUNNING":
        return "orchestrator still RUNNING after the bounded poll budget."
    return state.observation or "Agent escalated to a human."


def _record_observation(
    state: PipelineReliabilityState,
    action: str,
    observation_text: str,
) -> None:
    """Write shared observation/evidence fields used by every Apply path."""
    state.attempted_action = action
    state.observation = observation_text
    state.evidence.append(observation_text)


def _identity_available(
    state: PipelineReliabilityState,
    observation: OrchestratorRunResult,
) -> bool:
    """True when Databricks-style identity exists to compare against status."""
    return (
        observation.attempt_number is not None
        or bool(observation.latest_repair_id)
        or state.retry_baseline_attempt_number is not None
        or bool(state.retry_baseline_repair_id)
    )


def _repair_identity_advanced(
    state: PipelineReliabilityState,
    observation: OrchestratorRunResult,
) -> bool:
    """True when a later CHECK shows a new attempt or a new repair id."""
    baseline_attempt = state.retry_baseline_attempt_number
    observed_attempt = observation.attempt_number
    if (
        baseline_attempt is not None
        and observed_attempt is not None
        and observed_attempt > baseline_attempt
    ):
        return True
    observed_repair = observation.latest_repair_id or ""
    baseline_repair = state.retry_baseline_repair_id or ""
    if observed_repair and observed_repair != baseline_repair:
        return True
    return False


def _identity_advance_reason(
    state: PipelineReliabilityState,
    observation: OrchestratorRunResult,
) -> str:
    """Explain an identity-based UNKNOWN clear. Facts only, not authorization."""
    baseline_attempt = state.retry_baseline_attempt_number
    observed_attempt = observation.attempt_number
    if (
        baseline_attempt is not None
        and observed_attempt is not None
        and observed_attempt > baseline_attempt
    ):
        return (
            "UNKNOWN cleared because attempt_number advanced "
            f"{baseline_attempt} -> {observed_attempt}"
        )
    observed_repair = observation.latest_repair_id or ""
    baseline_repair = state.retry_baseline_repair_id or ""
    if observed_repair and observed_repair != baseline_repair:
        return (
            "UNKNOWN cleared because repair_id changed "
            f"{baseline_repair} -> {observed_repair}"
        )
    return ""


def _mark_prior_attempt_warehouse_stale(state: PipelineReliabilityState) -> None:
    """Warehouse facts from the failed attempt are not evidence for the new one.

    Shared by an accepted RetryResult and by reconciliation after external
    evidence already proves the UNKNOWN RETRY took effect. Evidence history
    stays. Orchestrator status is not cleared here: the accepted path clears
    it itself because that observation is not a fresh orchestrator read.
    """
    state.warehouse_stale = True
    state.poll_count = 0
    state.wait_backoff_seconds = 0.0


def _resolve_unknown_retry(
    state: PipelineReliabilityState,
    observation: OrchestratorRunResult,
    *,
    retry_took_effect: bool,
) -> None:
    """Clear UNKNOWN after the reconciliation branch already chose its evidence.

    ``retry_took_effect`` is true only for proof the RETRY landed: identity
    advanced, or SUCCESS. RUNNING without that proof still clears UNKNOWN so
    resume does not dispatch another clear, and must not mark warehouse facts
    stale.
    """
    state.retry_side_effect = ""
    if state.retries < 1:
        state.retries = 1
    if retry_took_effect:
        _mark_prior_attempt_warehouse_stale(state)
    elif observation.status == "RUNNING":
        state.poll_count = 0
        state.wait_backoff_seconds = 0.0


def _record_orchestrator_identity(
    state: PipelineReliabilityState,
    observation: OrchestratorRunResult,
) -> None:
    """Keep the latest observed identity; do not wipe it on a timed-out CHECK."""
    if observation.attempt_number is not None:
        state.attempt_number = observation.attempt_number
    if observation.latest_repair_id:
        state.latest_repair_id = observation.latest_repair_id


def _reconcile_unknown_retry(
    state: PipelineReliabilityState,
    observation: OrchestratorRunResult,
) -> str:
    """Resolve an unresolved RETRY only from evidence the clear already landed.

    Attempt advance is that evidence. SUCCESS is that evidence even when the
    attempt number has not moved yet. Those proofs also mark the failed
    attempt's warehouse facts stale, matching an accepted RetryResult, so
    completion cannot STOP or FINISH on them. RUNNING may already be the new
    attempt, so it must not authorize another clear; when an attempt baseline
    exists, RUNNING at that same attempt stays UNKNOWN. RUNNING without
    identity clears UNKNOWN only to avoid a second clear and does not mark
    the warehouse stale. The same attempt still FAILED is a stale read, not
    proof the clear never ran, and stays UNKNOWN. Unreadable status stays
    UNKNOWN. None of these paths issue a clear.
    """
    if state.retry_side_effect != "UNKNOWN":
        return ""
    if _repair_identity_advanced(state, observation):
        reason = _identity_advance_reason(state, observation)
        _resolve_unknown_retry(state, observation, retry_took_effect=True)
        return reason
    if observation.status == "SUCCESS":
        _resolve_unknown_retry(state, observation, retry_took_effect=True)
        return ""
    if _identity_available(state, observation):
        return ""
    if observation.status == "RUNNING":
        _resolve_unknown_retry(state, observation, retry_took_effect=False)
        return ""
    return ""


_INTELLIGENCE_REPAIR_FAILURE_REASON = (
    "Intelligence repair failed to produce a FactProposal."
)
_INTELLIGENCE_EXHAUSTED_REASON = (
    "Intelligence path exhausted after one-shot repair."
)
RETRY_DISPATCH_TIMEOUT_APPLY_REASON = (
    "retry side effect became UNKNOWN because dispatch timed out after possible mutation"
)
_PROTECTED_ARRIVAL_STATUSES = frozenset({"BACKFILLED", "VALIDATED"})


def _record_intelligence_event(
    trace: RunTrace | None,
    event_type: str,
    *,
    proposal: FactProposal | None = None,
    accepted: bool | None = None,
    reason: str = "",
) -> None:
    """Append an observation-only Intelligence event. No-op when trace is absent."""
    if trace is None:
        return
    snapshot = None
    if proposal is not None:
        snapshot = FactProposal(
            failure_type=proposal.failure_type,
            facts=dict(proposal.facts),
            confidence=proposal.confidence,
            evidence=proposal.evidence,
        )
    trace.intelligence_events.append(
        IntelligenceTraceEvent(
            event_type=event_type,
            proposal=snapshot,
            accepted=accepted,
            reason=reason,
        )
    )


_SCHEMA_DRIFT_TOKEN_RE = re.compile(r"\b(type|expected|observed|changed)=(\S+)")
_SCHEMA_DRIFT_SOURCE_RE = re.compile(r"\bsource=(\S+)")


def _apply_schema_drift_marker(state: PipelineReliabilityState, blob: str) -> None:
    """Copy DAG-provided SCHEMA_DRIFT tokens onto State. Does not classify drift."""
    tokens = dict(_SCHEMA_DRIFT_TOKEN_RE.findall(blob))
    if tokens.get("expected"):
        state.expected_schema = tokens["expected"]
    if tokens.get("observed"):
        state.observed_schema = tokens["observed"]
    if tokens.get("type"):
        state.drift_type = tokens["type"]
    if tokens.get("changed"):
        state.changed_fields = tokens["changed"]
    source_match = _SCHEMA_DRIFT_SOURCE_RE.search(blob)
    if source_match and not (state.expected_object or "").strip():
        state.expected_object = source_match.group(1)
    # Deterministic fact read of the human-approved store. Not a mutation.
    recipe = lookup_schema_drift_recipe(state)
    if recipe is None:
        state.repair_recipe_found = False
        state.repair_recipe_approved = False
        state.repair_recipe_id = ""
    else:
        state.repair_recipe_found = True
        state.repair_recipe_approved = recipe.status == "APPROVED"
        state.repair_recipe_id = recipe.recipe_id
    if not state.error:
        state.error = "missing_schema"


def _task_log_handled_deterministically(observation: TaskLogResult) -> bool:
    """True when V1 already classified or extracted facts from this log."""
    blob = f"{observation.message}\n{observation.detail}"
    if "SOURCE_FILE_MISSING" in blob:
        return True
    if "DATA_VALIDATION_FAILED" in blob:
        return True
    if "SCHEMA_DRIFT" in blob:
        return True
    if (
        "Error while reading data" in blob
        or "CSV processing encountered too many errors" in blob
    ):
        return True
    return observation.error_type in _LOG_ERROR_TYPE_TO_STATE


def _apply_intelligence_fallback(
    state: PipelineReliabilityState,
    observation: TaskLogResult,
    intelligence: FactIntelligence,
    trace: RunTrace | None = None,
) -> None:
    """Propose → validate → optional one-shot repair → safe-apply.

    Rejected proposals never write fact fields. Completeness failures
    (``validation.incomplete``) get exactly one ``repair_facts`` call.
    If that repair is rejected or the repair call fails, Apply records
    ``intelligence_exhausted=True`` (investigation fact, not an Action).
    First-proposal accept, illegal/low-confidence first proposals, and
    propose API failures do not set exhausted.

    ``trace`` is observation only. Recording events must not change State,
    validator results, or Action authority.
    """
    try:
        proposal = intelligence.propose_facts(observation)
    except Exception:
        # Call/JSON/conversion failure: do not write facts, do not repair.
        return
    _record_intelligence_event(trace, LLM_PROPOSAL, proposal=proposal)
    validation = validate_fact_proposal(proposal)
    if validation.accepted:
        _record_intelligence_event(
            trace,
            VALIDATION_ACCEPTED,
            proposal=proposal,
            accepted=True,
            reason=validation.reason,
        )
        apply_validated_fact_proposal(state, proposal)
        return
    _record_intelligence_event(
        trace,
        VALIDATION_REJECTED,
        proposal=proposal,
        accepted=False,
        reason=validation.reason,
    )
    if not validation.incomplete:
        return
    try:
        repaired = intelligence.repair_facts(
            observation,
            proposal,
            validation.reason,
        )
    except Exception:
        state.intelligence_exhausted = True
        _record_intelligence_event(
            trace,
            INTELLIGENCE_EXHAUSTED,
            accepted=False,
            reason=_INTELLIGENCE_REPAIR_FAILURE_REASON,
        )
        return
    _record_intelligence_event(trace, LLM_REPAIR, proposal=repaired)
    repaired_validation = validate_fact_proposal(repaired)
    if not repaired_validation.accepted:
        _record_intelligence_event(
            trace,
            VALIDATION_REJECTED,
            proposal=repaired,
            accepted=False,
            reason=repaired_validation.reason,
        )
        state.intelligence_exhausted = True
        _record_intelligence_event(
            trace,
            INTELLIGENCE_EXHAUSTED,
            accepted=False,
            reason=_INTELLIGENCE_EXHAUSTED_REASON,
        )
        return
    _record_intelligence_event(
        trace,
        VALIDATION_ACCEPTED,
        proposal=repaired,
        accepted=True,
        reason=repaired_validation.reason,
    )
    apply_validated_fact_proposal(state, repaired)


def _apply_expected_inputs(
    state: PipelineReliabilityState,
    observation: ExpectedInputsResult,
) -> None:
    """Copy CHECK_EXPECTED_INPUTS facts onto matching ArrivalItems.

    Does not add/remove items. Does not downgrade BACKFILLED or VALIDATED.
    Occupancy is always written; status follows source_present otherwise.
    """
    observed = {
        (item.asset, item.expected_partition): item for item in observation.items
    }
    for arrival in state.arrival_items:
        match = observed.get((arrival.asset, arrival.expected_partition))
        if match is None:
            continue
        arrival.target_partition_empty = match.target_partition_empty
        if arrival.status in _PROTECTED_ARRIVAL_STATUSES:
            continue
        if match.source_present is True:
            arrival.status = "ARRIVED"
        elif match.source_present is False:
            arrival.status = "MISSING"
        else:
            arrival.status = "UNKNOWN"
    state.sla_breached = observation.sla_breached


def _clear_backfill_intent(state: PipelineReliabilityState) -> None:
    """Resolved backfill mutation: no remaining uncertain warehouse write."""
    state.backfill_side_effect = ""
    state.backfill_asset = ""
    state.backfill_partition = ""


def _matching_arrival_item(state: PipelineReliabilityState, asset: str, partition: str):
    """ArrivalItem whose identity matches this asset + partition, or None."""
    for item in state.arrival_items:
        if item.asset == asset and item.expected_partition == partition:
            return item
    return None


def _apply_backfill_result(
    state: PipelineReliabilityState,
    observation: BackfillResult,
) -> None:
    """Merge one BackfillResult. Does not validate or complete the incident.

    SUCCEEDED marks the matching item BACKFILLED and clears UNKNOWN.
    UNKNOWN keeps the durable intent and does not mark BACKFILLED.
    FAILED does not mark BACKFILLED and stops safely so the loop cannot
    replay the same write. Uncertain intent is resolved by
    RECONCILE_BACKFILL, not by replaying this write.
    """
    uncertain = (
        observation.status == "UNKNOWN" or observation.side_effect == "UNKNOWN"
    )
    if observation.status == "SUCCEEDED" and not uncertain:
        item = _matching_arrival_item(
            state, observation.asset, observation.expected_partition
        )
        if item is not None:
            item.status = "BACKFILLED"
        _clear_backfill_intent(state)
        return

    if uncertain:
        state.backfill_side_effect = "UNKNOWN"
        if observation.asset:
            state.backfill_asset = observation.asset
        if observation.expected_partition:
            state.backfill_partition = observation.expected_partition
        return

    # FAILED or any other definite non-success: do not mark BACKFILLED,
    # do not replay. Intent is resolved as a failed attempt.
    _clear_backfill_intent(state)
    state.outcome = "STOP_SAFE"


def _apply_backfill_reconcile(
    state: PipelineReliabilityState,
    observation: BackfillReconcileResult,
) -> None:
    """Merge one read-only BackfillReconcileResult. Does not write the warehouse.

    LANDED: matching item becomes BACKFILLED; intent is cleared.
    NOT_LANDED: matching item becomes ARRIVED with an empty target; intent
    is cleared so Guard may authorize a new BACKFILL.
    UNKNOWN: keep the intent, do not replay, pause for a human.
    """
    if observation.status == "LANDED":
        item = _matching_arrival_item(
            state, observation.asset, observation.expected_partition
        )
        if item is not None:
            item.status = "BACKFILLED"
        _clear_backfill_intent(state)
        return

    if observation.status == "NOT_LANDED":
        item = _matching_arrival_item(
            state, observation.asset, observation.expected_partition
        )
        if item is not None:
            item.status = "ARRIVED"
            item.target_partition_empty = True
        _clear_backfill_intent(state)
        return

    # Cannot prove landed or empty. Keep UNKNOWN intent. Do not BACKFILL.
    if not state.escalation_reason:
        state.escalation_reason = (
            "Previous BACKFILL side effect is UNKNOWN; "
            "warehouse evidence could not prove the partition landed."
        )
    state.outcome = "ASK_HUMAN"
    state.awaiting_human = True


def _apply_partition_validation(
    state: PipelineReliabilityState,
    observation: PartitionValidationResult,
) -> None:
    """Merge one PartitionValidationResult. Does not backfill or FINISH.

    passed True: matching BACKFILLED item becomes VALIDATED.
    passed False: do not mark VALIDATED; STOP_SAFE so the loop cannot
    backfill again automatically or close as recovered.
    """
    if observation.passed:
        item = _matching_arrival_item(
            state, observation.asset, observation.expected_partition
        )
        if item is not None and item.status == "BACKFILLED":
            item.status = "VALIDATED"
        return

    state.outcome = "STOP_SAFE"


def apply(
    state: PipelineReliabilityState,
    action: str,
    observation: str
    | OrchestratorRunResult
    | WarehouseJobResult
    | TaskLogResult
    | DownstreamImpactResult
    | ExpectedInputsResult
    | RetryResult
    | RepairResult
    | BackfillResult
    | BackfillReconcileResult
    | PartitionValidationResult,
    intelligence: FactIntelligence | None = None,
    trace: RunTrace | None = None,
) -> PipelineReliabilityState:
    """Merge action + observation into State; return updated State for the next loop.

    ``intelligence`` is optional fallback for unstructured TaskLogResult only.
    It never runs when deterministic parsers already handled the log, and it
    never writes State directly — only a validated FactProposal may.

    ``trace`` is optional observation only. Omit it to skip Intelligence events
    and apply_reason. When present, Apply writes ``trace.last_apply_reason``
    for the current turn (runner copies it onto TraceStep).
    """
    if trace is not None:
        trace.last_apply_reason = ""

    if isinstance(observation, OrchestratorRunResult):
        observation_text = observation.detail or f"orchestrator:{observation.status}"
        _record_observation(state, action, observation_text)
        state.orchestrator_status = observation.status
        if observation.failed_task_id:
            state.task_id = observation.failed_task_id
        _record_orchestrator_identity(state, observation)
        reason = _reconcile_unknown_retry(state, observation)
        if trace is not None:
            trace.last_apply_reason = reason
        return state

    if isinstance(observation, WarehouseJobResult):
        observation_text = observation.detail or (
            f"warehouse:{observation.status}:{observation.rows_written} rows"
        )
        _record_observation(state, action, observation_text)
        # A post-retry read counts only when it is a terminal fact for a job
        # that was actually observed. Missing ids, UNKNOWN, and RUNNING stay
        # stale so completion cannot treat them as the new attempt.
        terminal = observation.authoritative and observation.status in {
            "SUCCEEDED",
            "FAILED",
            "NO_JOB",
        }
        if state.warehouse_stale and not terminal:
            return state
        state.warehouse_status = observation.status
        state.rows_written = observation.rows_written
        state.warehouse_stale = False
        classify_write_risk(state)
        return state

    if isinstance(observation, TaskLogResult):
        observation_text = observation.detail or observation.message or (
            f"log:{observation.error_type}"
        )
        _record_observation(state, action, observation_text)
        state.task_log_checked = True
        mapped_error = _LOG_ERROR_TYPE_TO_STATE.get(observation.error_type)
        if mapped_error:
            state.error = mapped_error
        # Stable DAG marker → State fact. Decide already has the playbook.
        blob = f"{observation.message}\n{observation.detail}"
        if "SOURCE_FILE_MISSING" in blob:
            state.file_present = False
            start = blob.find("gs://")
            if start >= 0:
                state.expected_object = blob[start:].split()[0].rstrip(".,;:)'\"")
        if "DATA_VALIDATION_FAILED" in blob:
            match = re.search(r"has (\d+) rows", blob)
            if match:
                observed = int(match.group(1))
                state.observed_rows = observed
                if observed <= 0:
                    state.volume_status = "too_low"
        if "SCHEMA_DRIFT" in blob:
            _apply_schema_drift_marker(state, blob)
        # Stable BigQuery CSV load-failure marker → existing format-contract
        # facts. Decide already ASK_HUMAN on delimiter mismatch.
        # "invalid" is malformed-CSV evidence for this PoW bridge, not a
        # literal observed delimiter character.
        # SCHEMA_DRIFT is a column-contract failure, not a delimiter mismatch.
        elif (
            "Error while reading data" in blob
            or "CSV processing encountered too many errors" in blob
        ):
            state.expected_delimiter = ","
            state.observed_delimiter = "invalid"
        if intelligence is not None and not _task_log_handled_deterministically(
            observation
        ):
            _apply_intelligence_fallback(
                state, observation, intelligence, trace=trace
            )
        return state

    if isinstance(observation, DownstreamImpactResult):
        observation_text = observation.detail or f"downstream:{observation.impact}"
        _record_observation(state, action, observation_text)
        state.downstream_impact = observation.impact
        return state

    if isinstance(observation, ExpectedInputsResult):
        observation_text = observation.detail or (
            f"expected_inputs:{len(observation.items)} item(s); "
            f"sla_breached={observation.sla_breached}"
        )
        _record_observation(state, action, observation_text)
        _apply_expected_inputs(state, observation)
        return state

    if isinstance(observation, RepairResult):
        observation_text = observation.detail or (
            f"repair:applied={observation.applied}:validated={observation.validated}"
        )
        _record_observation(state, action, observation_text)
        state.repair_applied = observation.applied
        state.repair_validation_passed = observation.validated
        if observation.recipe_id:
            state.repair_recipe_id = observation.recipe_id
        if observation.applied:
            state.repair_recipe_found = True
        return state

    if isinstance(observation, BackfillResult):
        observation_text = observation.detail or (
            f"backfill:{observation.asset}:{observation.expected_partition}:"
            f"{observation.status}"
        )
        _record_observation(state, action, observation_text)
        _apply_backfill_result(state, observation)
        return state

    if isinstance(observation, BackfillReconcileResult):
        observation_text = observation.detail or (
            f"reconcile_backfill:{observation.asset}:"
            f"{observation.expected_partition}:{observation.status}"
        )
        _record_observation(state, action, observation_text)
        _apply_backfill_reconcile(state, observation)
        return state

    if isinstance(observation, PartitionValidationResult):
        observation_text = observation.detail or (
            f"validate:{observation.asset}:{observation.expected_partition}:"
            f"{'passed' if observation.passed else 'failed'}"
        )
        _record_observation(state, action, observation_text)
        _apply_partition_validation(state, observation)
        return state

    if isinstance(observation, RetryResult):
        # Retry acceptance is not recovery. Count the attempt, invalidate stale
        # orchestrator status so the loop re-observes, and leave outcome unset.
        if observation.side_effect == "UNKNOWN":
            # Command timeout after dispatch: preserve the attempt, do not
            # treat it as accepted or rejected, force orchestrator reconciliation.
            observation_text = (
                observation.detail or "retry_request_timeout_side_effect_unknown"
            )
            _record_observation(state, action, observation_text)
            freeze_retry_baseline(state)
            state.retry_side_effect = "UNKNOWN"
            state.orchestrator_status = ""
            if trace is not None:
                trace.last_apply_reason = RETRY_DISPATCH_TIMEOUT_APPLY_REASON
            return state

        # Confirmed yes or no. The pre-dispatch intent is no longer unresolved.
        state.retry_side_effect = ""

        if observation.accepted:
            observation_text = (
                observation.detail or "retry_request_accepted"
            )
        else:
            observation_text = observation.detail or "retry_request_rejected"
        _record_observation(state, action, observation_text)

        if observation.accepted:
            state.retries += 1
            # The accepted retry starts a new attempt. Orchestrator and warehouse
            # facts from the failed attempt are not evidence for it. Clearing
            # orchestrator status forces CHECK_ORCHESTRATOR_RUN, then
            # CHECK_WAREHOUSE_JOB after that check reports SUCCESS. Evidence
            # history stays. Acceptance is not FINISH.
            state.orchestrator_status = ""
            _mark_prior_attempt_warehouse_stale(state)
        elif observation.detail.startswith("pre-retry revalidation"):
            # World changed under us (e.g. warehouse finished) — stop safely; no mutation.
            state.outcome = "STOP_SAFE"
        else:
            # Adapter was reached but the orchestrator rejected/errored — consume budget.
            state.retries += 1
        return state

    # --- General updates (every string observation, known or unknown) ---------
    _record_observation(state, action, observation)

    # --- Observation-specific rules -------------------------------------------

    if observation == WAIT_OBSERVATION:
        # Bounded poll completed — consume one poll, invalidate stale orchestrator
        # status so Decide chooses CHECK_ORCHESTRATOR_RUN next. Do not touch retries.
        state.poll_count += 1
        state.wait_backoff_seconds = next_wait_backoff_seconds(state.poll_count)
        state.orchestrator_status = ""

    elif observation == WAIT_FOR_SOURCE_OBSERVATION:
        # Source wait completed — consume one arrival recheck. Old MISSING
        # facts are stale after the delay, so they become UNKNOWN and the
        # next Decide must CHECK_EXPECTED_INPUTS. Do not touch poll_count,
        # orchestrator_status, ARRIVED, BACKFILLED, or VALIDATED.
        state.arrival_recheck_count += 1
        for item in state.arrival_items:
            if item.status == "MISSING":
                item.status = "UNKNOWN"

    elif observation == "timeout_found_in_log":
        # Log check classified the failure — promote empty/unknown error to timeout.
        state.error = "timeout"

    elif observation == "still_timeout":
        # Legacy string observation — count the attempt, keep failing.
        state.retries += 1
        state.error = "timeout"
        # outcome stays None: incident not resolved; Decide should escalate next.

    elif observation == "pipeline_recovered":
        # Legacy string observation — count the attempt, clear error, close.
        state.retries += 1
        state.error = ""
        state.outcome = "FINISH"

    elif observation == "ASK_HUMAN":
        # Human escalation path — pause for HITL; loop stops until a decision.
        state.outcome = "ASK_HUMAN"
        state.awaiting_human = True
        if not state.escalation_reason:
            state.escalation_reason = _default_escalation_reason(state)
        if state.error == "missing_schema":
            # Preserve why we paused and what a safe human might do (V1: no auto schema fix).
            state.suggested_action = STOP_SAFE

    elif observation == "STOP_SAFE":
        # Safe stop path — record outcome, no further unattended recovery.
        state.outcome = "STOP_SAFE"

    elif observation == "FINISH":
        # Explicit finish signal from tool/router — close without inventing success.
        state.outcome = "FINISH"

    # Unknown observations: general updates above already ran.
    # Do not crash, do not set outcome — let Decide handle ambiguity next turn.

    return state


def apply_validated_fact_proposal(
    state: PipelineReliabilityState,
    proposal: FactProposal,
) -> PipelineReliabilityState:
    """Copy accepted FactProposal facts into State. Does not choose an Action.

    Always re-runs deterministic validation. Rejected proposals raise
    ValueError and leave State unchanged. failure_type and confidence are
    not written and do not select RETRY / ASK_HUMAN.
    """
    result = validate_fact_proposal(proposal)
    if not result.accepted:
        raise ValueError(result.reason)
    for key, value in proposal.facts.items():
        if key in ALLOWED_FACT_KEYS:
            setattr(state, key, value)
    return state


def approve(state: PipelineReliabilityState, action: str) -> PipelineReliabilityState:
    """Record a human-approved action as an input and allow run_agent to resume.

    APPROVE is not Execute. Runner still passes the action through Guard.
    """
    if not state.awaiting_human:
        raise ValueError("Cannot approve: workflow is not awaiting human input.")
    state.awaiting_human = False
    state.outcome = None
    state.approved_action = action
    state.last_human_decision = "APPROVE"
    note = f"human_approved: authorized action {action}"
    state.observation = note
    state.evidence.append(note)
    return state


def reject(state: PipelineReliabilityState, reason: str = "") -> PipelineReliabilityState:
    """Decline the suggested HITL action. Does not terminate the incident.

    REJECT is not Execute and is not an automatic close. Clears the pause so
    the Agent can resume; Decide chooses the next safe behavior. A second
    ASK_HUMAN on the same snapshot becomes STOP_SAFE (see decide).
    """
    if not state.awaiting_human:
        raise ValueError("Cannot reject: workflow is not awaiting human input.")
    state.awaiting_human = False
    state.approved_action = None
    state.outcome = None
    state.last_human_decision = "REJECT"
    note = "human_rejected: suggested action declined"
    if reason.strip():
        note = f"{note}: {reason.strip()}"
    state.observation = note
    state.evidence.append(note)
    return state
