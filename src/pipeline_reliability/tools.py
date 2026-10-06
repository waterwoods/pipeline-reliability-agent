"""Pipeline Reliability Agent — Tools (Execute layer).

Loop position:

    State -> Decide -> Guard -> Execute/Tool -> Observation

    Decide  chooses what should happen next (no side effects).
    Guard   checks whether that action is allowed (no side effects).
    Tool    performs or simulates the operation and returns an observation.

Why Tools are separate from Decide
----------------------------------
Decide answers: "What is the best next move?"
Tools answer:   "Do the work and tell me what happened."

In production, tools are where side effects eventually live:
retrying an orchestrator task, inspecting a warehouse job, posting to Slack,
querying log storage. Decide and Guard must never do those things directly.

Why Tools no longer contain the fake external world
---------------------------------------------------
The scenario-specific "what does the orchestrator/warehouse say?" logic moved to
`adapters.py`. Tools are now the Agent-facing surface: they keep the same
names and signatures the Agent calls, and delegate the external lookup to a
`PipelineAdapter`.

    Agent -> Tool -> PipelineAdapter -> structured result

Swapping mocks for real integrations is then an adapter change, not a tool
change, and never a Decide/Guard/Apply change.
"""

from __future__ import annotations

import copy
import time
from pathlib import Path

from pipeline_reliability.adapters import (
    BQ_ALREADY_SUCCEEDED,
    CRITICAL_DOWNSTREAM,
    DATA_CONFLICT,
    DEFAULT_ADAPTER,
    LATE_ARRIVED_EMPTY,
    LATE_ARRIVED_OCCUPIED,
    LATE_MIXED_ARRIVAL,
    LATE_SOURCE_MISSING,
    MISSING_SCHEMA,
    PERMISSION_DENIED,
    TRANSIENT_TIMEOUT,
    OrchestratorRunResult,
    WarehouseJobResult,
    DownstreamImpactResult,
    ExpectedInputObservation,
    ExpectedInputsResult,
    MockPipelineAdapter,
    PipelineAdapter,
    RepairResult,
    RetryResult,
    BackfillResult,
    BackfillReconcileResult,
    PartitionValidationResult,
    TaskLogResult,
    effective_scenario,
    unknown_expected_inputs,
)
from pipeline_reliability.checkpoint import (
    CheckpointRecord,
    CheckpointStore,
    persist_store_retry_intent,
    record_backfill_intent,
    record_retry_intent,
    require_mutation_lease,
)
from pipeline_reliability.decide import (
    APPLY_APPROVED_REPAIR,
    ASK_HUMAN,
    BACKFILL_PARTITION,
    VALIDATE_PARTITION,
    RECONCILE_BACKFILL,
    CHECK_ORCHESTRATOR_RUN,
    CHECK_WAREHOUSE_JOB,
    CHECK_DOWNSTREAM_IMPACT,
    CHECK_EXPECTED_INPUTS,
    CHECK_LOG,
    FINISH,
    GET_TASK_LOG,
    RETRY,
    STOP_SAFE,
    WAIT,
    WAIT_FOR_SOURCE,
    select_backfill_candidate,
)
from pipeline_reliability.guard import guard
from pipeline_reliability.recovery import classify_write_risk
from pipeline_reliability.repair_recipes import apply_approved_repair
from pipeline_reliability.state import PipelineReliabilityState

# Brief delay for WAIT / WAIT_FOR_SOURCE; tests may set WAIT_SECONDS=0
# or replace _sleep_fn.
WAIT_SECONDS = 1.0
_sleep_fn = time.sleep
WAIT_OBSERVATION = "wait_completed_reobserve_orchestrator"
WAIT_FOR_SOURCE_OBSERVATION = "wait_completed_reobserve_source"

# Re-exported so existing callers keep importing scenarios and result types
# from tools; the definitions now live at the adapter boundary.
__all__ = [
    "ASK_HUMAN",
    "BackfillResult",
    "BackfillReconcileResult",
    "PartitionValidationResult",
    "OrchestratorRunResult",
    "BQ_ALREADY_SUCCEEDED",
    "WarehouseJobResult",
    "CRITICAL_DOWNSTREAM",
    "DATA_CONFLICT",
    "DownstreamImpactResult",
    "ExpectedInputObservation",
    "ExpectedInputsResult",
    "LATE_ARRIVED_EMPTY",
    "LATE_ARRIVED_OCCUPIED",
    "LATE_MIXED_ARRIVAL",
    "LATE_SOURCE_MISSING",
    "MISSING_SCHEMA",
    "MockPipelineAdapter",
    "PERMISSION_DENIED",
    "PipelineAdapter",
    "RepairResult",
    "RetryResult",
    "TRANSIENT_TIMEOUT",
    "TaskLogResult",
    "check_orchestrator_run",
    "check_warehouse_job",
    "check_downstream_impact",
    "check_expected_inputs",
    "check_log",
    "execute",
    "get_task_log",
    "retry_pipeline",
    "backfill_partition",
    "reconcile_backfill",
    "validate_partition",
    "wait_for_recovery",
    "wait_for_source",
]


def _resolve(adapter: PipelineAdapter | None) -> PipelineAdapter:
    """Dependency injection, minimal version: explicit argument wins, else default."""
    return adapter if adapter is not None else DEFAULT_ADAPTER


def check_orchestrator_run(
    state: PipelineReliabilityState,
    adapter: PipelineAdapter | None = None,
) -> OrchestratorRunResult:
    """Fetch the latest orchestrator run status through the adapter.

    A TimeoutError is an observation (status UNKNOWN), not an uncaught crash.
    """
    try:
        return _resolve(adapter).check_orchestrator_run(state)
    except TimeoutError as exc:
        return OrchestratorRunResult(
            status="UNKNOWN",
            detail=f"CHECK_ORCHESTRATOR_RUN timed out: {exc}",
        )


def get_task_log(
    state: PipelineReliabilityState,
    adapter: PipelineAdapter | None = None,
) -> TaskLogResult:
    """Read the failing task's log and extract the error, through the adapter.

    A TimeoutError is an observation, not an uncaught crash. It must not
    classify the pipeline error as timeout (that would invite a blind RETRY).
    """
    try:
        return _resolve(adapter).get_task_log(state)
    except TimeoutError as exc:
        return TaskLogResult(
            error_type="",
            message="GET_TASK_LOG timed out",
            detail=f"GET_TASK_LOG timed out: {exc}",
        )


def check_warehouse_job(
    state: PipelineReliabilityState,
    adapter: PipelineAdapter | None = None,
) -> WarehouseJobResult:
    """Inspect the warehouse load job for this pipeline run, through the adapter.

    A TimeoutError is an observation (status UNKNOWN), not an uncaught crash.
    """
    try:
        return _resolve(adapter).check_warehouse_job(state)
    except TimeoutError as exc:
        return WarehouseJobResult(
            status="UNKNOWN",
            rows_written=None,
            detail=f"CHECK_WAREHOUSE_JOB timed out: {exc}",
        )


def check_downstream_impact(
    state: PipelineReliabilityState,
    adapter: PipelineAdapter | None = None,
) -> DownstreamImpactResult:
    """Check which dashboards and jobs depend on this pipeline, through the adapter."""
    return _resolve(adapter).check_downstream_impact(state)


def check_expected_inputs(
    state: PipelineReliabilityState,
    adapter: PipelineAdapter | None = None,
) -> ExpectedInputsResult:
    """Observe source arrival, target occupancy, and SLA — read-only.

    A TimeoutError is an observation (unknown facts), not an uncaught crash.
    Does not backfill, overwrite, or mutate State.
    """
    try:
        return _resolve(adapter).check_expected_inputs(state)
    except TimeoutError as exc:
        return unknown_expected_inputs(
            state,
            detail=f"CHECK_EXPECTED_INPUTS timed out: {exc}",
        )


def check_log(state: PipelineReliabilityState) -> str:
    """Simulate reading pipeline logs and return a coarse string verdict.

    Predates the structured `get_task_log` tool; kept for the legacy CHECK_LOG
    action, so it stays outside the adapter contract.
    """
    scenario = effective_scenario(state)

    if scenario == MISSING_SCHEMA:
        return "missing_schema_found_in_log"

    if scenario == PERMISSION_DENIED:
        return "permission_denied_found_in_log"

    if scenario == DATA_CONFLICT:
        return "data_conflict_found_in_log"

    return "timeout_found_in_log"


def _pre_retry_revalidate(
    state: PipelineReliabilityState,
    adapter: PipelineAdapter,
) -> RetryResult | None:
    """Re-check the warehouse and re-ask Guard before any orchestrator mutation.

    Guard remains the only safety authority. Runs only when a concrete
    ``warehouse_job_id`` is present so mock scenario demos (no real job id)
    are not re-queried into a contradictory profile. Returns a blocked
    RetryResult when Guard denies; returns None when mutation may proceed.
    """
    if not (state.warehouse_job_id or "").strip():
        return None

    try:
        fresh_bq = adapter.check_warehouse_job(state)
    except NotImplementedError:
        return None
    except Exception as exc:
        return RetryResult(
            accepted=False,
            pipeline_id=(state.pipeline or "").strip(),
            run_id=(state.run_id or "").strip(),
            task_id=(state.task_id or "").strip(),
            detail=f"pre-retry revalidation failed: {exc}",
        )

    temp = copy.copy(state)
    temp.warehouse_status = fresh_bq.status
    temp.rows_written = fresh_bq.rows_written
    classify_write_risk(temp)
    recheck = guard(temp, RETRY)
    if recheck.allowed:
        return None
    return RetryResult(
        accepted=False,
        pipeline_id=(state.pipeline or "").strip(),
        run_id=(state.run_id or "").strip(),
        task_id=(state.task_id or "").strip(),
        detail=f"pre-retry revalidation: {recheck.reason}",
    )


def retry_pipeline(
    state: PipelineReliabilityState,
    adapter: PipelineAdapter | None = None,
    checkpoint_path: str | Path | None = None,
    store: CheckpointStore | None = None,
    checkpoint_record: CheckpointRecord | None = None,
) -> RetryResult:
    """Request a controlled orchestrator retry through the adapter.

    Performs a last-moment warehouse revalidation (when available) and re-asks
    Guard before calling the adapter. Does not claim pipeline recovery.

    When ``checkpoint_path`` is set, a durable RETRY intent is written after
    revalidation and immediately before the adapter mutation.

    A TimeoutError from the adapter is an observation (side_effect UNKNOWN),
    not an uncaught crash and not a confirmed rejection.
    """
    resolved = _resolve(adapter)
    blocked = _pre_retry_revalidate(state, resolved)
    if blocked is not None:
        return blocked
    # INTENT before SIDE EFFECT. Last moment before the orchestrator mutation so a
    # pre-retry Guard block does not record a false-positive dispatch.
    # Lease and version are re-read here, after the warehouse read above and
    # before any durable intent or orchestrator mutation. A lost lease aborts
    # with the checkpoint row unchanged.
    require_mutation_lease(store, checkpoint_record)
    if checkpoint_path is not None:
        record_retry_intent(state, checkpoint_path)
    if store is not None and checkpoint_record is not None:
        persist_store_retry_intent(state, store, checkpoint_record)
    try:
        # Crash window: INTENT is durable (UNKNOWN). The orchestrator may already have
        # accepted. Apply has not merged RetryResult; the post-Apply checkpoint
        # has not been written. Process death here must CHECK_ORCHESTRATOR_RUN, not
        # replay RETRY. TimeoutError is an observation; any other exception is
        # an uncaught worker crash (the crash/resume proof uses that path).
        return resolved.retry_orchestrator_task(state)
    except TimeoutError as exc:
        # The clear/retry request may already have reached the orchestrator. Return an
        # observation so Apply can record UNKNOWN; do not raise out of Execute.
        return RetryResult(
            accepted=False,
            pipeline_id=(state.pipeline or "").strip(),
            run_id=(state.run_id or "").strip(),
            task_id=(state.task_id or "").strip(),
            detail=f"retry request timed out after dispatch: {exc}",
            side_effect="UNKNOWN",
        )


def backfill_partition(
    state: PipelineReliabilityState,
    adapter: PipelineAdapter | None = None,
    checkpoint_path: str | Path | None = None,
) -> BackfillResult:
    """Backfill exactly one Guard-authorized ARRIVED partition.

    INTENT before SIDE EFFECT. Selects the oldest confirmed-empty ARRIVED
    partition, durably records UNKNOWN + asset + partition when
    ``checkpoint_path`` is set, then calls the adapter. Does not write
    ArrivalItem.status (Apply does). An occupied target is not selected.

    An already-recorded UNKNOWN intent is not replayed: return UNKNOWN
    without a second warehouse call. TimeoutError after dispatch is an
    observation (status/side_effect UNKNOWN), not an uncaught crash.
    """
    if state.backfill_side_effect == "UNKNOWN":
        return BackfillResult(
            asset=state.backfill_asset,
            expected_partition=state.backfill_partition,
            status="UNKNOWN",
            side_effect="UNKNOWN",
            detail=(
                "BACKFILL_PARTITION not replayed: unresolved UNKNOWN intent "
                f"for '{state.backfill_asset}' partition "
                f"'{state.backfill_partition}'"
            ),
        )

    item = select_backfill_candidate(state)
    if item is None:
        return BackfillResult(
            asset="",
            expected_partition="",
            status="FAILED",
            side_effect="",
            detail="BACKFILL_PARTITION: no confirmed-empty ARRIVED partition to backfill",
        )
    asset = (item.asset or "").strip()
    partition = (item.expected_partition or "").strip()
    if not asset or not partition:
        return BackfillResult(
            asset=asset,
            expected_partition=partition,
            status="FAILED",
            side_effect="",
            detail="BACKFILL_PARTITION: ARRIVED item is missing asset or partition",
        )

    resolved = _resolve(adapter)
    if checkpoint_path is not None:
        record_backfill_intent(state, asset, partition, checkpoint_path)
    try:
        # Crash window: INTENT is durable (UNKNOWN). The warehouse may already
        # have accepted. Apply has not merged BackfillResult. Process death
        # here must reconcile, not replay BACKFILL_PARTITION.
        return resolved.backfill_partition(state, asset, partition)
    except TimeoutError as exc:
        return BackfillResult(
            asset=asset,
            expected_partition=partition,
            status="UNKNOWN",
            side_effect="UNKNOWN",
            detail=f"BACKFILL_PARTITION timed out after dispatch: {exc}",
        )
    except NotImplementedError as exc:
        return BackfillResult(
            asset=asset,
            expected_partition=partition,
            status="FAILED",
            side_effect="",
            detail=f"BACKFILL_PARTITION is not implemented: {exc}",
        )


def reconcile_backfill(
    state: PipelineReliabilityState,
    adapter: PipelineAdapter | None = None,
) -> BackfillReconcileResult:
    """Read-only inspect of the recorded UNKNOWN backfill identity.

    Uses ONLY ``state.backfill_asset`` and ``state.backfill_partition``.
    Does not select a different ArrivalItem. Does not call
    ``backfill_partition()``. Timeout / missing identity is UNKNOWN, not a
    write and not a replay.
    """
    asset = (state.backfill_asset or "").strip()
    partition = (state.backfill_partition or "").strip()
    if not asset or not partition:
        return BackfillReconcileResult(
            asset=asset,
            expected_partition=partition,
            status="UNKNOWN",
            detail=(
                "RECONCILE_BACKFILL: missing recorded backfill asset or partition"
            ),
        )

    try:
        return _resolve(adapter).reconcile_backfill(state, asset, partition)
    except TimeoutError as exc:
        return BackfillReconcileResult(
            asset=asset,
            expected_partition=partition,
            status="UNKNOWN",
            detail=f"RECONCILE_BACKFILL timed out: {exc}",
        )
    except NotImplementedError as exc:
        return BackfillReconcileResult(
            asset=asset,
            expected_partition=partition,
            status="UNKNOWN",
            detail=f"RECONCILE_BACKFILL is not implemented: {exc}",
        )


def _next_backfilled_item(state: PipelineReliabilityState):
    """First ArrivalItem still eligible for VALIDATE_PARTITION, or None.

    Same selection Decide already proposed: first status == BACKFILLED.
    """
    for item in state.arrival_items:
        if item.status == "BACKFILLED":
            return item
    return None


def validate_partition(
    state: PipelineReliabilityState,
    adapter: PipelineAdapter | None = None,
) -> PartitionValidationResult:
    """Read-only check of exactly one BACKFILLED partition.

    Selects the first BACKFILLED item and asks the adapter whether that
    identity exists with acceptable rows. Does not write ArrivalItem.status
    (Apply does). Does not backfill, overwrite, or mutate State.
    """
    item = _next_backfilled_item(state)
    if item is None:
        return PartitionValidationResult(
            asset="",
            expected_partition="",
            passed=False,
            detail="VALIDATE_PARTITION: no BACKFILLED item to validate",
        )
    asset = (item.asset or "").strip()
    partition = (item.expected_partition or "").strip()
    if not asset or not partition:
        return PartitionValidationResult(
            asset=asset,
            expected_partition=partition,
            passed=False,
            detail=(
                "VALIDATE_PARTITION: BACKFILLED item is missing asset or partition"
            ),
        )

    try:
        return _resolve(adapter).validate_partition(state, asset, partition)
    except TimeoutError as exc:
        return PartitionValidationResult(
            asset=asset,
            expected_partition=partition,
            passed=False,
            detail=f"VALIDATE_PARTITION timed out: {exc}",
        )
    except NotImplementedError as exc:
        return PartitionValidationResult(
            asset=asset,
            expected_partition=partition,
            passed=False,
            detail=f"VALIDATE_PARTITION is not implemented: {exc}",
        )


def wait_for_recovery() -> str:
    """Bounded WAIT tool: brief delay, then signal Apply to re-observe the orchestrator.

    Sleep lives here (Execute), not in Runner or Guard. Tests may set
    WAIT_SECONDS to 0 or replace ``_sleep_fn`` so they do not block.
    """
    _sleep_fn(WAIT_SECONDS)
    return WAIT_OBSERVATION


def wait_for_source() -> str:
    """Bounded WAIT_FOR_SOURCE tool: delay only, then signal Apply to re-check sources.

    Reuses the WAIT sleep hook so tests can set WAIT_SECONDS=0 or replace
    ``_sleep_fn``. Does not inspect the warehouse, backfill, choose safety,
    or mutate State — Apply owns those facts after this observation.
    """
    _sleep_fn(WAIT_SECONDS)
    return WAIT_FOR_SOURCE_OBSERVATION


def execute(
    state: PipelineReliabilityState,
    action: str,
    adapter: PipelineAdapter | None = None,
    checkpoint_path: str | Path | None = None,
    store: CheckpointStore | None = None,
    checkpoint_record: CheckpointRecord | None = None,
) -> (
    str
    | OrchestratorRunResult
    | WarehouseJobResult
    | TaskLogResult
    | DownstreamImpactResult
    | ExpectedInputsResult
    | RetryResult
    | RepairResult
    | BackfillResult
    | BackfillReconcileResult
    | PartitionValidationResult
):
    """Route an allowed action to the matching tool; return an observation.

    Call this only after Guard returns allowed=True (enforced by runner later).
    """
    # `state` — passed through to tools that need pipeline context or retry count.
    # `action` — the action string Decide proposed and Guard approved.
    # `adapter` — optional external-system implementation; None uses the mock.

    if action == CHECK_ORCHESTRATOR_RUN:
        return check_orchestrator_run(state, adapter)

    if action == CHECK_WAREHOUSE_JOB:
        return check_warehouse_job(state, adapter)

    if action == GET_TASK_LOG:
        return get_task_log(state, adapter)

    if action == CHECK_DOWNSTREAM_IMPACT:
        return check_downstream_impact(state, adapter)

    if action == CHECK_EXPECTED_INPUTS:
        return check_expected_inputs(state, adapter)

    if action == CHECK_LOG:
        return check_log(state)

    if action == RETRY:
        return retry_pipeline(
            state,
            adapter,
            checkpoint_path=checkpoint_path,
            store=store,
            checkpoint_record=checkpoint_record,
        )

    if action == BACKFILL_PARTITION:
        require_mutation_lease(store, checkpoint_record)
        return backfill_partition(state, adapter, checkpoint_path=checkpoint_path)

    if action == RECONCILE_BACKFILL:
        return reconcile_backfill(state, adapter)

    if action == VALIDATE_PARTITION:
        return validate_partition(state, adapter)

    if action == APPLY_APPROVED_REPAIR:
        return apply_approved_repair(state)

    if action == WAIT:
        return wait_for_recovery()

    if action == WAIT_FOR_SOURCE:
        return wait_for_source()

    # ASK_HUMAN, STOP_SAFE, FINISH — no tool yet; echo the action as observation.
    if action in (ASK_HUMAN, STOP_SAFE, FINISH):
        return action

    # Unknown action — should not happen if Decide and Guard stay in sync.
    return f"unknown_action:{action}"
