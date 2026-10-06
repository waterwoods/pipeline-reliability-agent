"""Pipeline Reliability Agent — Adapters (external-system boundary).

Loop position:

    State -> Decide -> Guard -> Tool -> Adapter -> Observation

This showcase ships a deterministic mock adapter so the Agent core can run
with no cloud account. Real Airflow / BigQuery adapters lived in the private
lab repo and are intentionally not copied here.

Adapters must not mutate State — Apply owns that.
Adapters must not decide whether RETRY or BACKFILL_PARTITION is safe — Guard owns that.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Protocol

from pipeline_reliability.state import PipelineReliabilityState

# Demo/testing scenario labels — plain strings, not enums.
TRANSIENT_TIMEOUT = "TRANSIENT_TIMEOUT"
BQ_ALREADY_SUCCEEDED = "BQ_ALREADY_SUCCEEDED"
MISSING_SCHEMA = "MISSING_SCHEMA"
PERMISSION_DENIED = "PERMISSION_DENIED"
DATA_CONFLICT = "DATA_CONFLICT"
CRITICAL_DOWNSTREAM = "CRITICAL_DOWNSTREAM"
LATE_SOURCE_MISSING = "LATE_SOURCE_MISSING"
LATE_ARRIVED_EMPTY = "LATE_ARRIVED_EMPTY"
LATE_ARRIVED_OCCUPIED = "LATE_ARRIVED_OCCUPIED"
LATE_MIXED_ARRIVAL = "LATE_MIXED_ARRIVAL"

_DEFAULT_SCENARIO = BQ_ALREADY_SUCCEEDED


def effective_scenario(state: PipelineReliabilityState) -> str:
    """Return the mock profile to use; empty scenario keeps prior default behavior."""
    return state.scenario or _DEFAULT_SCENARIO


# ---------------------------------------------------------------------------
# Structured results — the shared vocabulary between adapters and tools
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class WarehouseJobResult:
    """Structured observation from a warehouse job inspection.

    ``authoritative`` is false when this read did not observe a terminal job.
    A missing job id after a retry is not a failed job. Apply keeps the prior
    warehouse fact until an authoritative terminal status arrives.
    """

    status: str
    rows_written: int | None
    detail: str = ""
    authoritative: bool = True


@dataclass(frozen=True)
class OrchestratorRunResult:
    """Structured observation from an orchestrator DAG-run inspection.

    ``attempt_number`` and ``latest_repair_id`` are optional vendor identity
    fields (Databricks ``get_run``). None/empty when the adapter does not
    observe them. They do not replace ``status``.
    """

    status: str
    failed_task_id: str = ""
    detail: str = ""
    attempt_number: int | None = None
    latest_repair_id: str = ""


@dataclass(frozen=True)
class TaskLogResult:
    """Structured observation from reading a task's log output."""

    error_type: str
    message: str = ""
    detail: str = ""


@dataclass(frozen=True)
class DownstreamImpactResult:
    """Structured observation from checking downstream blast radius."""

    impact: str
    affected_assets: list[str]
    detail: str = ""


@dataclass(frozen=True)
class ExpectedInputObservation:
    """One expected source partition: presence + target occupancy."""

    asset: str
    expected_partition: str
    source_present: bool | None
    target_partition_empty: bool | None


@dataclass(frozen=True)
class ExpectedInputsResult:
    """Structured observation from CHECK_EXPECTED_INPUTS.

    Read-only. Does not backfill, overwrite, or mutate the warehouse.
    ``source_present`` / ``target_partition_empty`` may be None when the
    adapter could not determine that fact. Never invent True.
    ``sla_breached`` is one verdict for the whole check.
    """

    items: list[ExpectedInputObservation]
    sla_breached: bool
    detail: str = ""


def unknown_expected_inputs(
    state: PipelineReliabilityState,
    *,
    detail: str,
) -> ExpectedInputsResult:
    """Fail-closed CHECK_EXPECTED_INPUTS observation. Does not invent True."""
    return ExpectedInputsResult(
        items=[
            ExpectedInputObservation(
                asset=item.asset,
                expected_partition=item.expected_partition,
                source_present=None,
                target_partition_empty=None,
            )
            for item in state.arrival_items
        ],
        sla_breached=False,
        detail=detail,
    )


def _expected_input_items(
    state: PipelineReliabilityState,
    *,
    source_present: bool | None,
    target_partition_empty: bool | None,
) -> list[ExpectedInputObservation]:
    return [
        ExpectedInputObservation(
            asset=item.asset,
            expected_partition=item.expected_partition,
            source_present=source_present,
            target_partition_empty=target_partition_empty,
        )
        for item in state.arrival_items
    ]


def _mixed_arrival_facts(asset: str) -> tuple[bool | None, bool | None]:
    """Demo mixed partition: customer missing; other assets arrived + empty."""
    token = asset.replace("/", ".").rsplit(".", 1)[-1].strip().lower()
    if token in {"customer", "customers"}:
        return False, None
    return True, True


@dataclass(frozen=True)
class RepairResult:
    """Structured observation from APPLY_APPROVED_REPAIR.

    ``applied`` means the trusted rename was published to staging.
    ``validated`` means the repaired header exactly equals the expected contract.
    Neither field is an Action and neither claims Airflow recovery.
    """

    applied: bool
    validated: bool
    recipe_id: str = ""
    detail: str = ""
    repaired_schema: str = ""
    source_path: str = ""
    staging_path: str = ""


@dataclass(frozen=True)
class RetryResult:
    """Structured observation from requesting an orchestrator task retry.

    ``accepted`` means the orchestrator accepted the clear/retry request — not
    that the pipeline recovered. Recovery is confirmed only by a later
    observation.

    ``side_effect`` is "" when ``accepted`` is a confirmed yes/no. It is
    ``UNKNOWN`` when the request may already have reached the orchestrator
    but the transport timed out — that is not failure and not success.

    ``attempt_number`` / ``latest_repair_id`` are optional vendor identity
    fields copied from a Databricks ``get_run`` (or repair submit) when
    present. They identify which repair/attempt was observed, not recovery.
    """

    accepted: bool
    pipeline_id: str = ""
    run_id: str = ""
    task_id: str = ""
    detail: str = ""
    orchestrator_status: str = ""
    side_effect: str = ""
    attempt_number: int | None = None
    latest_repair_id: str = ""


@dataclass(frozen=True)
class BackfillResult:
    """Structured observation from one targeted partition backfill.

    ``status`` is SUCCEEDED, FAILED, or UNKNOWN. SUCCEEDED means this
    adapter call reported a definite write of ``asset`` /
    ``expected_partition`` — not that the pipeline recovered and not that
    the partition is VALIDATED.

    ``side_effect`` is "" when ``status`` is a confirmed yes/no. It is
    ``UNKNOWN`` when the write may already have reached the warehouse
    but the transport timed out — that is not failure and not success.
    """

    asset: str
    expected_partition: str
    status: str
    side_effect: str
    detail: str = ""


@dataclass(frozen=True)
class BackfillReconcileResult:
    """Structured observation from one read-only BACKFILL crash reconcile.

    ``status`` is LANDED, NOT_LANDED, or UNKNOWN. LANDED means warehouse
    evidence shows this exact asset + partition exists. NOT_LANDED means
    evidence shows the target is still empty. UNKNOWN means neither can
    be proved. Not a write, not BACKFILLED, and not a second backfill.
    """

    asset: str
    expected_partition: str
    status: str
    detail: str = ""


@dataclass(frozen=True)
class PartitionValidationResult:
    """Structured observation from one read-only partition health check.

    ``passed`` True means this adapter call observed that ``asset`` /
    ``expected_partition`` exists, has acceptable rows, and matches the
    requested identity. That is not FINISH and not a second backfill.

    ``passed`` False is not a license to write again. Apply owns the
    BACKFILLED → VALIDATED promotion.
    """

    asset: str
    expected_partition: str
    passed: bool
    detail: str = ""


class OrchestratorAdapter(Protocol):
    """Orchestrator inspection and one controlled retry."""

    def check_orchestrator_run(self, state: PipelineReliabilityState) -> OrchestratorRunResult:
        """Return the latest orchestrator DAG run status for this incident."""
        ...

    def get_task_log(self, state: PipelineReliabilityState) -> TaskLogResult:
        """Return the failing task's extracted error signal."""
        ...

    def retry_orchestrator_task(self, state: PipelineReliabilityState) -> RetryResult:
        """Request a targeted orchestrator retry for the incident's failed task."""
        ...


class WarehouseAdapter(Protocol):
    """Warehouse job inspection and expected-input occupancy."""

    def check_warehouse_job(self, state: PipelineReliabilityState) -> WarehouseJobResult:
        """Return the warehouse load job state for this pipeline run."""
        ...

    def check_expected_inputs(self, state: PipelineReliabilityState) -> ExpectedInputsResult:
        """Return source-arrival and target-occupancy facts for expected partitions.

        Read-only. Does not write, merge, overwrite, or backfill.
        """
        ...

    def backfill_partition(
        self,
        state: PipelineReliabilityState,
        asset: str,
        expected_partition: str,
    ) -> BackfillResult:
        """Write exactly one selected asset + partition.

        Not a policy decision. Guard already authorized an empty target.
        Does not overwrite, merge, or choose a different partition.
        """
        ...

    def validate_partition(
        self,
        state: PipelineReliabilityState,
        asset: str,
        expected_partition: str,
    ) -> PartitionValidationResult:
        """Read-only check that one backfilled partition is healthy.

        Does not write, merge, overwrite, or backfill.
        """
        ...

    def reconcile_backfill(
        self,
        state: PipelineReliabilityState,
        asset: str,
        expected_partition: str,
    ) -> BackfillReconcileResult:
        """Read-only occupancy of one recorded backfill asset + partition.

        Inspects exactly ``asset`` / ``expected_partition``. Does not write,
        merge, overwrite, backfill, or choose a different ArrivalItem.
        """
        ...


class DownstreamAdapter(Protocol):
    """Downstream blast-radius inspection."""

    def check_downstream_impact(
        self, state: PipelineReliabilityState
    ) -> DownstreamImpactResult:
        """Return which downstream assets this failure blocks."""
        ...


class PipelineAdapter(OrchestratorAdapter, WarehouseAdapter, DownstreamAdapter, Protocol):
    """Full external-system boundary: orchestrator + warehouse + downstream.

    A Protocol (structural typing) rather than an ABC: any object with these
    methods works, so a three-line test stub needs no base class.

    Prefer the focused Protocols at composition boundaries. Keep this umbrella
    type where the Agent needs the full surface (tools, runner, evals).
    """


class MockPipelineAdapter:
    """Scenario-driven fake external world; no I/O, no clock, no randomness.

    Behavior is keyed on `state.scenario` so one Agent loop can be replayed
    against six different incident shapes. This is SYNTHETIC / lab fixture
    data, not a live cloud account.
    """

    def check_orchestrator_run(self, state: PipelineReliabilityState) -> OrchestratorRunResult:
        _ = effective_scenario(state)
        return OrchestratorRunResult(
            status="FAILED",
            failed_task_id="load_orders_to_bigquery",
            detail=(
                "DAG run failed at the BigQuery load task; "
                "upstream extract/transform succeeded."
            ),
        )

    def get_task_log(self, state: PipelineReliabilityState) -> TaskLogResult:
        scenario = effective_scenario(state)

        if scenario == MISSING_SCHEMA:
            return TaskLogResult(
                error_type="SchemaError",
                message="Required column 'order_status' not found in source table schema.",
                detail=(
                    "BigQuery load rejected: schema mismatch on required column "
                    "order_status."
                ),
            )

        if scenario == PERMISSION_DENIED:
            return TaskLogResult(
                error_type="PermissionDenied",
                message=(
                    "Access Denied: bigquery.jobs.create permission required "
                    "on project example-project."
                ),
                detail="Service account lacks IAM role to run load jobs in target dataset.",
            )

        if scenario == DATA_CONFLICT:
            return TaskLogResult(
                error_type="DataConflict",
                message="Load aborted: duplicate key conflict on orders.order_id during merge.",
                detail="Log shows partial write followed by duplicate key failure.",
            )

        return TaskLogResult(
            error_type="TimeoutError",
            message=(
                "Task exceeded execution_timeout of 3600s while polling "
                "BigQuery job status."
            ),
            detail=(
                "Log tail shows repeated poll attempts with no terminal job state "
                "from Airflow's view."
            ),
        )

    def check_warehouse_job(self, state: PipelineReliabilityState) -> WarehouseJobResult:
        scenario = effective_scenario(state)

        if scenario == TRANSIENT_TIMEOUT:
            return WarehouseJobResult(
                status="FAILED",
                rows_written=0,
                detail="BigQuery load job failed; no rows committed to destination table.",
            )

        if scenario == BQ_ALREADY_SUCCEEDED:
            return WarehouseJobResult(
                status="SUCCEEDED",
                rows_written=1_240_000,
                detail=(
                    "BigQuery load job completed successfully; Airflow may have timed out "
                    "while waiting for the job to finish."
                ),
            )

        if scenario == MISSING_SCHEMA:
            return WarehouseJobResult(
                status="FAILED",
                rows_written=0,
                detail=(
                    "BigQuery job failed: required column order_status missing "
                    "from source schema."
                ),
            )

        if scenario == PERMISSION_DENIED:
            return WarehouseJobResult(
                status="FAILED",
                rows_written=0,
                detail="BigQuery job failed: permission denied on dataset orders_gold.",
            )

        if scenario == DATA_CONFLICT:
            return WarehouseJobResult(
                status="FAILED",
                rows_written=450_000,
                detail=(
                    "Partial load detected: 450000 rows written before duplicate key "
                    "conflict aborted the job."
                ),
            )

        return WarehouseJobResult(
            status="FAILED",
            rows_written=0,
            detail="BigQuery load job failed; no rows committed to destination table.",
        )

    def check_downstream_impact(
        self, state: PipelineReliabilityState
    ) -> DownstreamImpactResult:
        scenario = effective_scenario(state)

        if scenario == CRITICAL_DOWNSTREAM:
            return DownstreamImpactResult(
                impact="critical",
                affected_assets=["executive_revenue_dashboard", "monthly_close_report"],
                detail=(
                    "Revenue-close SLA dashboards depend on this pipeline; "
                    "delay blocks executive reporting."
                ),
            )

        if scenario == TRANSIENT_TIMEOUT:
            return DownstreamImpactResult(
                impact="low",
                affected_assets=["orders_daily_dashboard"],
                detail="Downstream dashboard can tolerate a same-day delay.",
            )

        return DownstreamImpactResult(
            impact="medium",
            affected_assets=["orders_daily_dashboard", "inventory_reconciliation_job"],
            detail=(
                "Downstream assets can tolerate a same-day delay; "
                "no revenue-close SLA at risk."
            ),
        )

    def check_expected_inputs(self, state: PipelineReliabilityState) -> ExpectedInputsResult:
        """Read-only source/target facts. Scenario-keyed; never invents True."""
        scenario = effective_scenario(state)

        if scenario == LATE_SOURCE_MISSING:
            return ExpectedInputsResult(
                items=_expected_input_items(
                    state,
                    source_present=False,
                    target_partition_empty=None,
                ),
                sla_breached=False,
                detail="Mock: expected source partitions are missing.",
            )

        if scenario == LATE_ARRIVED_EMPTY:
            return ExpectedInputsResult(
                items=_expected_input_items(
                    state,
                    source_present=True,
                    target_partition_empty=True,
                ),
                sla_breached=False,
                detail=(
                    "Mock: expected source partitions arrived; "
                    "target partitions are empty."
                ),
            )

        if scenario == LATE_ARRIVED_OCCUPIED:
            return ExpectedInputsResult(
                items=_expected_input_items(
                    state,
                    source_present=True,
                    target_partition_empty=False,
                ),
                sla_breached=False,
                detail=(
                    "Mock: expected source partitions arrived; "
                    "target partitions already contain data."
                ),
            )

        if scenario == LATE_MIXED_ARRIVAL:
            items = []
            for item in state.arrival_items:
                source_present, target_empty = _mixed_arrival_facts(item.asset)
                items.append(
                    ExpectedInputObservation(
                        asset=item.asset,
                        expected_partition=item.expected_partition,
                        source_present=source_present,
                        target_partition_empty=target_empty,
                    )
                )
            return ExpectedInputsResult(
                items=items,
                sla_breached=False,
                detail=(
                    "Mock: mixed arrival — customer source missing; "
                    "other sources arrived with empty targets."
                ),
            )

        return unknown_expected_inputs(
            state,
            detail=(
                "Mock: expected-input facts are unknown for this scenario; "
                "source presence and target occupancy were not invented."
            ),
        )

    def retry_orchestrator_task(self, state: PipelineReliabilityState) -> RetryResult:
        task_id = (state.task_id or "").strip() or "load_orders_to_bigquery"
        return RetryResult(
            accepted=True,
            pipeline_id=(state.pipeline or "").strip() or "unknown_dag",
            run_id=(state.run_id or "").strip() or "unknown_run",
            task_id=task_id,
            detail=(
                f"Mock retry request accepted for task '{task_id}' "
                "(recovery not claimed; re-observe Airflow next)."
            ),
            orchestrator_status="queued",
        )

    def backfill_partition(
        self,
        state: PipelineReliabilityState,
        asset: str,
        expected_partition: str,
    ) -> BackfillResult:
        """Deterministic fake write of one partition. Does not check occupancy."""
        _ = effective_scenario(state)
        return BackfillResult(
            asset=asset,
            expected_partition=expected_partition,
            status="SUCCEEDED",
            side_effect="",
            detail=(
                f"Mock backfill accepted for '{asset}' partition "
                f"'{expected_partition}' (recovery not claimed; "
                "validation not performed)."
            ),
        )

    def validate_partition(
        self,
        state: PipelineReliabilityState,
        asset: str,
        expected_partition: str,
    ) -> PartitionValidationResult:
        """Deterministic fake read of one partition. Does not write."""
        _ = effective_scenario(state)
        asset = (asset or "").strip()
        expected_partition = (expected_partition or "").strip()
        if not asset or not expected_partition:
            return PartitionValidationResult(
                asset=asset,
                expected_partition=expected_partition,
                passed=False,
                detail=(
                    "Mock validation failed: missing asset or partition identity."
                ),
            )

        if state.arrival_items:
            known = any(
                item.asset == asset and item.expected_partition == expected_partition
                for item in state.arrival_items
            )
            if not known:
                return PartitionValidationResult(
                    asset=asset,
                    expected_partition=expected_partition,
                    passed=False,
                    detail=(
                        f"Mock validation failed: '{asset}' partition "
                        f"'{expected_partition}' does not match a known "
                        "arrival identity."
                    ),
                )

        return PartitionValidationResult(
            asset=asset,
            expected_partition=expected_partition,
            passed=True,
            detail=(
                f"Mock validation passed: '{asset}' partition "
                f"'{expected_partition}' exists with rows; "
                "identity matches the request."
            ),
        )

    def reconcile_backfill(
        self,
        state: PipelineReliabilityState,
        asset: str,
        expected_partition: str,
    ) -> BackfillReconcileResult:
        """Deterministic fake read of one recorded partition. Does not write."""
        asset = (asset or "").strip()
        expected_partition = (expected_partition or "").strip()
        if not asset or not expected_partition:
            return BackfillReconcileResult(
                asset=asset,
                expected_partition=expected_partition,
                status="UNKNOWN",
                detail=(
                    "Mock reconcile cannot prove landed or empty: "
                    "missing asset or partition identity."
                ),
            )

        occupancy = None
        found = False
        for item in self.check_expected_inputs(state).items:
            if item.asset == asset and item.expected_partition == expected_partition:
                occupancy = item.target_partition_empty
                found = True
                break
        if not found:
            return BackfillReconcileResult(
                asset=asset,
                expected_partition=expected_partition,
                status="UNKNOWN",
                detail=(
                    f"Mock reconcile cannot prove landed or empty: "
                    f"'{asset}' partition '{expected_partition}' "
                    "was not found in warehouse evidence."
                ),
            )
        if occupancy is True:
            return BackfillReconcileResult(
                asset=asset,
                expected_partition=expected_partition,
                status="NOT_LANDED",
                detail=(
                    f"Mock reconcile: '{asset}' partition "
                    f"'{expected_partition}' is still empty."
                ),
            )
        if occupancy is False:
            return BackfillReconcileResult(
                asset=asset,
                expected_partition=expected_partition,
                status="LANDED",
                detail=(
                    f"Mock reconcile: '{asset}' partition "
                    f"'{expected_partition}' exists in the warehouse."
                ),
            )
        return BackfillReconcileResult(
            asset=asset,
            expected_partition=expected_partition,
            status="UNKNOWN",
            detail=(
                f"Mock reconcile cannot prove landed or empty: "
                f"'{asset}' partition '{expected_partition}' "
                "occupancy is unknown."
            ),
        )


DEFAULT_ADAPTER: PipelineAdapter = MockPipelineAdapter()


def parse_task_log_content(content: str) -> TaskLogResult:
    """Extract the most relevant error signal from raw task log text.

    Generic parser for lab / eval fixtures. Does not call Airflow.
    """
    lines = [line.strip() for line in content.splitlines() if line.strip()]
    if lines and re.fullmatch(r"[0-9a-f]{8,}", lines[0]):
        lines = lines[1:]
    if not lines:
        return TaskLogResult(
            error_type="UnknownError",
            message="Task log is empty.",
            detail="No log content returned from Airflow.",
        )

    exception_line = ""
    for line in reversed(lines):
        if line.startswith(
            (
                "TimeoutError:",
                "AirflowException:",
                "ValueError:",
                "RuntimeError:",
                "Exception:",
            )
        ):
            exception_line = line
            break
        match = re.search(
            r"\b(TimeoutError|AirflowException|ValueError|RuntimeError|SchemaError|PermissionDenied|DataConflict)\b: (.+)$",
            line,
        )
        if match:
            exception_line = f"{match.group(1)}: {match.group(2)}"
            break

    if not exception_line:
        for line in reversed(lines):
            if " ERROR " in line or "Traceback" in line:
                exception_line = line
                break

    if not exception_line:
        exception_line = lines[-1]

    error_type = "UnknownError"
    message = exception_line
    if ":" in exception_line:
        prefix, _, remainder = exception_line.partition(":")
        if prefix.endswith("Error") or prefix in {
            "AirflowException",
            "PermissionDenied",
            "DataConflict",
            "SchemaError",
        }:
            error_type = prefix.split(".")[-1]
            message = remainder.strip() or exception_line

    tail = "\n".join(lines[-8:])
    return TaskLogResult(
        error_type=error_type,
        message=message,
        detail=tail,
    )


# Kept for tests that still import the private lab name.
_parse_task_log_content = parse_task_log_content


class CompositePipelineAdapter:
    """Delegate each PipelineAdapter method to a dedicated backing adapter.

    Constructor takes the narrowest Protocol for each surface so a warehouse-only
    implementation need not implement orchestrator or downstream methods.
    Enables swapping one mock surface at a time without changing Decide,
    Guard, Apply, or tool signatures.
    """

    def __init__(
        self,
        *,
        orchestrator_adapter: OrchestratorAdapter,
        warehouse_adapter: WarehouseAdapter,
        downstream_adapter: DownstreamAdapter,
    ) -> None:
        self._orchestrator_adapter = orchestrator_adapter
        self._warehouse_adapter = warehouse_adapter
        self._downstream_adapter = downstream_adapter

    def check_orchestrator_run(self, state: PipelineReliabilityState) -> OrchestratorRunResult:
        return self._orchestrator_adapter.check_orchestrator_run(state)

    def get_task_log(self, state: PipelineReliabilityState) -> TaskLogResult:
        return self._orchestrator_adapter.get_task_log(state)

    def check_warehouse_job(self, state: PipelineReliabilityState) -> WarehouseJobResult:
        return self._warehouse_adapter.check_warehouse_job(state)

    def check_expected_inputs(self, state: PipelineReliabilityState) -> ExpectedInputsResult:
        checker = getattr(self._warehouse_adapter, "check_expected_inputs", None)
        if checker is None:
            return unknown_expected_inputs(
                state,
                detail=(
                    "CHECK_EXPECTED_INPUTS is not implemented on the "
                    "warehouse adapter; source/target facts left unknown."
                ),
            )
        return checker(state)

    def check_downstream_impact(
        self, state: PipelineReliabilityState
    ) -> DownstreamImpactResult:
        return self._downstream_adapter.check_downstream_impact(state)

    def retry_orchestrator_task(self, state: PipelineReliabilityState) -> RetryResult:
        return self._orchestrator_adapter.retry_orchestrator_task(state)

    def backfill_partition(
        self,
        state: PipelineReliabilityState,
        asset: str,
        expected_partition: str,
    ) -> BackfillResult:
        writer = getattr(self._warehouse_adapter, "backfill_partition", None)
        if writer is None:
            return BackfillResult(
                asset=asset,
                expected_partition=expected_partition,
                status="FAILED",
                side_effect="",
                detail=(
                    "backfill_partition is not implemented on the "
                    "warehouse adapter; no warehouse write was attempted."
                ),
            )
        return writer(state, asset, expected_partition)

    def validate_partition(
        self,
        state: PipelineReliabilityState,
        asset: str,
        expected_partition: str,
    ) -> PartitionValidationResult:
        checker = getattr(self._warehouse_adapter, "validate_partition", None)
        if checker is None:
            return PartitionValidationResult(
                asset=asset,
                expected_partition=expected_partition,
                passed=False,
                detail=(
                    "validate_partition is not implemented on the "
                    "warehouse adapter; partition health left unconfirmed."
                ),
            )
        return checker(state, asset, expected_partition)

    def reconcile_backfill(
        self,
        state: PipelineReliabilityState,
        asset: str,
        expected_partition: str,
    ) -> BackfillReconcileResult:
        checker = getattr(self._warehouse_adapter, "reconcile_backfill", None)
        if checker is None:
            return BackfillReconcileResult(
                asset=asset,
                expected_partition=expected_partition,
                status="UNKNOWN",
                detail=(
                    "reconcile_backfill is not implemented on the "
                    "warehouse adapter; landed vs empty left unproven."
                ),
            )
        return checker(state, asset, expected_partition)
