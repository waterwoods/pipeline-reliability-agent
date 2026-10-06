"""Pipeline Reliability Agent — State.

State = the agent's current snapshot of the world.

The loop will later be:

    State -> Decide -> Guard -> Execute -> Observation -> Apply -> New State

Each pass through the loop reads the current State, chooses an action, runs
safety checks, executes a tool, observes the result, and writes an updated
State. Nothing else in the agent needs hidden global variables if every fact
and decision input lives here.
"""

from __future__ import annotations

from dataclasses import dataclass, field


# ---------------------------------------------------------------------------
# What "State" means in an agent workflow
# ---------------------------------------------------------------------------
# State is not a database, a log file, or a chat history. It is the smallest
# structured bundle of information the agent needs to answer one question:
# "Given what I know right now, what should I do next?"
#
# The Decide step reads State. The Guard step reads State. Execute uses State
# to pick parameters. Observation and Apply merge new facts back into State.
# ---------------------------------------------------------------------------


@dataclass
class ArrivalItem:
    """One expected source partition for a PARTITIONED input.

    Distinct from State.expected_partition / target_partition, which record
    the warehouse write identity of *this run* (wrong-partition detection).
    ArrivalItem is "did this source partition arrive?" — one row per expected
    input. V1 statuses are plain strings: UNKNOWN, MISSING, ARRIVED,
    BACKFILLED, VALIDATED. Not an enum.
    """

    asset: str
    # Source/table this row is about (e.g. "stg_orders").

    expected_partition: str
    # Daily partition this asset should have delivered (e.g. "2026-09-17").

    status: str
    # Arrival/recovery fact for this item only. Empty is not used; callers
    # should start at UNKNOWN until a check has run.

    target_partition_empty: bool | None = None
    # Warehouse occupancy for this item's target partition only.
    # True = checked and empty. False = already contains data.
    # None = not yet observed. Default unknown; never invent True.


@dataclass
class PipelineReliabilityState:
    """One moment in a pipeline incident — everything needed for the next decision."""

    # --- 1. Identity & 2. Current Facts (required) ---------------------------
    # Dataclass rule: fields without defaults must come first. pipeline and
    # error stay required so existing callers and tests need no changes.

    pipeline: str
    # Human-readable pipeline name (e.g. "orders_daily_etl").
    # The agent scopes every decision to one pipeline at a time.

    error: str
    # The failure signal: exception text, task status, or alert summary.
    # Decide uses this to classify severity and pick a recovery strategy.

    # --- 1. Identity (correlation ids) ---------------------------------------
    # Who/what is this incident about? These scope every tool call and decision
    # to one concrete run so the agent does not mix up incidents.

    task_id: str = ""
    # Orchestrator task instance id (e.g. "load_orders.run").
    # Lets Execute target the exact failing task instead of re-running the DAG.

    run_id: str = ""
    # Orchestrator run id or execution timestamp (e.g. "scheduled__2026-08-30T06:00:00").
    # Distinguishes today's failure from yesterday's when the same task fails often.

    warehouse_job_id: str = ""
    # Warehouse job id for read-only job inspection (e.g. "abc123-def456-7890").
    # Mock adapters ignore this; empty means the job id is not yet known.

    # --- 2. Current Facts (platform-reported status) -------------------------
    # Inputs the agent did not create — reported by the orchestrator, warehouse, or alerts.

    orchestrator_status: str = ""
    # Latest orchestrator run state (e.g. "FAILED", "RUNNING", "SUCCESS").
    # A retry may already be in flight; Decide should not fight the scheduler.

    warehouse_status: str = ""
    # Latest warehouse job state (e.g. "SUCCEEDED", "FAILED", "RUNNING", "UNKNOWN").
    # Separates "orchestrator failed" from "load job still running or stuck."

    rows_written: int | None = None
    # Row count from the load step, when known. None means we have not checked yet.
    # Partial loads (some rows, then error) need different handling than total failure.

    partial_write: bool | None = None
    # True = warehouse wrote some rows but the job did not fully succeed.
    # False = confirmed no partial write (zero rows, or a full SUCCEEDED load).
    # None = not yet classified. Derived by Apply from warehouse facts, not
    # from evidence-string keywords.

    retry_may_duplicate: bool | None = None
    # True = a RETRY could insert the same committed rows again (partial or
    # full warehouse writes already observed). False = confirmed zero writes.
    # None = not yet classified. Guard treats True as a hard RETRY block.

    baseline_rows: int | None = None
    # Median row count from the previous 7 days, already collected by a Tool/Adapter.
    # Decide does not compute this median; it is an observation for this incident.

    observed_rows: int | None = None
    # Row count observed for this run (source file or load inspection).
    # Distinct from rows_written: observed is what arrived; rows_written is what committed.

    volume_status: str = ""
    # Derived volume verdict for this run: "" (unknown), "ok", or "too_low".
    # too_low means observed_rows < baseline_rows * 0.5. Empty means not yet classified.

    expected_object: str | None = None
    # Source object this run expects (e.g. "orders_20260901.csv").
    # Identity only; Decide does not list buckets or glob storage.
    # Filename is a label, not proof that two deliveries are the same bytes.

    source_checksum: str | None = None
    # Strong identity of the current source delivery (content hash, or an
    # object generation / etag used as a hash-equivalent). None = not yet
    # observed. Filename + size + row count must not be stored here.

    prior_source_checksum: str | None = None
    # Strong identity of the source delivery that was already loaded for
    # this business date. None = prior identity unknown. Compared with
    # source_checksum as evidence (identical vs restatement), not as a
    # license to DELETE or overwrite.

    prior_load_succeeded: bool | None = None
    # Whether a *previous* run already successfully loaded this business
    # date / source identity. True = history says a successful load exists.
    # False = no prior success for this date. None = history not yet known.
    # This is not "the current run succeeded."

    file_present: bool | None = None
    # Whether expected_object currently exists. None = not yet observed.
    # False is a Tool/Adapter fact, not a Decide probe of GCS.

    sla_breached: bool = False
    # True when the allowed arrival window has already expired.
    # Already computed by a Tool/Adapter; Decide does not calculate SLA here.

    expected_delimiter: str | None = None
    # Source delimiter this run expects (e.g. ","). None = not yet known.
    # Identity of the format contract; Decide does not sniff or guess it.

    observed_delimiter: str | None = None
    # Delimiter found in the incoming file. None = not yet observed.
    # Compared to expected_delimiter only when both sides are set.

    expected_encoding: str | None = None
    # Source encoding this run expects (e.g. "UTF-8"). None = not yet known.
    # Identity of the format contract; Decide does not detect or re-encode.

    observed_encoding: str | None = None
    # Encoding found in the incoming file. None = not yet observed.
    # Compared to expected_encoding only when both sides are set.

    expected_schema: str | None = None
    # Source column contract this run expects (e.g. "customer_id,name,amount").
    # Identity only; Decide does not diff schemas.

    observed_schema: str | None = None
    # Header actually found in the incoming file. None = not yet observed.

    drift_type: str = ""
    # Deterministic DAG classification (e.g. "RENAMED_COLUMN"). Empty = unknown.

    changed_fields: str = ""
    # Compact DAG-provided change (e.g. "customer_id->cust_id"). Empty = unknown.

    repair_recipe_found: bool | None = None
    # True = an approved recipe exists for this exact drift. False = none.
    # None = not yet classified. Movie 1 records False. Decide does not look up recipes.

    repair_recipe_id: str = ""
    # Identity of the human-approved recipe when an exact match exists.

    repair_recipe_approved: bool | None = None
    # True = matched recipe status is APPROVED. False = present but not approved.
    # None = not yet classified.

    repair_applied: bool | None = None
    # True = trusted code published the repaired staging file.
    # False = a repair attempt ran and failed closed. None = not attempted.

    repair_validation_passed: bool | None = None
    # True = repaired header exactly equals expected_schema. False = failed.
    # None = not yet validated. Rerun is forbidden unless True.

    expected_business_date: str | None = None
    # Processing date this run should load (e.g. "2026-09-01").
    # Identity of the intended business day; Decide does not rewrite it.

    observed_business_date: str | None = None
    # Business date found in the incoming data. None = not yet observed.
    # Compared to expected_business_date only when both sides are set.

    expected_partition: str | None = None
    # Warehouse partition this run should write (e.g. "2026-09-01").
    # Decide does not create, drop, or redirect partitions.

    target_partition: str | None = None
    # Partition the load actually targeted. None = not yet observed.
    # Compared to expected_partition only when both sides are set.

    arrival_items: list[ArrivalItem] = field(default_factory=list)
    # Expected source partitions for this incident (one ArrivalItem each).
    # Empty = not yet checked / not a late-partition incident.
    # Does not replace file_present or expected_partition.

    load_mode: str = "PARTITIONED"
    # How this pipeline loads data. V1 supports PARTITIONED only.
    # SNAPSHOT / APPEND are reserved for later; not interpreted here.

    downstream_impact: str = ""
    # What depends on this pipeline (dashboards, downstream jobs, SLAs).
    # Helps Decide weigh "retry quietly" vs "escalate now."

    scenario: str = ""
    # Demo/testing label that selects mock tool behavior (e.g. "TRANSIENT_TIMEOUT").
    # Empty string uses the default mock profile (BQ_ALREADY_SUCCEEDED).

    # --- 3. Control / Memory -----------------------------------------------
    # Working memory the agent builds across loop turns — not raw platform truth.

    retries: int = 0
    # How many recovery attempts have already been made in this incident.
    # Control input for Guard: stop retrying after a limit to avoid thrashing.

    poll_count: int = 0
    # How many bounded WAIT polls this Agent has already performed while
    # waiting for the current asynchronous orchestrator recovery (e.g. after RETRY).
    # Distinct from retries: retries count re-executions; poll_count counts
    # re-observations of in-flight progress. Reset when a new RETRY is accepted.

    arrival_recheck_count: int = 0
    # How many source-arrival rechecks this Agent has already performed
    # while waiting for missing PARTITIONED inputs. Distinct from poll_count:
    # poll_count is in-flight orchestrator WAIT; this is wait-for-source.
    # Do not reuse poll_count for late-partition recovery.

    wait_backoff_seconds: float = 0.0
    # Bounded WAIT metadata: linear seconds for the last completed poll,
    # capped at MAX_POLLS. Not a scheduler. Execute still owns the actual delay.

    retry_side_effect: str = ""
    warehouse_stale: bool = False
    # True after an accepted RETRY, or after reconciliation proves an UNKNOWN
    # RETRY took effect, until the new attempt's warehouse job is read.
    # The previous warehouse fact belongs to the failed attempt.
    # "" = no unresolved RETRY mutation (never attempted, or already reconciled).
    # "UNKNOWN" = a RETRY side effect may have occurred and the orchestrator's result is
    # not durably known. Covers command timeout after dispatch AND a durable
    # RETRY intent recorded before dispatch (crash window: intent written,
    # process died before Apply/checkpoint of the result).
    # Distinct from retries: a lost transport response or stale checkpoint
    # must not look like "RETRY never happened."

    action_id: str = ""
    # Stable identity of the pending RETRY mutation. Minted in
    # record_retry_intent before dispatch and reused after process restart
    # so the same logical retry is not assigned a new id. Empty = none.

    attempt_number: int | None = None
    # Latest orchestrator attempt identity (Databricks get_run). None = not observed.
    # Used to reconcile an UNKNOWN RETRY: a higher number than the RETRY baseline
    # is evidence a repair/new attempt started. Status alone is not that proof.

    latest_repair_id: str = ""
    # Latest orchestrator repair identity (Databricks latest_repair_id / repair_id).
    # Empty = not observed. A new id vs the RETRY baseline is evidence a repair started.

    retry_baseline_attempt_number: int | None = None
    # attempt_number frozen at RETRY intent / UNKNOWN recording. Survives restart.
    # Compared with later CHECK observations; not overwritten while UNKNOWN.

    retry_baseline_repair_id: str = ""
    # latest_repair_id frozen at RETRY intent / UNKNOWN recording. Survives restart.

    backfill_side_effect: str = ""
    # "" = no unresolved BACKFILL mutation (never attempted, or already reconciled).
    # "UNKNOWN" = a BACKFILL_PARTITION side effect may have occurred and the
    # warehouse result is not durably known. Crash window: intent written,
    # process died before Apply/checkpoint of the write result.
    # Distinct from retry_side_effect: RETRY is one orchestrator mutation;
    # BACKFILL is a warehouse write of one asset+partition. Do not reuse
    # retry_side_effect — resume would reconcile the wrong system.

    backfill_asset: str = ""
    # Asset identity of the pending BACKFILL mutation (e.g. "stg_orders").
    # Empty = none. Survives restart so resume knows *which* write is uncertain.

    backfill_partition: str = ""
    # Partition identity of the pending BACKFILL mutation (e.g. "2026-09-17").
    # Empty = none. Paired with backfill_asset; State.expected_partition is the
    # run's warehouse write identity, not this per-item mutation.

    intelligence_exhausted: bool = False
    # True when Intelligence was attempted, including the one allowed repair,
    # and still produced no acceptable structured facts. Investigation fact,
    # not an Action. Apply records it; Decide may ASK_HUMAN; Intelligence must not.

    task_log_checked: bool = False
    # True after Apply has recorded one TaskLogResult. Investigation fact so
    # Decide can finish warehouse/downstream collection when the log stays
    # unclassified. Not an Action and not a failure class.

    observation: str = ""
    # The latest tool or check result (e.g. "source table has 0 rows today").
    # Usually overwritten each loop turn; it is "what we just learned."

    evidence: list[str] = field(default_factory=list)
    # Append-only notes from each loop turn (log snippets, query results).
    # Unlike observation, evidence accumulates so Guard and humans can audit
    # why the agent chose a path.
    #
    # Why default_factory=list instead of evidence=[]?
    # In Python, a mutable default like [] is created ONCE and shared by
    # every instance that omits the field. Two states would share the same
    # list — a silent, dangerous bug. default_factory=list creates a fresh
    # empty list for each new PipelineReliabilityState.

    attempted_action: str | None = None
    # The action Decide chose before Guard and Execute (e.g. "RETRY").
    # Distinct from outcome, which records what happened after execution.

    awaiting_human: bool = False
    # True = workflow paused for human approve/reject; agent must not run Decide/Guard/Execute.

    approved_action: str | None = None
    # Human-authorized action to run once on resume; None = no pending approval.

    escalation_reason: str = ""
    # Why ASK_HUMAN was chosen — set at pause so humans see intent without re-reading Decide.

    suggested_action: str | None = None
    # Non-binding recommendation for the human (e.g. STOP_SAFE); not auto-executed.

    human_request_id: str = ""
    # Pointer to the persisted HumanRequest (HITL V1). Empty = no durable ticket.
    # The request file is the HITL source of truth; this is only a correlation id.

    last_human_decision: str = ""
    # Last applied HITL decision: "" | "APPROVE" | "REJECT" | "EDIT".
    # REJECT tells Decide not to immediately re-ASK_HUMAN on the same snapshot.

    # --- 4. Terminal ---------------------------------------------------------
    # When set, the incident loop is done (success, escalation, or safe stop).

    outcome: str | None = None
    # Terminal or in-progress result: "retried", "escalated", "resolved", etc.
    # None means the incident is still open and the loop should continue.
    #
    # Why keep State small?
    # Only include fields needed for the *next* decision. Full chat transcripts,
    # raw API payloads, and historical metrics belong in logs or external stores.
    # A bloated State is hard to test, hard to review in production, and tempts
    # the agent to over-fit on noise instead of the facts that matter now.
