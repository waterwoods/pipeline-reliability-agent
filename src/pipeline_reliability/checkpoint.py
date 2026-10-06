"""Pipeline Reliability Agent — Checkpoint / Resume.

Save and restore PipelineReliabilityState plus RunTrace to a local JSON file
so a paused or crashed worker can continue the same incident.

    State + Trace -> save_checkpoint -> JSON file
    load_checkpoint -> State
    load_trace -> RunTrace
    run_agent(state, trace=loaded_or_omitted, checkpoint_path=...)

RESUME != REPLAY. A checkpoint may lag behind an external RETRY that the
orchestrator already accepted, or a BACKFILL_PARTITION that the warehouse
already wrote. load_checkpoint reconstructs State only — it does not run
Decide, Guard, or Execute. If a durable RETRY or BACKFILL intent is present,
it forces re-observation of that system so resume cannot treat a stale
empty side-effect as a license to mutate again.

The caller resumes by submitting a HITL decision (see ``hitl.py``) or by
setting approved_action (via approve) and calling run_agent again. Passing
the same checkpoint_path into run_agent continues an in-process checkpoint.
When ``trace`` is omitted, run_agent reloads Trace history from this file.
Resume is not replay of a tool side effect.

CheckpointStore is the durable run record for one agent_run_id. FileCheckpointStore
is the default. PostgresCheckpointStore is opt-in and stores the same snapshot
in a separate database. Neither store chooses an action or mutates the orchestrator.
"""

from __future__ import annotations

import json
import os
import uuid
from collections.abc import Mapping
from dataclasses import asdict, dataclass, fields
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Protocol

from pipeline_reliability.observability import RunTrace, trace_from_dict, trace_to_dict
from pipeline_reliability.state import ArrivalItem, PipelineReliabilityState

_ARRIVAL_ITEM_FIELDS = frozenset(item.name for item in fields(ArrivalItem))

RETRY_INTENT_NOTE = "retry_intent: RETRY about to be dispatched"
DEFAULT_CHECKPOINT_DIR = Path(".local/checkpoints")
DEMO_CHECKPOINT_PATH = DEFAULT_CHECKPOINT_DIR / "demo.json"


def save_checkpoint(
    state: PipelineReliabilityState,
    path: str | Path,
    trace: RunTrace | None = None,
) -> None:
    """Serialize State and RunTrace to JSON and replace path atomically.

    After every Apply the runner passes the in-memory ``trace`` so the latest
    TraceStep is durable. Callers that omit ``trace`` (RETRY intent, HITL)
    keep the Trace already on disk so a State-only write cannot wipe history.
    """
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    if trace is not None:
        trace_payload = trace_to_dict(trace)
    else:
        existing = _read_payload(target)
        _, existing_trace = _split_payload(existing) if existing is not None else (None, None)
        trace_payload = existing_trace if existing_trace is not None else trace_to_dict(RunTrace())
    payload = json.dumps(
        {"state": asdict(state), "trace": trace_payload},
        indent=2,
        sort_keys=True,
    )
    tmp_path = target.with_name(target.name + ".tmp")
    tmp_path.write_text(payload, encoding="utf-8")
    tmp_path.replace(target)


def mark_retry_intent(state: PipelineReliabilityState) -> None:
    """Mark one pending RETRY on State. Does not write a checkpoint.

    The pending RETRY gets a stable ``action_id`` here, before the mutation.
    A second call reuses that id and does not move the frozen baseline.
    """
    if not (state.action_id or "").strip():
        state.action_id = str(uuid.uuid4())
    freeze_retry_baseline(state)
    state.retry_side_effect = "UNKNOWN"
    state.orchestrator_status = ""
    if RETRY_INTENT_NOTE not in state.evidence:
        state.evidence.append(RETRY_INTENT_NOTE)


def retry_mutation_phase(state: PipelineReliabilityState) -> str:
    """Read the durable RETRY phase from fields CheckpointStore already saves.

    ``RETRY_INTENT_RECORDED`` — intent is durable and Apply has not confirmed
    a dispatch. ``UNKNOWN`` — a dispatch outcome was recorded and is still
    unresolved. ``CONFIRMED`` — Apply recorded an accepted or rejected RETRY
    whose side effect is not UNKNOWN. Empty when this state has no RETRY
    intent or result.
    """
    if state.retry_side_effect == "UNKNOWN":
        if state.attempted_action == "RETRY":
            return "UNKNOWN"
        if RETRY_INTENT_NOTE in state.evidence:
            return "RETRY_INTENT_RECORDED"
        return "UNKNOWN"
    if state.attempted_action == "RETRY":
        return "CONFIRMED"
    return ""


def record_retry_intent(state: PipelineReliabilityState, path: str | Path) -> None:
    """Durably record that RETRY is about to be dispatched, then persist.

    INTENT → SIDE EFFECT → RESULT. A checkpoint written only after Apply cannot
    know whether the orchestrator already accepted the mutation. Writing UNKNOWN before
    the adapter call closes the crash window: resume reconstructs uncertainty
    and reconciles instead of replaying RETRY. Trace history already on disk is
    preserved (this write happens before the current step is appended).

    The pending RETRY gets a stable ``action_id`` here, before the mutation.
    Process restart must reuse that id — it must not mint a second identity
    for the same intent.
    """
    mark_retry_intent(state)
    save_checkpoint(state, path)


def record_backfill_intent(
    state: PipelineReliabilityState,
    asset: str,
    partition: str,
    path: str | Path,
) -> None:
    """Durably record that BACKFILL_PARTITION is about to mutate the warehouse.

    INTENT → SIDE EFFECT → RESULT. A checkpoint written only after Apply cannot
    know whether the warehouse already accepted the write. Writing UNKNOWN
    plus the exact asset+partition before the adapter call closes the crash
    window: resume reconstructs uncertainty and reconciles instead of
    replaying BACKFILL_PARTITION. Does not execute any warehouse operation.
    Trace history already on disk is preserved (State-only write).
    """
    state.backfill_side_effect = "UNKNOWN"
    state.backfill_asset = asset
    state.backfill_partition = partition
    save_checkpoint(state, path)


def persist_store_retry_intent(
    state: PipelineReliabilityState,
    store: CheckpointStore,
    record: CheckpointRecord,
) -> CheckpointRecord:
    """Write the RETRY intent onto this agent_run_id before the mutation.

    Same fields as ``record_retry_intent``: ``action_id``, frozen baseline,
    ``retry_side_effect=UNKNOWN``, and the intent note. The store status stays
    RUNNING. ``record.version`` advances so the post-Apply save matches.
    """
    mark_retry_intent(state)
    record.state = state
    record.status = RUNNING
    saved = store.save(record)
    record.version = saved.version
    record.updated_at = saved.updated_at
    record.created_at = saved.created_at or record.created_at
    return saved


def freeze_retry_baseline(state: PipelineReliabilityState) -> None:
    """Snapshot orchestrator identity once, before UNKNOWN is recorded.

    Later CHECKs update ``attempt_number`` / ``latest_repair_id``. Reconciliation
    compares those live fields to this frozen baseline after process restart.
    No-op when UNKNOWN is already set so a second intent write cannot move the
    baseline.
    """
    if state.retry_side_effect == "UNKNOWN":
        return
    state.retry_baseline_attempt_number = state.attempt_number
    state.retry_baseline_repair_id = state.latest_repair_id


def _reconstruct_uncertain_retry(state: PipelineReliabilityState) -> PipelineReliabilityState:
    """Stale orchestrator status is not proof of the pending RETRY's result."""
    if state.retry_side_effect == "UNKNOWN":
        state.orchestrator_status = ""
    return state


def state_from_dict(payload: dict[str, Any]) -> PipelineReliabilityState:
    """Rebuild State from checkpoint JSON. UNKNOWN still forces re-observation."""
    hydrated = dict(payload)
    hydrated["arrival_items"] = _hydrate_arrival_items(hydrated.get("arrival_items"))
    return _reconstruct_uncertain_retry(PipelineReliabilityState(**hydrated))


def load_checkpoint(path: str | Path) -> PipelineReliabilityState:
    """Read JSON from path and reconstruct a new PipelineReliabilityState."""
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    state_payload, _ = _split_payload(payload)
    return state_from_dict(state_payload)


def _hydrate_arrival_items(raw: object) -> list[ArrivalItem]:
    """Rebuild ArrivalItem objects from asdict() JSON dicts.

    ``asdict`` flattens nested dataclasses to dicts. Without this, resume
    would leave ``arrival_items`` as raw dicts and ``item.status`` would fail.
    Missing / empty / absent ``arrival_items`` stays an empty list so legacy
    checkpoints remain loadable.
    """
    if not raw:
        return []
    if not isinstance(raw, list):
        raise TypeError("arrival_items must be a list")
    items: list[ArrivalItem] = []
    for item in raw:
        if isinstance(item, ArrivalItem):
            items.append(item)
            continue
        if not isinstance(item, dict):
            raise TypeError("each arrival_items entry must be a dict or ArrivalItem")
        cleaned = {key: value for key, value in item.items() if key in _ARRIVAL_ITEM_FIELDS}
        items.append(ArrivalItem(**cleaned))
    return items


def load_trace(path: str | Path) -> RunTrace:
    """Read RunTrace history from the same checkpoint file as State.

    Legacy State-only JSON (no ``trace`` key) returns an empty RunTrace.
    """
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    _, trace_payload = _split_payload(payload)
    return trace_from_dict(trace_payload)


def _read_payload(path: Path) -> dict[str, Any] | None:
    if not path.is_file():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def _split_payload(raw: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any] | None]:
    """Accept wrapped {state, trace} files and legacy State-only JSON."""
    nested = raw.get("state")
    if isinstance(nested, dict) and "pipeline" in nested:
        trace = raw.get("trace")
        return nested, trace if isinstance(trace, dict) else None
    return raw, None


# External checkpoint statuses. Internal Agent outcomes stay FINISH / STOP_SAFE / ASK_HUMAN.
RUNNING = "RUNNING"
WAITING_EXTERNAL = "WAITING_EXTERNAL"
COMPLETED = "COMPLETED"
STOP_SAFE_STATUS = "STOP_SAFE"
WAITING_FOR_HUMAN = "WAITING_FOR_HUMAN"
FAILED_STATUS = "FAILED"

CHECKPOINT_STORE_ENV = "PRA_CHECKPOINT_STORE"
CHECKPOINT_DIR_ENV = "PRA_CHECKPOINT_DIR"
CHECKPOINT_DATABASE_URL_ENV = "PRA_CHECKPOINT_DATABASE_URL"
RUNTIME_ENV = "PRA_RUNTIME"
LOCAL_RUNTIME = "local"
CONTAINER_RUNTIME = "container"


class CheckpointConfigError(Exception):
    """Process environment cannot assemble the checkpoint store."""


_OUTCOME_TO_STATUS = {
    "FINISH": COMPLETED,
    "STOP_SAFE": STOP_SAFE_STATUS,
    "ASK_HUMAN": WAITING_FOR_HUMAN,
}


class CheckpointConflict(RuntimeError):
    """save() lost a version or idempotency race. claim() reports that as None."""


class OwnershipLost(RuntimeError):
    """This worker no longer holds the expected version and lease.

    Raised before an external mutation. The checkpoint row is left as the
    current owner saved it.
    """


# How long a successful claim owns the run. After this, another worker may
# take the row. This is not a retry budget.
DEFAULT_LEASE_SECONDS = 30


def external_status(state: PipelineReliabilityState, *, paused_for_external: bool = False) -> str:
    """Map internal outcome to the store status. Does not change State."""
    if paused_for_external:
        return WAITING_EXTERNAL
    if state.awaiting_human or state.outcome == "ASK_HUMAN":
        return WAITING_FOR_HUMAN
    if state.outcome in _OUTCOME_TO_STATUS:
        return _OUTCOME_TO_STATUS[state.outcome]
    if state.outcome:
        return FAILED_STATUS
    return RUNNING


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


@dataclass
class CheckpointRecord:
    """One durable Agent run. State and trace stay the existing JSON shapes.

    The six schedule fields are the alarm clock and the ownership slip.
    They are not Agent state and they do not authorize RETRY.
    """

    agent_run_id: str
    status: str
    state: PipelineReliabilityState
    trace: RunTrace
    version: int
    idempotency_key: str | None = None
    created_at: str = ""
    updated_at: str = ""
    # When a WAITING_EXTERNAL run becomes eligible to wake. NULL means manual only.
    next_check_at: str | None = None
    # When polling must stop and the existing human-review path takes over.
    wait_deadline_at: str | None = None
    # Worker that currently owns this resume. NULL means nobody owns it.
    lease_owner: str | None = None
    # When that ownership expires. After this, another worker may claim.
    lease_until: str | None = None
    # How many times this run was woken. Not a retry count.
    wake_attempt: int = 0
    last_wake_at: str | None = None


class CheckpointStore(Protocol):
    """Smallest durable run API. Implementations must not call Decide or tools."""

    def save(self, record: CheckpointRecord) -> CheckpointRecord:
        """Insert version 0, or update when version matches. Returns the new version."""

    def load(self, agent_run_id: str) -> CheckpointRecord | None:
        """Return the snapshot, or None when the id is unknown."""

    def claim(
        self,
        agent_run_id: str,
        version: int,
        from_status: str,
        to_status: str,
        *,
        worker_id: str | None = None,
        lease_seconds: int = DEFAULT_LEASE_SECONDS,
        now: datetime | None = None,
    ) -> CheckpointRecord | None:
        """Conditional status change. None means this caller lost the race.

        ``worker_id`` takes the lease in the same update. A live lease held
        by anyone, including this worker, makes the claim lose. Callers that
        omit ``worker_id`` keep the previous version-and-status claim.
        """

    def find_by_idempotency_key(self, key: str) -> CheckpointRecord | None:
        """Return the run created with this key, or None."""

    def ping(self) -> None:
        """Raise when this store cannot be reached. Does not read or write a run."""


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def claim_clock(now: datetime | None = None) -> datetime:
    """UTC clock used for lease grants. Microseconds are dropped so the
    stored timestamp round-trips through the checkpoint JSON shape.
    """
    return _as_utc(now or datetime.now(timezone.utc)).replace(microsecond=0)


def lease_is_held(lease_until: str | None, now: datetime) -> bool:
    """True when ``lease_until`` is still strictly after ``now``.

    An empty lease is free. A lease at exactly ``now`` is expired.
    """
    text = (lease_until or "").strip()
    if not text:
        return False
    return _as_utc(datetime.fromisoformat(text)) > _as_utc(now)


def require_mutation_lease(
    store: CheckpointStore | None,
    record: CheckpointRecord | None,
    *,
    now: datetime | None = None,
) -> None:
    """Abort before a mutation when this worker lost version or lease.

    No-op when the record has no ``lease_owner``. File-store runs and any
    path that never took a lease keep their existing behavior. Reads are
    not checked: overlapping checks are allowed. A RETRY or backfill is not.
    """
    if record is None or not (record.lease_owner or "").strip():
        return
    owner = record.lease_owner.strip()
    if store is None:
        raise OwnershipLost(
            f"{record.agent_run_id}: leased mutation requires the checkpoint store"
        )
    clock = claim_clock(now) if now is not None else _as_utc(datetime.now(timezone.utc))
    current = store.load(record.agent_run_id)
    if (
        current is None
        or current.version != record.version
        or (current.lease_owner or "") != owner
        or (current.lease_until or "") != (record.lease_until or "")
        or not lease_is_held(current.lease_until, clock)
    ):
        raise OwnershipLost(
            f"{record.agent_run_id}: lost lease before mutation "
            f"(owner={owner}, version={record.version})"
        )


def _atomic_write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_name(path.name + ".tmp")
    tmp_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    tmp_path.replace(path)


def _safe_run_id(agent_run_id: str) -> str:
    run_id = (agent_run_id or "").strip()
    if not run_id or run_id in {".", ".."} or "/" in run_id or "\\" in run_id:
        raise ValueError("agent_run_id must be a single path segment")
    return run_id


class FileCheckpointStore:
    """One JSON file per agent_run_id. Single-process demo behavior.

    Path: ``<directory>/{agent_run_id}.json``. The legacy save_checkpoint helper
    is unchanged and is not pointed at these files.
    """

    def __init__(self, directory: str | Path | None = None) -> None:
        self._directory = Path(directory) if directory else DEFAULT_CHECKPOINT_DIR

    def _path(self, agent_run_id: str) -> Path:
        return self._directory / f"{_safe_run_id(agent_run_id)}.json"

    def save(self, record: CheckpointRecord) -> CheckpointRecord:
        path = self._path(record.agent_run_id)
        existing = self._read(path)
        now = _utc_now()
        if record.idempotency_key:
            other = self.find_by_idempotency_key(record.idempotency_key)
            if other is not None and other.agent_run_id != record.agent_run_id:
                raise CheckpointConflict(
                    f"idempotency key already belongs to {other.agent_run_id}"
                )
        if existing is None:
            if record.version != 0:
                raise CheckpointConflict(
                    f"cannot update missing checkpoint {record.agent_run_id}"
                )
            version = 1
            created_at = record.created_at or now
        else:
            if existing.get("version") != record.version:
                raise CheckpointConflict(
                    f"version conflict for {record.agent_run_id}: "
                    f"store has {existing.get('version')}, save has {record.version}"
                )
            version = int(existing["version"]) + 1
            created_at = str(existing.get("created_at") or now)
        payload = _record_payload(
            record,
            version=version,
            created_at=created_at,
            updated_at=now,
        )
        _atomic_write_json(path, payload)
        return _record_from_payload(payload)

    def load(self, agent_run_id: str) -> CheckpointRecord | None:
        return self._load_path(self._path(agent_run_id))

    def claim(
        self,
        agent_run_id: str,
        version: int,
        from_status: str,
        to_status: str,
        *,
        worker_id: str | None = None,
        lease_seconds: int = DEFAULT_LEASE_SECONDS,
        now: datetime | None = None,
    ) -> CheckpointRecord | None:
        current = self.load(agent_run_id)
        if current is None or current.status != from_status or current.version != version:
            return None
        if worker_id is not None:
            # Single-process stand-in for the Postgres lease update. Two
            # OS processes can still race here; Postgres is the multi-worker store.
            owner = worker_id.strip()
            if not owner:
                raise ValueError("worker_id is required to take a lease")
            if lease_seconds < 1:
                raise ValueError("lease_seconds must be positive")
            clock = claim_clock(now)
            if lease_is_held(current.lease_until, clock):
                return None
            current.lease_owner = owner
            current.lease_until = (clock + timedelta(seconds=lease_seconds)).isoformat()
            current.last_wake_at = clock.isoformat()
            current.wake_attempt = int(current.wake_attempt or 0) + 1
        current.status = to_status
        try:
            return self.save(current)
        except CheckpointConflict:
            return None

    def claim_due_waiting(
        self,
        worker_id: str,
        now: datetime,
        *,
        lease_seconds: int = DEFAULT_LEASE_SECONDS,
        limit: int = 1,
        agent_run_id: str | None = None,
        before_commit: Any | None = None,
    ) -> list[CheckpointRecord]:
        """Single-process stand-in for the Postgres due-queue claim.

        Same predicate as ``PostgresCheckpointStore.claim_due_waiting``:
        WAITING_EXTERNAL, alarm fired, lease free. Two OS processes can still
        race. This process claims one row at a time through ``claim``, which
        refuses a live lease. State JSON is not rewritten here.
        """
        return self._claim_matching(
            worker_id,
            now,
            lease_seconds=lease_seconds,
            limit=limit,
            agent_run_id=agent_run_id,
            before_commit=before_commit,
            want_status=WAITING_EXTERNAL,
            to_status=RUNNING,
            require_due=True,
        )

    def reclaim_expired(
        self,
        worker_id: str,
        now: datetime,
        *,
        lease_seconds: int = DEFAULT_LEASE_SECONDS,
        limit: int = 1,
        agent_run_id: str | None = None,
        before_commit: Any | None = None,
    ) -> list[CheckpointRecord]:
        """Single-process stand-in for taking an expired RUNNING lease.

        Status stays RUNNING. A row that never took a lease is left alone.
        State JSON is not rewritten, so this cannot replay RETRY.
        """
        return self._claim_matching(
            worker_id,
            now,
            lease_seconds=lease_seconds,
            limit=limit,
            agent_run_id=agent_run_id,
            before_commit=before_commit,
            want_status=RUNNING,
            to_status=RUNNING,
            require_due=False,
        )

    def _claim_matching(
        self,
        worker_id: str,
        now: datetime,
        *,
        lease_seconds: int,
        limit: int,
        agent_run_id: str | None,
        before_commit: Any | None,
        want_status: str,
        to_status: str,
        require_due: bool,
    ) -> list[CheckpointRecord]:
        if limit < 1:
            raise ValueError("limit must be positive")
        owner = (worker_id or "").strip()
        if not owner:
            raise ValueError("worker_id is required to take a lease")
        clock = claim_clock(now)
        wanted = (agent_run_id or "").strip()
        candidates: list[CheckpointRecord] = []
        for record in self._iter_records():
            if wanted and record.agent_run_id != wanted:
                continue
            if record.status != want_status:
                continue
            if require_due:
                if not record.next_check_at:
                    continue
                due_at = _as_utc(datetime.fromisoformat(record.next_check_at))
                if due_at > clock:
                    continue
            elif not (record.lease_until or "").strip():
                continue
            if lease_is_held(record.lease_until, clock):
                continue
            candidates.append(record)
        if require_due:
            candidates.sort(key=lambda item: (item.next_check_at or "", item.agent_run_id))
        else:
            candidates.sort(key=lambda item: (item.lease_until or "", item.agent_run_id))
        won: list[CheckpointRecord] = []
        for record in candidates:
            if len(won) >= limit:
                break
            claimed = self.claim(
                record.agent_run_id,
                record.version,
                want_status,
                to_status,
                worker_id=owner,
                lease_seconds=lease_seconds,
                now=clock,
            )
            if claimed is not None:
                won.append(claimed)
        if won and before_commit is not None:
            before_commit()
        return won

    def _iter_records(self) -> list[CheckpointRecord]:
        if not self._directory.is_dir():
            return []
        records: list[CheckpointRecord] = []
        for path in self._directory.glob("*.json"):
            loaded = self._load_path(path)
            if loaded is not None:
                records.append(loaded)
        return records

    def find_by_idempotency_key(self, key: str) -> CheckpointRecord | None:
        wanted = (key or "").strip()
        if not wanted or not self._directory.is_dir():
            return None
        for path in self._directory.glob("*.json"):
            payload = self._read(path)
            if payload is None or payload.get("idempotency_key") != wanted:
                continue
            if not payload.get("agent_run_id"):
                continue
            return _record_from_payload(payload)
        return None

    def ping(self) -> None:
        self._directory.mkdir(parents=True, exist_ok=True)
        if not self._directory.is_dir() or not os.access(self._directory, os.W_OK):
            raise OSError("checkpoint directory is not reachable")

    def _load_path(self, path: Path) -> CheckpointRecord | None:
        payload = self._read(path)
        if payload is None or not payload.get("agent_run_id"):
            return None
        return _record_from_payload(payload)

    def _read(self, path: Path) -> dict[str, Any] | None:
        if not path.is_file():
            return None
        raw = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(raw, dict):
            return None
        return raw


def _record_payload(
    record: CheckpointRecord,
    *,
    version: int,
    created_at: str,
    updated_at: str,
) -> dict[str, Any]:
    return {
        "agent_run_id": record.agent_run_id,
        "status": record.status,
        "version": version,
        "idempotency_key": record.idempotency_key,
        "created_at": created_at,
        "updated_at": updated_at,
        "next_check_at": record.next_check_at,
        "wait_deadline_at": record.wait_deadline_at,
        "lease_owner": record.lease_owner,
        "lease_until": record.lease_until,
        "wake_attempt": int(record.wake_attempt or 0),
        "last_wake_at": record.last_wake_at,
        "state": asdict(record.state),
        "trace": trace_to_dict(record.trace),
    }


def _record_from_payload(payload: dict[str, Any]) -> CheckpointRecord:
    state_payload = payload.get("state")
    if not isinstance(state_payload, dict):
        raise ValueError("checkpoint record is missing state")
    return CheckpointRecord(
        agent_run_id=str(payload["agent_run_id"]),
        status=str(payload["status"]),
        state=state_from_dict(state_payload),
        trace=trace_from_dict(payload.get("trace") if isinstance(payload.get("trace"), dict) else None),
        version=int(payload["version"]),
        idempotency_key=payload.get("idempotency_key") or None,
        created_at=str(payload.get("created_at") or ""),
        updated_at=str(payload.get("updated_at") or ""),
        next_check_at=_optional_text(payload.get("next_check_at")),
        wait_deadline_at=_optional_text(payload.get("wait_deadline_at")),
        lease_owner=_optional_text(payload.get("lease_owner")),
        lease_until=_optional_text(payload.get("lease_until")),
        wake_attempt=int(payload.get("wake_attempt") or 0),
        last_wake_at=_optional_text(payload.get("last_wake_at")),
    )


def _optional_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def runtime_name(environ: Mapping[str, str] | None = None) -> str:
    """``local`` outside the image. ``container`` rejects file checkpoints."""
    env = os.environ if environ is None else environ
    raw = (env.get(RUNTIME_ENV) or "").strip().lower()
    if raw in ("", LOCAL_RUNTIME):
        return LOCAL_RUNTIME
    if raw == CONTAINER_RUNTIME:
        return CONTAINER_RUNTIME
    raise CheckpointConfigError(
        f"{RUNTIME_ENV}={raw!r} is not supported; use {LOCAL_RUNTIME!r} or {CONTAINER_RUNTIME!r}."
    )


def container_runtime(environ: Mapping[str, str] | None = None) -> bool:
    return runtime_name(environ) == CONTAINER_RUNTIME


def store_from_environ(environ: Mapping[str, str] | None = None) -> CheckpointStore:
    """Build the process checkpoint store.

    Local development (``PRA_RUNTIME`` unset or ``local``) defaults to a file
    directory. The container image sets ``PRA_RUNTIME=container`` and accepts
    only Postgres. A missing or blank ``PRA_CHECKPOINT_DATABASE_URL`` is an
    error. There is no built-in DSN. This function does not open the Airflow
    metadata database and does not read ``AIRFLOW__DATABASE__SQL_ALCHEMY_CONN``.
    """
    env = os.environ if environ is None else environ
    runtime = runtime_name(env)
    mode = (env.get(CHECKPOINT_STORE_ENV) or "").strip().lower()
    if runtime == CONTAINER_RUNTIME and mode != "postgres":
        raise CheckpointConfigError(
            "container runtime requires "
            f"{CHECKPOINT_STORE_ENV}=postgres and {CHECKPOINT_DATABASE_URL_ENV}. "
            "File checkpoints are only for local development."
        )
    if mode in ("", "file"):
        directory = (env.get(CHECKPOINT_DIR_ENV) or "").strip()
        return FileCheckpointStore(directory or DEFAULT_CHECKPOINT_DIR)
    if mode != "postgres":
        raise CheckpointConfigError(
            f"{CHECKPOINT_STORE_ENV}={mode!r} is not supported; use 'file' or 'postgres'."
        )
    from pipeline_reliability.postgres_checkpoint import PostgresCheckpointStore

    dsn = (env.get(CHECKPOINT_DATABASE_URL_ENV) or "").strip()
    if not dsn:
        raise CheckpointConfigError(
            f"{CHECKPOINT_STORE_ENV}=postgres requires {CHECKPOINT_DATABASE_URL_ENV}."
        )
    store = PostgresCheckpointStore(dsn)
    store.ensure_schema()
    return store


def load_runtime_store(environ: Mapping[str, str] | None = None) -> CheckpointStore:
    """Open the store for process startup.

    Configuration errors keep their public message. Connection failures become
    ``checkpoint store is unavailable`` so a DSN cannot land in the process log.
    Container startup also pings. Local startup does not.
    """
    env = os.environ if environ is None else environ
    try:
        store = store_from_environ(env)
    except CheckpointConfigError:
        raise
    except Exception:
        raise CheckpointConfigError("checkpoint store is unavailable") from None
    if runtime_name(env) != CONTAINER_RUNTIME:
        return store
    try:
        store.ping()
    except Exception:
        raise CheckpointConfigError("checkpoint store is unavailable") from None
    return store
