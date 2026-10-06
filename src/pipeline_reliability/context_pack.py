"""Pipeline Reliability Agent — Context Pack V1.

Read-only incident evidence for a human on-call engineer and a future LLM
Advisor. The deterministic Agent investigates first; this module only
summarizes what is already on State.

ContextPack is information. It is not State, not an Action, and not
authorization. Building a pack must not mutate State or call tools.
"""

from __future__ import annotations

import re
from dataclasses import asdict, dataclass, fields
from typing import Any

from pipeline_reliability.adapters import TaskLogResult
from pipeline_reliability.recovery import evaluate_retry_safety
from pipeline_reliability.state import PipelineReliabilityState

# Hard cap inside the 4–8k window. Preserve original text; never LLM-summarize.
LOG_EXCERPT_MAX_CHARS = 6000
SECONDARY_EVIDENCE_MAX_CHARS = 800
_LOG_CONTEXT_PREFIX_CHARS = 400

LOG_SOURCE_TASK_LOG = "task_log"
LOG_SOURCE_FALLBACK = "fallback"

_CONTROL_NOTES = frozenset({"ASK_HUMAN", "STOP_SAFE", "FINISH"})
_SKIP_EVIDENCE_PREFIXES = (
    "hitl_request:",
    "human_approved:",
    "human_rejected:",
)

# Prefer the last traceback, then Error / Exception / FAILED / quota markers.
_TRACEBACK_ANCHOR = "traceback (most recent call last):"
_MARKER_RE = re.compile(
    r"(?i)(\berror\b|\bexception\b|\bfailed\b|http 429|quotaexceeded|retry-after)"
)

# Strong task-log markers. Generic "failed" is not enough — warehouse notes
# also say FAILED and must not compete for log_excerpt.
_TASK_LOG_HINTS = (
    "traceback (most recent call last):",
    "schema_drift",
    "schemaerror",
    "http 429",
    "quotaexceeded",
    "retry-after",
    "too many requests",
    "timeouterror",
    "permissiondenied",
    "dataconflict",
    "runtimeerror",
    "task exited with return code",
    "source_file_missing",
    "data_validation_failed",
)

# Apply warehouse/downstream crumbs. Not the task failure log.
_WAREHOUSE_HINTS = (
    "rows_written",
    "no load job",
    "warehouse load",
    "warehouse:",
    "bigquery job-api",
    "no_job",
)
_DOWNSTREAM_HINTS = (
    "downstream",
    "blast-radius",
    "blast radius",
    "affected_assets",
    "affected assets",
)


@dataclass(frozen=True)
class ContextPack:
    """Compact incident evidence. Not State. Not an Action. Not Guard."""

    pipeline: str = ""
    run_id: str = ""
    task_id: str = ""
    orchestrator_status: str = ""

    error: str = ""
    log_excerpt: str = ""
    log_source: str = ""
    warehouse_evidence: str = ""
    downstream_evidence: str = ""
    escalation_reason: str = ""

    warehouse_status: str = ""
    rows_written: int | None = None
    partial_write: bool | None = None
    retry_may_duplicate: bool | None = None

    downstream_impact: str = ""

    retries: int = 0
    retry_side_effect: str = ""
    attempt_number: int | None = None
    latest_repair_id: str = ""
    repair_recipe_found: bool | None = None
    repair_recipe_approved: bool | None = None

    # Snapshot of existing recovery policy. Informational only — not authority.
    retry_safety_verdict: str = ""
    retry_safety_reason: str = ""


def compact_log_excerpt(
    text: str | None,
    *,
    max_chars: int = LOG_EXCERPT_MAX_CHARS,
) -> str:
    """Return a bounded original-text excerpt. Empty / None → "".

    Prefers the last traceback or Error / Exception / FAILED / HTTP 429 /
    QuotaExceeded / Retry-After region. Otherwise keeps a tail window.
    Never invents or rewrites log text. Callers must pass the real task log,
    not a later warehouse or downstream note.
    """
    if text is None:
        return ""
    raw = text if isinstance(text, str) else str(text)
    if not raw.strip():
        return ""
    limit = max(1, int(max_chars))
    if len(raw) <= limit:
        return raw
    start = _excerpt_start(raw, limit)
    return raw[start : start + limit]


def build_context_pack(
    state: PipelineReliabilityState,
    *,
    log: TaskLogResult | str | None = None,
    escalation_reason: str = "",
) -> ContextPack:
    """Copy compact facts from State. Pure: no I/O, no State mutation, no Action.

    ``log`` overrides the task-log excerpt. When omitted, evidence is split:
    task-log notes fill ``log_excerpt``; warehouse/downstream notes stay in
    their own fields and cannot hide the task log.
    """
    safety = evaluate_retry_safety(state)
    reason = (escalation_reason or "").strip()
    classified = _classify_evidence(state)
    if log is not None:
        excerpt_source = _log_text(log)
        log_source = LOG_SOURCE_TASK_LOG if excerpt_source.strip() else ""
    elif classified.task_log:
        excerpt_source = classified.task_log
        log_source = LOG_SOURCE_TASK_LOG
    else:
        excerpt_source = classified.fallback
        log_source = LOG_SOURCE_FALLBACK if excerpt_source.strip() else ""
    return ContextPack(
        pipeline=state.pipeline or "",
        run_id=state.run_id or "",
        task_id=state.task_id or "",
        orchestrator_status=state.orchestrator_status or "",
        error=state.error or "",
        log_excerpt=compact_log_excerpt(excerpt_source),
        log_source=log_source,
        warehouse_evidence=compact_log_excerpt(
            classified.warehouse, max_chars=SECONDARY_EVIDENCE_MAX_CHARS
        ),
        downstream_evidence=compact_log_excerpt(
            classified.downstream, max_chars=SECONDARY_EVIDENCE_MAX_CHARS
        ),
        escalation_reason=reason,
        warehouse_status=state.warehouse_status or "",
        rows_written=state.rows_written,
        partial_write=state.partial_write,
        retry_may_duplicate=state.retry_may_duplicate,
        downstream_impact=state.downstream_impact or "",
        retries=int(state.retries),
        retry_side_effect=state.retry_side_effect or "",
        attempt_number=state.attempt_number,
        latest_repair_id=state.latest_repair_id or "",
        repair_recipe_found=state.repair_recipe_found,
        repair_recipe_approved=state.repair_recipe_approved,
        retry_safety_verdict=safety.verdict or "",
        retry_safety_reason=safety.reason or "",
    )


def context_pack_to_dict(pack: ContextPack) -> dict[str, Any]:
    """JSON-ready mapping. Inverse of ``context_pack_from_dict``."""
    return asdict(pack)


def context_pack_from_dict(raw: dict[str, Any] | None) -> ContextPack | None:
    """Rebuild a pack from HITL JSON. Unknown keys are ignored."""
    if not raw:
        return None
    allowed = {item.name for item in fields(ContextPack)}
    return ContextPack(**{key: raw[key] for key in raw if key in allowed})


def _excerpt_start(text: str, max_chars: int) -> int:
    """Index of the preferred excerpt window. 0 if no marker is found."""
    lower = text.lower()
    traceback_at = lower.rfind(_TRACEBACK_ANCHOR)
    if traceback_at >= 0:
        return max(0, traceback_at - _LOG_CONTEXT_PREFIX_CHARS)
    last_marker = -1
    for match in _MARKER_RE.finditer(text):
        last_marker = match.start()
    if last_marker >= 0:
        return max(0, last_marker - _LOG_CONTEXT_PREFIX_CHARS)
    return max(0, len(text) - max_chars)


def _log_text(log: TaskLogResult | str) -> str:
    if isinstance(log, str):
        return log
    parts = [log.error_type, log.message, log.detail]
    return "\n".join(part for part in parts if part)


@dataclass(frozen=True)
class _ClassifiedEvidence:
    """Private split of append-only evidence. Not a public Evidence model."""

    task_log: str = ""
    warehouse: str = ""
    downstream: str = ""
    fallback: str = ""


def _has_hint(text: str, hints: tuple[str, ...]) -> bool:
    lower = text.lower()
    return any(hint in lower for hint in hints)


def _is_task_log_note(text: str) -> bool:
    """True for primary task-log evidence. Generic FAILED is not enough."""
    return _has_hint(text, _TASK_LOG_HINTS)


def _is_warehouse_note(text: str) -> bool:
    if _is_task_log_note(text):
        return False
    return _has_hint(text, _WAREHOUSE_HINTS)


def _is_downstream_note(text: str) -> bool:
    if _is_task_log_note(text):
        return False
    return _has_hint(text, _DOWNSTREAM_HINTS)


def _useful_evidence_notes(state: PipelineReliabilityState) -> list[str]:
    useful: list[str] = []
    for note in state.evidence:
        text = str(note or "")
        if not text.strip() or text in _CONTROL_NOTES:
            continue
        if text.startswith(_SKIP_EVIDENCE_PREFIXES):
            continue
        useful.append(text)
    return useful


def _classify_evidence(state: PipelineReliabilityState) -> _ClassifiedEvidence:
    """Split evidence by provenance. Secondary notes never become log_excerpt."""
    useful = _useful_evidence_notes(state)
    task_logs = [text for text in useful if _is_task_log_note(text)]
    warehouses = [text for text in useful if _is_warehouse_note(text)]
    downstreams = [text for text in useful if _is_downstream_note(text)]
    claimed = set(task_logs)
    claimed.update(warehouses)
    claimed.update(downstreams)
    leftovers = [text for text in useful if text not in claimed]
    fallback = leftovers[-1] if leftovers else ""
    if not fallback:
        observation = state.observation or ""
        if (
            observation.strip()
            and observation not in _CONTROL_NOTES
            and not observation.startswith(_SKIP_EVIDENCE_PREFIXES)
            and not _is_warehouse_note(observation)
            and not _is_downstream_note(observation)
        ):
            fallback = observation
    return _ClassifiedEvidence(
        task_log=task_logs[-1] if task_logs else "",
        warehouse=warehouses[-1] if warehouses else "",
        downstream=downstreams[-1] if downstreams else "",
        fallback=fallback,
    )
