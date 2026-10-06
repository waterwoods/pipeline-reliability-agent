"""One-line JSON logs for operators. Observation only.

Stdout carries info. Stderr carries warning and error. A broken stream, a
redaction miss, or any other logging error is discarded. This module does
not Decide, Guard, Apply, or change a checkpoint.
"""

from __future__ import annotations

import json
import os
import re
import sys
from contextlib import contextmanager
from contextvars import ContextVar, Token
from datetime import datetime, timezone
from typing import Any, Iterator

# Same triggers as the API's public config errors: a URL, a userinfo
# separator, or an embedded credential. The whole value is replaced.
_REQUEST_ID_RE = re.compile(r"^[A-Za-z0-9._:-]{1,128}$")
_MAX_TEXT = 500

_request_id: ContextVar[str] = ContextVar("pra_request_id", default="")
_agent_run_id: ContextVar[str] = ContextVar("pra_agent_run_id", default="")
_worker_id: ContextVar[str] = ContextVar("pra_worker_id", default="")
_process: ContextVar[str] = ContextVar("pra_process", default="")

_MUTATIONS = frozenset({"RETRY", "BACKFILL_PARTITION", "APPLY_APPROVED_REPAIR"})
_RECONCILE_ACTIONS = frozenset(
    {"CHECK_ORCHESTRATOR_RUN", "CHECK_WAREHOUSE_JOB", "RECONCILE_BACKFILL"}
)


def redact_text(value: str) -> str:
    """Replace a value that could carry a DSN, token, or password."""
    lowered = value.lower()
    if (
        "://" in value
        or "@" in value
        or "password=" in lowered
        or "bearer " in lowered
        or "token=" in lowered
        or "secret=" in lowered
        or "api_key=" in lowered
        or "apikey=" in lowered
    ):
        return "[redacted]"
    return value


def incoming_request_id(raw: str | None) -> str:
    """Accept a caller-supplied id only when it is a short safe token."""
    if not raw:
        return ""
    candidate = raw.strip()
    if not _REQUEST_ID_RE.fullmatch(candidate):
        return ""
    if redact_text(candidate) != candidate:
        return ""
    return candidate


def bind_request_id(request_id: str) -> Token[str]:
    return _request_id.set(request_id)


def reset_request_id(token: Token[str]) -> None:
    _reset(lambda: _request_id.reset(token))


def bind_agent_run_id(agent_run_id: str) -> Token[str]:
    return _agent_run_id.set(agent_run_id)


def reset_agent_run_id(token: Token[str]) -> None:
    _reset(lambda: _agent_run_id.reset(token))


@contextmanager
def bound_worker(worker_id: str, *, process: str) -> Iterator[None]:
    """Pin process and worker for the logs emitted inside one wake pass."""
    worker_token = _worker_id.set(worker_id)
    process_token = _process.set(process)
    try:
        yield
    finally:
        _reset(lambda: _worker_id.reset(worker_token))
        _reset(lambda: _process.reset(process_token))


def current_process() -> str:
    pinned = _process.get()
    if pinned:
        return pinned
    if _request_id.get():
        return "api"
    if (os.environ.get("PRA_WAKE_WORKER") or "").strip().lower() == "true":
        return "wake"
    return "runner"


def side_effect_status(
    *,
    action: str,
    guard_allowed: bool,
    observation: object,
    retry_side_effect: str = "",
    backfill_side_effect: str = "",
) -> str:
    """Project an already-finished step onto completed / not_executed / unknown.

    Empty means this step did not pose that question. The projection is not
    a new side-effect decision.
    """
    if not guard_allowed:
        return "not_executed"
    observed = str(getattr(observation, "side_effect", "") or "")
    if (
        observed == "UNKNOWN"
        or retry_side_effect == "UNKNOWN"
        or backfill_side_effect == "UNKNOWN"
    ):
        return "unknown"
    if action not in _MUTATIONS:
        return ""
    accepted = getattr(observation, "accepted", None)
    if accepted is True:
        return "completed"
    if accepted is False:
        return "not_executed"
    applied = getattr(observation, "applied", None)
    if applied is False:
        return "not_executed"
    return "completed"


def reconcile_result(action: str, facts: object | None) -> str:
    """Compact reconcile crumb already stored on the trace step."""
    if facts is None:
        return ""
    recorded = str(getattr(facts, "reconcile_result", "") or "")
    if recorded:
        return recorded
    if action not in _RECONCILE_ACTIONS:
        return ""
    return str(getattr(facts, "result", "") or getattr(facts, "status", "") or "")


def emit(level: str, event: str, **fields: Any) -> None:
    """Write one JSON object. Never raises."""
    try:
        _write(level, event, fields)
    except Exception:
        return


def _write(level: str, event: str, fields: dict[str, Any]) -> None:
    clean_level = level if level in {"info", "warning", "error"} else "info"
    payload: dict[str, Any] = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "level": clean_level,
        "event": _text(event) or "log",
        "pid": os.getpid(),
        "process": _text(str(fields.get("process") or current_process())) or "runner",
    }
    request_id = _chosen(fields, "request_id", _request_id.get())
    if request_id:
        payload["request_id"] = request_id
        payload["correlation_id"] = request_id
    agent_run_id = _chosen(fields, "agent_run_id", _agent_run_id.get())
    if agent_run_id:
        payload["agent_run_id"] = agent_run_id
    worker_id = _chosen(fields, "worker_id", _worker_id.get())
    if worker_id:
        payload["worker_id"] = worker_id
    for key in (
        "action",
        "guard_reason",
        "side_effect_status",
        "reconcile_result",
        "status_from",
        "status_to",
        "outcome",
        "pipeline",
        "http_method",
        "http_path",
        "error_type",
    ):
        if key not in fields or fields[key] is None or fields[key] == "":
            continue
        payload[key] = _text(str(fields[key]))
    if isinstance(fields.get("guard_allowed"), bool):
        payload["guard_allowed"] = fields["guard_allowed"]
    for key in (
        "duration_ms",
        "http_status",
        "step_number",
        "reclaimed_count",
        "claimed_count",
    ):
        value = fields.get(key)
        if isinstance(value, bool) or value is None:
            continue
        if isinstance(value, int):
            payload[key] = value
        elif isinstance(value, float):
            payload[key] = round(value, 3)
    line = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    stream = sys.stderr if clean_level in {"warning", "error"} else sys.stdout
    stream.write(line + "\n")
    stream.flush()


def _chosen(fields: dict[str, Any], key: str, fallback: str) -> str:
    """Use an explicit value. A blank explicit value keeps the context value."""
    if key in fields and fields[key] not in (None, ""):
        return _text(str(fields[key]))
    return _text(fallback)


def _text(value: str) -> str:
    if not value:
        return ""
    collapsed = value.replace("\r", " ").replace("\n", " ").strip()
    if len(collapsed) > _MAX_TEXT:
        collapsed = collapsed[:_MAX_TEXT]
    return redact_text(collapsed)


def _reset(action: Any) -> None:
    try:
        action()
    except Exception:
        return
