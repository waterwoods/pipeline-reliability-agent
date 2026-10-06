"""Pipeline Reliability Agent — Real HITL V1 (lab / PoW).

Turns the existing ASK_HUMAN pause into a durable, auditable human workflow.

This module does **not** redesign the Agent loop. Humans never call tools.
Authorization is not Guard. Approval is not execution.

    Agent ASK_HUMAN
        → persist HumanRequest
        → human APPROVE / REJECT / EDIT
        → identity + authorization
        → persist HumanDecision  (before resume)
        → apply to State (approve / reject / allowlisted facts)
        → resume Agent
        → Decide → Guard → Execute or STOP_SAFE
        → audit (request/decision files + State.evidence + Trace)

CURRENT V1 persistence = local JSON files (atomic replace), same philosophy
as checkpoint.py. PRODUCTION TARGET = shared transactional store (PostgreSQL).
BigQuery Run History is analytics/audit history, not this mutable store.

LAB / PoW AUTHORIZATION MODEL — not enterprise authentication.
Identity = who (actor string). Authorization = whether that role may submit
this decision. Guard still runs after a successful APPROVE.
"""

from __future__ import annotations

import json
import uuid
from dataclasses import asdict, dataclass, field, fields
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Literal

from pipeline_reliability.apply import approve, reject
from pipeline_reliability.checkpoint import load_checkpoint, save_checkpoint
from pipeline_reliability.context_pack import (
    ContextPack,
    build_context_pack,
    context_pack_from_dict,
)
from pipeline_reliability.incident_analysis import (
    IncidentAnalysis,
    incident_analysis_from_dict,
)
from pipeline_reliability.decide import RETRY, STOP_SAFE
from pipeline_reliability.guard import guard
from pipeline_reliability.recovery import classify_write_risk
from pipeline_reliability.state import PipelineReliabilityState

# ---------------------------------------------------------------------------
# Closed vocabularies
# ---------------------------------------------------------------------------

HumanDecisionType = Literal["APPROVE", "REJECT", "EDIT"]
HumanRole = Literal["viewer", "operator", "approver"]
HumanRequestStatus = Literal["PENDING", "DECIDED", "APPLIED"]

APPROVE: HumanDecisionType = "APPROVE"
REJECT: HumanDecisionType = "REJECT"
EDIT: HumanDecisionType = "EDIT"

VIEWER: HumanRole = "viewer"
OPERATOR: HumanRole = "operator"
APPROVER: HumanRole = "approver"

PENDING: HumanRequestStatus = "PENDING"
DECIDED: HumanRequestStatus = "DECIDED"
APPLIED: HumanRequestStatus = "APPLIED"

ALLOWED_DECISIONS: tuple[HumanDecisionType, ...] = (APPROVE, REJECT, EDIT)
ALLOWED_ROLES: frozenset[str] = frozenset({VIEWER, OPERATOR, APPROVER})
DANGEROUS_ACTIONS: frozenset[str] = frozenset({RETRY})

# Smallest V1 human-fact allowlist. Not a general State mutation API.
# rows_written / warehouse_status are Guard inputs humans often confirm.
# file_present is already a first-class investigation fact.
HUMAN_ALLOWED_FACT_KEYS: frozenset[str] = frozenset(
    {
        "rows_written",
        "warehouse_status",
        "file_present",
    }
)
HUMAN_FORBIDDEN_FACT_KEYS: frozenset[str] = frozenset(
    {
        "approved_action",
        "suggested_action",
        "awaiting_human",
        "outcome",
        "retries",
        "retry_side_effect",
        "poll_count",
        "partial_write",
        "retry_may_duplicate",
        "wait_backoff_seconds",
        "last_human_decision",
        "human_request_id",
        "RETRY",
        "DELETE",
        "RUN_TOOL",
    }
)
_BIGQUERY_STATUS_VALUES: frozenset[str] = frozenset(
    {"", "SUCCEEDED", "FAILED", "RUNNING", "PENDING", "DONE", "UNKNOWN"}
)

DEFAULT_HITL_DIR = Path(".local/hitl")


# ---------------------------------------------------------------------------
# Data model — point-in-time HITL ticket, not a second State
# ---------------------------------------------------------------------------


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


@dataclass
class HumanRequest:
    """Frozen ASK_HUMAN ticket. Survives process restart. Not Agent State."""

    request_id: str
    agent_run_id: str
    pipeline: str
    task_id: str
    run_id: str
    reason: str
    evidence: list[str]
    suggested_action: str | None
    allowed_decisions: list[str]
    status: str
    created_at: str
    checkpoint_path: str = ""
    error: str = ""
    orchestrator_status: str = ""
    warehouse_status: str = ""
    rows_written: int | None = None
    # Read-only Context Pack. Not an Action and not Guard authorization.
    context_pack: ContextPack | None = None
    # Advisory only. Not State and not an executable instruction.
    incident_analysis: IncidentAnalysis | None = None


@dataclass
class HumanDecision:
    """One human input. Persisted before Agent resume. Not a tool call."""

    decision_id: str
    request_id: str
    actor: str
    role: str
    decision: str
    approved_action: str | None
    reason: str
    submitted_facts: dict[str, Any]
    decided_at: str
    authorization_allowed: bool = True
    authorization_reason: str = ""


@dataclass
class HitlAuditEvent:
    """Identity / authorization / duplicate attempt. Evidence, not execution."""

    at: str
    actor: str
    role: str
    attempted_decision: str
    identity_ok: bool
    authorization_allowed: bool
    duplicate: bool
    reason: str


@dataclass
class HitlRecord:
    """One request file: ticket + accepted decision + audit attempts."""

    request: HumanRequest
    decisions: list[HumanDecision] = field(default_factory=list)
    audit: list[HitlAuditEvent] = field(default_factory=list)


@dataclass(frozen=True)
class IdentityResult:
    ok: bool
    reason: str


@dataclass(frozen=True)
class AuthorizationResult:
    allowed: bool
    reason: str


@dataclass(frozen=True)
class HumanFactValidationResult:
    accepted: bool
    reason: str
    facts: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class HumanSubmissionResult:
    """Result of submit_human_decision. Never includes a tool side effect."""

    accepted: bool
    duplicate: bool
    identity_ok: bool
    authorization_allowed: bool
    reason: str
    request: HumanRequest | None
    decision: HumanDecision | None
    state: PipelineReliabilityState | None


# ---------------------------------------------------------------------------
# Identity + authorization (lab / PoW — not SSO)
# ---------------------------------------------------------------------------


def check_identity(actor: str) -> IdentityResult:
    """Who is making this decision? Empty actor is not an identity."""
    name = (actor or "").strip()
    if not name:
        return IdentityResult(
            ok=False,
            reason="identity rejected: actor is required (lab PoW identity, not SSO).",
        )
    if len(name) > 80:
        return IdentityResult(
            ok=False,
            reason="identity rejected: actor exceeds 80 characters.",
        )
    return IdentityResult(ok=True, reason="identity accepted (lab PoW).")


def authorize(
    role: str,
    decision: str,
    approved_action: str | None = None,
) -> AuthorizationResult:
    """Is this role allowed to submit this decision? Not a Guard check.

    LAB / PoW AUTHORIZATION MODEL
      viewer   — cannot submit decisions
      operator — REJECT and EDIT only
      approver — APPROVE, REJECT, EDIT (including dangerous RETRY)
    Authorization success does not bypass Guard.
    """
    if role not in ALLOWED_ROLES:
        return AuthorizationResult(
            allowed=False,
            reason=(
                f"authorization rejected: unknown role {role!r} "
                "(lab PoW roles: viewer, operator, approver)."
            ),
        )
    if decision not in ALLOWED_DECISIONS:
        return AuthorizationResult(
            allowed=False,
            reason=f"authorization rejected: decision {decision!r} is not allowed.",
        )
    if role == VIEWER:
        return AuthorizationResult(
            allowed=False,
            reason="authorization rejected: viewer cannot submit HITL decisions.",
        )
    if decision == APPROVE:
        if role != APPROVER:
            action = approved_action or "(none)"
            return AuthorizationResult(
                allowed=False,
                reason=(
                    f"authorization rejected: role {role!r} cannot APPROVE "
                    f"{action}; only approver may APPROVE."
                ),
            )
        if approved_action in DANGEROUS_ACTIONS:
            return AuthorizationResult(
                allowed=True,
                reason=(
                    "authorization allowed: approver may APPROVE RETRY "
                    "(Guard still applies; APPROVE is not Execute)."
                ),
            )
        return AuthorizationResult(
            allowed=True,
            reason="authorization allowed: approver may APPROVE.",
        )
    if decision in {REJECT, EDIT} and role in {OPERATOR, APPROVER}:
        return AuthorizationResult(
            allowed=True,
            reason=f"authorization allowed: {role} may {decision}.",
        )
    return AuthorizationResult(
        allowed=False,
        reason=f"authorization rejected: role {role!r} cannot {decision}.",
    )


# ---------------------------------------------------------------------------
# Controlled EDIT facts
# ---------------------------------------------------------------------------


def validate_human_facts(
    state: PipelineReliabilityState,
    submitted_facts: dict[str, Any] | None,
    *,
    reason: str = "",
    evidence: str = "",
) -> HumanFactValidationResult:
    """Accept or reject a human fact bundle. Pure: no State mutation."""
    facts = dict(submitted_facts or {})
    if not facts:
        return HumanFactValidationResult(
            accepted=False,
            reason="EDIT rejected: submitted_facts must be a non-empty dict.",
        )
    note = (reason or "").strip()
    proof = (evidence or "").strip()
    if not note or not proof:
        return HumanFactValidationResult(
            accepted=False,
            reason="EDIT rejected: reason and evidence are required.",
        )

    for key, value in facts.items():
        if key in HUMAN_FORBIDDEN_FACT_KEYS:
            return HumanFactValidationResult(
                accepted=False,
                reason=(
                    f"EDIT rejected: {key!r} is not a human-writable field "
                    "(no arbitrary State mutation, no action authority)."
                ),
            )
        if key not in HUMAN_ALLOWED_FACT_KEYS:
            return HumanFactValidationResult(
                accepted=False,
                reason=(
                    f"EDIT rejected: {key!r} is not in the V1 human-fact allowlist "
                    f"({', '.join(sorted(HUMAN_ALLOWED_FACT_KEYS))})."
                ),
            )
        type_error = _fact_type_error(key, value)
        if type_error:
            return HumanFactValidationResult(accepted=False, reason=type_error)

        conflict = _fact_conflict(state, key, value)
        if conflict:
            return HumanFactValidationResult(accepted=False, reason=conflict)

    return HumanFactValidationResult(
        accepted=True,
        reason="human facts accepted for Apply.",
        facts=facts,
    )


def _fact_type_error(key: str, value: object) -> str:
    if key == "rows_written":
        if isinstance(value, bool) or not isinstance(value, int):
            return "EDIT rejected: rows_written must be an int."
        if value < 0:
            return "EDIT rejected: rows_written must be >= 0."
        return ""
    if key == "warehouse_status":
        if not isinstance(value, str) or value not in _BIGQUERY_STATUS_VALUES:
            allowed = ", ".join(sorted(s for s in _BIGQUERY_STATUS_VALUES if s))
            return (
                "EDIT rejected: warehouse_status must be one of "
                f"{allowed} (or empty)."
            )
        return ""
    if key == "file_present":
        if not isinstance(value, bool):
            return "EDIT rejected: file_present must be a bool."
        return ""
    return f"EDIT rejected: {key!r} has no V1 type rule."


def _fact_conflict(
    state: PipelineReliabilityState,
    key: str,
    value: object,
) -> str:
    """Obvious conflicts: a human cannot erase a known warehouse write."""
    if key == "rows_written":
        current = state.rows_written
        if current is not None and current > 0 and value == 0:
            return (
                "EDIT rejected: cannot set rows_written=0 while State already "
                f"has rows_written={current} (human facts must not bypass Guard)."
            )
    return ""


def apply_human_facts(
    state: PipelineReliabilityState,
    facts: dict[str, Any],
) -> PipelineReliabilityState:
    """Write allowlisted human facts through explicit field assignment.

    Re-checks allowlist, types, and conflicts. Does not setattr arbitrary
    keys. Does not choose an Action.
    """
    for key, value in facts.items():
        if key not in HUMAN_ALLOWED_FACT_KEYS:
            raise ValueError(
                f"refusing to apply non-allowlisted human fact {key!r}."
            )
        type_error = _fact_type_error(key, value)
        if type_error:
            raise ValueError(type_error)
        conflict = _fact_conflict(state, key, value)
        if conflict:
            raise ValueError(conflict)
    if "rows_written" in facts:
        state.rows_written = int(facts["rows_written"])
    if "warehouse_status" in facts:
        state.warehouse_status = str(facts["warehouse_status"])
    if "file_present" in facts:
        state.file_present = bool(facts["file_present"])
    if "rows_written" in facts or "warehouse_status" in facts:
        classify_write_risk(state)
    return state


# ---------------------------------------------------------------------------
# Persistence — lab JSON, atomic replace
# ---------------------------------------------------------------------------


def _record_path(hitl_dir: str | Path, request_id: str) -> Path:
    return Path(hitl_dir) / f"{request_id}.json"


def _request_from_payload(raw: dict[str, Any]) -> HumanRequest:
    """Rebuild HumanRequest from JSON. Unknown keys and missing pack are safe."""
    payload = dict(raw)
    pack_raw = payload.pop("context_pack", None)
    analysis_raw = payload.pop("incident_analysis", None)
    known = {item.name for item in fields(HumanRequest)}
    cleaned = {key: value for key, value in payload.items() if key in known}
    cleaned["context_pack"] = (
        context_pack_from_dict(pack_raw) if isinstance(pack_raw, dict) else None
    )
    cleaned["incident_analysis"] = (
        incident_analysis_from_dict(analysis_raw)
        if isinstance(analysis_raw, dict)
        else None
    )
    return HumanRequest(**cleaned)


def save_hitl_record(record: HitlRecord, hitl_dir: str | Path) -> Path:
    """Serialize one HITL record and replace the file atomically."""
    target = _record_path(hitl_dir, record.request.request_id)
    target.parent.mkdir(parents=True, exist_ok=True)
    payload = json.dumps(
        {
            "request": asdict(record.request),
            "decisions": [asdict(item) for item in record.decisions],
            "audit": [asdict(item) for item in record.audit],
        },
        indent=2,
        sort_keys=True,
    )
    tmp_path = target.with_name(target.name + ".tmp")
    tmp_path.write_text(payload, encoding="utf-8")
    tmp_path.replace(target)
    return target


def load_hitl_record(hitl_dir: str | Path, request_id: str) -> HitlRecord:
    payload = json.loads(_record_path(hitl_dir, request_id).read_text(encoding="utf-8"))
    return HitlRecord(
        request=_request_from_payload(payload["request"]),
        decisions=[HumanDecision(**item) for item in payload.get("decisions", [])],
        audit=[HitlAuditEvent(**item) for item in payload.get("audit", [])],
    )


def list_hitl_records(hitl_dir: str | Path) -> list[HitlRecord]:
    root = Path(hitl_dir)
    if not root.is_dir():
        return []
    records: list[HitlRecord] = []
    for path in sorted(root.glob("*.json")):
        if path.name.endswith(".tmp"):
            continue
        payload = json.loads(path.read_text(encoding="utf-8"))
        records.append(
            HitlRecord(
                request=_request_from_payload(payload["request"]),
                decisions=[
                    HumanDecision(**item) for item in payload.get("decisions", [])
                ],
                audit=[HitlAuditEvent(**item) for item in payload.get("audit", [])],
            )
        )
    return records


def list_pending_requests(hitl_dir: str | Path) -> list[HumanRequest]:
    return [
        record.request
        for record in list_hitl_records(hitl_dir)
        if record.request.status == PENDING
    ]


# ---------------------------------------------------------------------------
# Open request on ASK_HUMAN
# ---------------------------------------------------------------------------


def format_human_ticket(request: HumanRequest) -> str:
    """Readable on-call report. Advisory analysis is not an executable instruction."""
    lines = [
        f"HITL ticket: {request.request_id}",
        f"Pipeline: {request.pipeline}",
        f"Task: {request.task_id}",
        f"Run: {request.run_id}",
        f"Why paused: {request.reason}",
        f"Suggested action (non-binding): {request.suggested_action}",
        f"Allowed decisions: {', '.join(request.allowed_decisions)}",
        f"Orchestrator: {request.orchestrator_status}",
        f"Warehouse: {request.warehouse_status}",
        f"Rows written: {request.rows_written}",
    ]
    analysis = request.incident_analysis
    if analysis is None:
        lines.append("Incident analysis: (none — advisor missing or rejected)")
        return "\n".join(lines)
    lines.extend(
        [
            "",
            "What happened?",
            analysis.summary,
            "",
            "Why does the Advisor think that?",
            analysis.probable_root_cause,
            "",
            "What evidence supports it?",
            *[f"- {item}" for item in analysis.evidence],
            "",
            f"How confident is it? {analysis.confidence:.2f}",
            "",
            "What is still unknown?",
            *([f"- {item}" for item in analysis.unknowns] or ["- (none listed)"]),
            "",
            "What should I inspect next?",
            *(
                [f"- {item}" for item in analysis.suggested_next_checks]
                or ["- (none listed)"]
            ),
            "",
            "What repair might make sense?",
            analysis.candidate_repair or "(none)",
            "",
            "How risky is that repair?",
            analysis.repair_risk or "(none)",
            "",
            "What decision/question needs a human?",
            analysis.human_question or "(none)",
            "",
            "Authority note: this report is advisory. It is not RETRY, "
            "not APPLY_APPROVED_REPAIR, and not authorization.",
        ]
    )
    return "\n".join(lines)


def describe_escalation(state: PipelineReliabilityState) -> str:
    """Why the Agent needs a human — prefers State.escalation_reason."""
    if (state.escalation_reason or "").strip():
        return state.escalation_reason.strip()
    if state.retry_side_effect == "UNKNOWN":
        return (
            "Previous RETRY side effect is UNKNOWN; "
            "reconcile with orchestrator before another mutation."
        )
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


def suggest_hitl_action(state: PipelineReliabilityState) -> str:
    """Non-binding suggestion. Uses existing suggested_action, else Guard."""
    if state.suggested_action:
        return state.suggested_action
    if guard(state, RETRY).allowed:
        return RETRY
    return STOP_SAFE


def open_human_request(
    state: PipelineReliabilityState,
    *,
    agent_run_id: str,
    hitl_dir: str | Path,
    checkpoint_path: str | Path | None = None,
    incident_analysis: IncidentAnalysis | None = None,
) -> HumanRequest:
    """Persist a HumanRequest for an ASK_HUMAN pause. Idempotent per PENDING id."""
    if state.human_request_id:
        existing_path = _record_path(hitl_dir, state.human_request_id)
        if existing_path.is_file():
            existing = load_hitl_record(hitl_dir, state.human_request_id)
            if existing.request.status == PENDING:
                return existing.request

    reason = describe_escalation(state)
    request = HumanRequest(
        request_id=str(uuid.uuid4()),
        agent_run_id=agent_run_id,
        pipeline=state.pipeline,
        task_id=state.task_id,
        run_id=state.run_id,
        reason=reason,
        evidence=list(state.evidence),
        suggested_action=suggest_hitl_action(state),
        allowed_decisions=list(ALLOWED_DECISIONS),
        status=PENDING,
        created_at=_utc_now(),
        checkpoint_path=str(checkpoint_path) if checkpoint_path else "",
        error=state.error,
        orchestrator_status=state.orchestrator_status,
        warehouse_status=state.warehouse_status,
        rows_written=state.rows_written,
        context_pack=build_context_pack(state, escalation_reason=reason),
        incident_analysis=incident_analysis,
    )
    save_hitl_record(HitlRecord(request=request), hitl_dir)
    state.human_request_id = request.request_id
    if not state.escalation_reason:
        state.escalation_reason = request.reason
    if state.suggested_action is None:
        state.suggested_action = request.suggested_action
    note = f"hitl_request: {request.request_id}"
    if note not in state.evidence:
        state.evidence.append(note)
    return request


# ---------------------------------------------------------------------------
# Submit decision — persist first, then apply to State. No tools.
# ---------------------------------------------------------------------------


def submit_human_decision(
    hitl_dir: str | Path,
    request_id: str,
    *,
    actor: str,
    role: str,
    decision: str,
    approved_action: str | None = None,
    reason: str = "",
    submitted_facts: dict[str, Any] | None = None,
    evidence: str = "",
    state: PipelineReliabilityState | None = None,
) -> HumanSubmissionResult:
    """Identity → authorization → persist HumanDecision → apply to State.

    Does not call tools. Does not resume the Agent. Duplicate submissions
    of a already-decided request are no-ops (no second Apply, no Execute).
    """
    record = load_hitl_record(hitl_dir, request_id)
    request = record.request

    identity = check_identity(actor)
    if not identity.ok:
        _append_audit(
            record,
            hitl_dir,
            actor=actor,
            role=role,
            attempted=decision,
            identity_ok=False,
            authorization_allowed=False,
            duplicate=False,
            reason=identity.reason,
        )
        return HumanSubmissionResult(
            accepted=False,
            duplicate=False,
            identity_ok=False,
            authorization_allowed=False,
            reason=identity.reason,
            request=request,
            decision=None,
            state=state,
        )

    action_for_authz = approved_action or request.suggested_action
    authz = authorize(role, decision, action_for_authz)
    if not authz.allowed:
        _append_audit(
            record,
            hitl_dir,
            actor=actor.strip(),
            role=role,
            attempted=decision,
            identity_ok=True,
            authorization_allowed=False,
            duplicate=False,
            reason=authz.reason,
        )
        return HumanSubmissionResult(
            accepted=False,
            duplicate=False,
            identity_ok=True,
            authorization_allowed=False,
            reason=authz.reason,
            request=request,
            decision=None,
            state=state,
        )

    if request.status != PENDING:
        existing = record.decisions[-1] if record.decisions else None
        _append_audit(
            record,
            hitl_dir,
            actor=actor.strip(),
            role=role,
            attempted=decision,
            identity_ok=True,
            authorization_allowed=True,
            duplicate=True,
            reason=(
                f"duplicate ignored: request {request_id} is {request.status}; "
                "HumanDecision already persisted."
            ),
        )
        return HumanSubmissionResult(
            accepted=False,
            duplicate=True,
            identity_ok=True,
            authorization_allowed=True,
            reason=(
                f"duplicate ignored: request {request_id} is {request.status}; "
                "no second Apply and no tool execution."
            ),
            request=request,
            decision=existing,
            state=state,
        )

    if decision not in request.allowed_decisions:
        return HumanSubmissionResult(
            accepted=False,
            duplicate=False,
            identity_ok=True,
            authorization_allowed=True,
            reason=f"decision {decision!r} is not in allowed_decisions.",
            request=request,
            decision=None,
            state=state,
        )

    working = state
    if working is None and request.checkpoint_path:
        working = load_checkpoint(request.checkpoint_path)

    if decision == EDIT:
        if working is None:
            return HumanSubmissionResult(
                accepted=False,
                duplicate=False,
                identity_ok=True,
                authorization_allowed=True,
                reason="EDIT rejected: State or checkpoint is required to validate facts.",
                request=request,
                decision=None,
                state=None,
            )
        fact_check = validate_human_facts(
            working,
            submitted_facts,
            reason=reason,
            evidence=evidence or reason,
        )
        if not fact_check.accepted:
            _append_audit(
                record,
                hitl_dir,
                actor=actor.strip(),
                role=role,
                attempted=EDIT,
                identity_ok=True,
                authorization_allowed=True,
                duplicate=False,
                reason=fact_check.reason,
            )
            return HumanSubmissionResult(
                accepted=False,
                duplicate=False,
                identity_ok=True,
                authorization_allowed=True,
                reason=fact_check.reason,
                request=request,
                decision=None,
                state=working,
            )
        facts_to_apply = fact_check.facts
    else:
        facts_to_apply = {}

    if decision == APPROVE:
        action = approved_action or request.suggested_action
        if not action:
            return HumanSubmissionResult(
                accepted=False,
                duplicate=False,
                identity_ok=True,
                authorization_allowed=True,
                reason="APPROVE rejected: no approved_action or suggested_action.",
                request=request,
                decision=None,
                state=working,
            )
    else:
        action = approved_action

    human_decision = HumanDecision(
        decision_id=str(uuid.uuid4()),
        request_id=request_id,
        actor=actor.strip(),
        role=role,
        decision=decision,
        approved_action=action if decision == APPROVE else None,
        reason=(reason or "").strip(),
        submitted_facts=facts_to_apply,
        decided_at=_utc_now(),
        authorization_allowed=True,
        authorization_reason=authz.reason,
    )

    # Persist decision BEFORE applying to State / resuming the Agent.
    request.status = DECIDED
    record.decisions.append(human_decision)
    record.audit.append(
        HitlAuditEvent(
            at=human_decision.decided_at,
            actor=human_decision.actor,
            role=role,
            attempted_decision=decision,
            identity_ok=True,
            authorization_allowed=True,
            duplicate=False,
            reason=authz.reason,
        )
    )
    save_hitl_record(record, hitl_dir)

    if working is not None:
        working = apply_persisted_decision(working, human_decision)
        request.status = APPLIED
        save_hitl_record(record, hitl_dir)
        if request.checkpoint_path:
            save_checkpoint(working, request.checkpoint_path)

    return HumanSubmissionResult(
        accepted=True,
        duplicate=False,
        identity_ok=True,
        authorization_allowed=True,
        reason=(
            f"HumanDecision {human_decision.decision_id} persisted "
            f"({decision}); Agent not resumed by this call."
        ),
        request=request,
        decision=human_decision,
        state=working,
    )


def apply_persisted_decision(
    state: PipelineReliabilityState,
    decision: HumanDecision,
) -> PipelineReliabilityState:
    """Replay a persisted decision onto State. No tools. Idempotent enough for resume.

    APPROVE uses existing approve(). REJECT uses reject() (does not terminate).
    EDIT writes allowlisted facts then clears the pause so Decide can run.
    """
    if decision.decision == APPROVE:
        if state.awaiting_human:
            approve(state, decision.approved_action or RETRY)
        state.last_human_decision = APPROVE
        _record_decision_evidence(state, decision)
        return state

    if decision.decision == REJECT:
        if state.awaiting_human:
            reject(state, reason=decision.reason)
        else:
            state.last_human_decision = REJECT
            state.approved_action = None
        _record_decision_evidence(state, decision)
        return state

    if decision.decision == EDIT:
        apply_human_facts(state, decision.submitted_facts)
        if state.awaiting_human:
            state.awaiting_human = False
            state.outcome = None
            state.approved_action = None
        state.last_human_decision = EDIT
        note = (
            f"human_edited: applied {sorted(decision.submitted_facts)} "
            f"({decision.reason})"
        )
        state.observation = note
        if note not in state.evidence:
            state.evidence.append(note)
        _record_decision_evidence(state, decision)
        return state

    raise ValueError(f"unknown HITL decision {decision.decision!r}")


def ensure_decision_applied(
    state: PipelineReliabilityState,
    record: HitlRecord,
) -> PipelineReliabilityState:
    """Crash recovery: decision persisted, State not yet updated → apply, do not Execute.

    Only runs while the record is DECIDED. APPLIED means State already received
    the decision; resume then continues the Agent loop, it does not replay Apply.
    """
    if record.request.status != DECIDED or not record.decisions:
        return state
    decision = record.decisions[-1]
    if not decision.authorization_allowed:
        return state
    apply_persisted_decision(state, decision)
    record.request.status = APPLIED
    return state


def resume_after_hitl(
    hitl_dir: str | Path,
    request_id: str,
    *,
    adapter: Any = None,
    intelligence: Any = None,
    advisor: Any = None,
) -> Any:
    """Load persisted decision, apply to State if needed, then run_agent.

    Lazy-imports runner to avoid an import cycle. Guard still runs inside
    run_agent. This is Resume, not Replay of a tool side effect.
    """
    from pipeline_reliability.runner import run_agent

    record = load_hitl_record(hitl_dir, request_id)
    if record.request.status == PENDING:
        raise ValueError(
            f"cannot resume request {request_id}: still PENDING (no HumanDecision)."
        )
    if not record.request.checkpoint_path:
        raise ValueError(
            f"cannot resume request {request_id}: no checkpoint_path on HumanRequest."
        )
    state = load_checkpoint(record.request.checkpoint_path)
    ensure_decision_applied(state, record)
    save_hitl_record(record, hitl_dir)
    if record.request.checkpoint_path:
        save_checkpoint(state, record.request.checkpoint_path)
    return run_agent(
        state,
        adapter=adapter,
        checkpoint_path=record.request.checkpoint_path,
        hitl_dir=hitl_dir,
        intelligence=intelligence,
        advisor=advisor,
        agent_run_id=record.request.agent_run_id,
    )


def _append_audit(
    record: HitlRecord,
    hitl_dir: str | Path,
    *,
    actor: str,
    role: str,
    attempted: str,
    identity_ok: bool,
    authorization_allowed: bool,
    duplicate: bool,
    reason: str,
) -> None:
    record.audit.append(
        HitlAuditEvent(
            at=_utc_now(),
            actor=actor,
            role=role,
            attempted_decision=attempted,
            identity_ok=identity_ok,
            authorization_allowed=authorization_allowed,
            duplicate=duplicate,
            reason=reason,
        )
    )
    save_hitl_record(record, hitl_dir)


def _record_decision_evidence(
    state: PipelineReliabilityState,
    decision: HumanDecision,
) -> None:
    note = (
        f"hitl_decision: {decision.decision} "
        f"decision_id={decision.decision_id} "
        f"actor={decision.actor} role={decision.role} "
        f"authz={'allowed' if decision.authorization_allowed else 'blocked'}"
    )
    if note not in state.evidence:
        state.evidence.append(note)
    state.human_request_id = decision.request_id
