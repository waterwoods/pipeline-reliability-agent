"""Pipeline Reliability Agent — Observability (RunTrace + RunMetrics + Export).

Records the step-by-step execution path of one agent run and derives simple
per-run metrics from the trace. Also provides a pure mapping layer that
translates internal domain objects into a neutral OpenTelemetry-style
representation for external backends (no network I/O here).

Intelligence lifecycle events live on the same RunTrace
(``intelligence_events``). They are observation only.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import TYPE_CHECKING, Any

from pipeline_reliability.decide import APPLY_APPROVED_REPAIR, ASK_HUMAN, RETRY
from pipeline_reliability.facts import FactProposal
from pipeline_reliability.incident_analysis import (
    IncidentAnalysis,
    incident_analysis_from_dict,
)

if TYPE_CHECKING:
    from pipeline_reliability.runner import AgentRunResult

# V1 Intelligence lifecycle events — observation only. Not Actions.
LLM_PROPOSAL = "LLM_PROPOSAL"
VALIDATION_REJECTED = "VALIDATION_REJECTED"
LLM_REPAIR = "LLM_REPAIR"
VALIDATION_ACCEPTED = "VALIDATION_ACCEPTED"
INTELLIGENCE_EXHAUSTED = "INTELLIGENCE_EXHAUSTED"
INCIDENT_ANALYSIS = "INCIDENT_ANALYSIS"
ANALYSIS_REJECTED = "ANALYSIS_REJECTED"
LLM_ANALYZE_INCIDENT = "LLM_ANALYZE_INCIDENT"


@dataclass
class TraceStateAfter:
    """Compact State crumbs. Not a full State snapshot.

    Used for both the pre-step view (``TraceStep.state_before``) and the
    post-Apply view (``TraceStep.state_after``). ``None`` means this step
    never recorded crumbs (legacy JSON).
    """

    retry_side_effect: str = ""
    retries: int | None = None
    attempt_number: int | None = None
    latest_repair_id: str = ""
    orchestrator_status: str = ""
    warehouse_status: str = ""
    arrival_status: str = ""
    backfill_side_effect: str = ""
    awaiting_human: bool = False


@dataclass
class TraceObservationFacts:
    """Compact structured observation crumbs. Not raw vendor JSON.

    Distinguishes "CHECK saw attempt_number=1" from the human-readable
    ``TraceStep.observation`` string. ``observation_type`` is the execute()
    result class name (e.g. ``OrchestratorRunResult``), or empty for a
    plain string observation.
    """

    observation_type: str = ""
    side_effect: str = ""
    attempt_number: int | None = None
    latest_repair_id: str = ""
    status: str = ""
    result: str = ""
    asset: str = ""
    partition: str = ""
    reconcile_result: str = ""
    validation_result: str = ""


@dataclass
class TraceStep:
    step_number: int
    action: str
    guard_allowed: bool
    guard_reason: str
    observation: str
    outcome: str | None
    duration_ms: float | None = None
    timestamp: str | None = None
    action_id: str | None = None
    state_before: TraceStateAfter | None = None
    state_after: TraceStateAfter | None = None
    observation_facts: TraceObservationFacts | None = None
    apply_reason: str = ""
    # Safe extra span tags (llm.provider, llm.model, ...). Never prompts or secrets.
    attributes: dict[str, Any] = field(default_factory=dict)


@dataclass
class IntelligenceTraceEvent:
    """One Intelligence lifecycle observation. Not an Action. Does not write State."""

    event_type: str
    proposal: FactProposal | None = None
    accepted: bool | None = None
    reason: str = ""
    analysis: IncidentAnalysis | None = None


@dataclass
class RunTrace:
    steps: list[TraceStep] = field(default_factory=list)
    final_outcome: str | None = None
    intelligence_events: list[IntelligenceTraceEvent] = field(default_factory=list)
    # Scratch pad for the current Apply turn. Runner copies it onto TraceStep.
    # Not a durable incident field; trace_to_dict omits it.
    last_apply_reason: str = ""


@dataclass
class RunMetrics:
    total_steps: int
    total_duration_ms: float
    retry_count: int
    guard_block_count: int
    human_escalation_count: int


def calculate_metrics(trace: RunTrace) -> RunMetrics:
    """Derive per-run metrics purely from a completed or in-progress trace."""
    total_duration_ms = sum(
        step.duration_ms for step in trace.steps if step.duration_ms is not None
    )
    retry_count = sum(1 for step in trace.steps if step.action == RETRY)
    guard_block_count = sum(1 for step in trace.steps if not step.guard_allowed)
    human_escalation_count = sum(
        1 for step in trace.steps if step.outcome == ASK_HUMAN
    )
    return RunMetrics(
        total_steps=len(trace.steps),
        total_duration_ms=total_duration_ms,
        retry_count=retry_count,
        guard_block_count=guard_block_count,
        human_escalation_count=human_escalation_count,
    )


# ---------------------------------------------------------------------------
# Observability Exporter V1 — pure mapping, no side effects
# ---------------------------------------------------------------------------
# Internal RunTrace / TraceStep remain the source of truth. ExportedTrace /
# ExportedSpan are translation-only views for OpenTelemetry-style consumers.


@dataclass(frozen=True)
class ExportedSpan:
    """One TraceStep mapped to a span-like neutral record."""

    span_id: str
    operation_name: str
    duration_ms: float | None
    attributes: dict[str, Any]
    status: str | None


@dataclass(frozen=True)
class ExportedTrace:
    """One AgentRunResult mapped to a trace-like neutral record."""

    trace_id: str
    pipeline: str
    final_outcome: str | None
    attributes: dict[str, Any]
    spans: tuple[ExportedSpan, ...]


def export_trace(result: AgentRunResult) -> ExportedTrace:
    """Map AgentRunResult -> one trace-like object with ordered span-like steps.

    Pure function: does not mutate ``result``, State, or Trace.
    """
    state = result.state
    trace = result.trace
    metrics = result.metrics
    orchestrator_run_id = (state.run_id or "").strip() or None

    attributes: dict[str, Any] = {
        "pipeline": state.pipeline,
        "scenario": state.scenario or None,
        "error": state.error or None,
        "orchestrator_run_id": orchestrator_run_id,
        "orchestrator_status": state.orchestrator_status or None,
        "warehouse_status": state.warehouse_status or None,
        "rows_written": state.rows_written,
        "partial_write": state.partial_write,
        "retry_may_duplicate": state.retry_may_duplicate,
        "drift_type": state.drift_type or None,
        "expected_schema": state.expected_schema,
        "observed_schema": state.observed_schema,
        "changed_fields": state.changed_fields or None,
        "repair_recipe_found": state.repair_recipe_found,
        "repair_recipe_id": state.repair_recipe_id or None,
        "repair_recipe_approved": state.repair_recipe_approved,
        "repair_applied": state.repair_applied,
        "repair_validation_passed": state.repair_validation_passed,
        "mutation_count": sum(
            1
            for step in trace.steps
            if step.action in {RETRY, APPLY_APPROVED_REPAIR}
        ),
        "downstream_impact": state.downstream_impact or None,
        "total_steps": metrics.total_steps,
        "total_duration_ms": metrics.total_duration_ms,
        "retry_count": metrics.retry_count,
        "guard_block_count": metrics.guard_block_count,
        "human_escalation_count": metrics.human_escalation_count,
        "human_escalation": metrics.human_escalation_count > 0,
    }

    spans = tuple(_export_span(result.agent_run_id, step) for step in trace.steps)

    return ExportedTrace(
        trace_id=result.agent_run_id,
        pipeline=state.pipeline,
        final_outcome=trace.final_outcome,
        attributes=attributes,
        spans=spans,
    )


def _put_present(attributes: dict[str, Any], key: str, value: Any) -> None:
    """Add a span tag only when this step actually has the fact."""
    if value is None or value == "":
        return
    attributes[key] = value


def _emit_state_transition(
    attributes: dict[str, Any],
    before: TraceStateAfter | None,
    after: TraceStateAfter | None,
) -> None:
    """Emit before/after pairs. Omit a field when both sides are blank."""
    for name in (
        "orchestrator_status",
        "warehouse_status",
        "arrival_status",
        "backfill_side_effect",
        "retry_side_effect",
    ):
        before_text = "" if before is None else str(getattr(before, name, "") or "")
        after_text = "" if after is None else str(getattr(after, name, "") or "")
        if not before_text and not after_text:
            continue
        if before_text:
            attributes[f"state.before.{name}"] = before_text
        # Keep a blank after-value when this step cleared a fact. That is why
        # the next action re-observes instead of continuing from the old one.
        if after_text or before_text:
            attributes[f"state.after.{name}"] = after_text
    before_waiting = bool(before and before.awaiting_human)
    after_waiting = bool(after and after.awaiting_human)
    if before_waiting:
        attributes["state.before.awaiting_human"] = True
    if after_waiting:
        attributes["state.after.awaiting_human"] = True


def _arrival_status(state: object) -> str:
    """One compact status, or asset:status when several items are open."""
    items = getattr(state, "arrival_items", None) or []
    if not items:
        return ""
    multiple = len(items) > 1
    labels: list[str] = []
    for item in items:
        status = str(getattr(item, "status", "") or "")
        if not status:
            continue
        if multiple:
            asset = str(getattr(item, "asset", "") or "")
            labels.append(f"{asset}:{status}" if asset else status)
        else:
            labels.append(status)
    return ",".join(labels)


def _export_span(trace_id: str, step: TraceStep) -> ExportedSpan:
    """Map one TraceStep -> span-like record."""
    return ExportedSpan(
        span_id=f"{trace_id}:{step.step_number}",
        operation_name=step.action,
        duration_ms=step.duration_ms,
        attributes=_span_attributes(step),
        status=step.outcome,
    )


def _span_attributes(step: TraceStep) -> dict[str, Any]:
    """Copy compact TraceStep / Section 5 crumbs. Skip missing nested objects."""
    attributes: dict[str, Any] = {
        "action": step.action,
        "step_number": step.step_number,
        "guard_allowed": step.guard_allowed,
        "guard_reason": step.guard_reason,
        "observation": step.observation,
        "outcome": step.outcome,
        "apply_reason": step.apply_reason,
        "action_id": step.action_id,
        "human_escalation": step.outcome == ASK_HUMAN,
        "timestamp": step.timestamp,
    }
    attributes["guard.allowed"] = step.guard_allowed
    attributes["guard.reason"] = step.guard_reason
    facts = step.observation_facts
    if facts is not None:
        attributes["observation_facts.observation_type"] = facts.observation_type
        attributes["observation_facts.side_effect"] = facts.side_effect
        attributes["observation_facts.attempt_number"] = facts.attempt_number
        attributes["observation_facts.latest_repair_id"] = facts.latest_repair_id
        _put_present(attributes, "observation.type", facts.observation_type)
        _put_present(attributes, "observation.status", facts.status)
        _put_present(attributes, "observation.result", facts.result)
        _put_present(attributes, "reconcile_result", facts.reconcile_result)
        _put_present(attributes, "validation_result", facts.validation_result)
        _put_present(attributes, "asset", facts.asset)
        _put_present(attributes, "partition", facts.partition)
    _emit_state_transition(attributes, step.state_before, step.state_after)
    after = step.state_after
    if after is not None:
        attributes["state_after.retry_side_effect"] = after.retry_side_effect
        attributes["state_after.retries"] = after.retries
        attributes["state_after.attempt_number"] = after.attempt_number
        attributes["state_after.latest_repair_id"] = after.latest_repair_id
        attributes["state_after.orchestrator_status"] = after.orchestrator_status
        if after.awaiting_human:
            attributes["awaiting_human"] = True
    extra = step.attributes or {}
    for key, value in extra.items():
        if value is None or key in attributes:
            continue
        if isinstance(value, (str, bool, int, float)):
            attributes[str(key)] = value
    return attributes


def trace_to_dict(trace: RunTrace) -> dict[str, Any]:
    """Serialize RunTrace to a JSON-ready dict. Inverse of ``trace_from_dict``."""
    payload = asdict(trace)
    payload.pop("last_apply_reason", None)
    return payload


def trace_from_dict(payload: dict[str, Any] | None) -> RunTrace:
    """Reconstruct RunTrace from checkpoint JSON. Unknown keys are ignored."""
    if not payload:
        return RunTrace()
    steps = [_step_from_dict(item) for item in payload.get("steps") or []]
    events = [
        _intelligence_event_from_dict(item)
        for item in payload.get("intelligence_events") or []
    ]
    return RunTrace(
        steps=steps,
        final_outcome=payload.get("final_outcome"),
        intelligence_events=events,
    )


def _optional_int(value: Any) -> int | None:
    if value is None or value == "":
        return None
    return int(value)


def observation_facts_from_result(observation: object) -> TraceObservationFacts:
    """Copy compact identity crumbs from an execute() result.

    Reads only attributes that exist on the real observation. Does not invent
    identity, does not copy vendor JSON, detail text, or SQL. Plain strings
    keep a short result and no structured type.
    """
    if isinstance(observation, str):
        text = observation.strip()
        return TraceObservationFacts(result=text if 0 < len(text) <= 80 else "")
    type_name = type(observation).__name__
    status = getattr(observation, "status", None)
    status_text = status if isinstance(status, str) else ""
    side_effect = str(getattr(observation, "side_effect", None) or "")
    passed = getattr(observation, "passed", None)
    accepted = getattr(observation, "accepted", None)
    result = ""
    reconcile_result = ""
    validation_result = ""
    if type_name == "BackfillReconcileResult" and status_text:
        reconcile_result = status_text
        result = status_text
    elif type_name == "PartitionValidationResult" and isinstance(passed, bool):
        validation_result = "passed" if passed else "failed"
        result = validation_result
    elif type_name == "RetryResult":
        if side_effect == "UNKNOWN":
            result = "UNKNOWN"
        elif accepted is True:
            result = "accepted"
        elif accepted is False:
            result = "rejected"
    elif status_text:
        result = status_text
    return TraceObservationFacts(
        observation_type=type_name,
        side_effect=side_effect,
        attempt_number=_optional_int(getattr(observation, "attempt_number", None)),
        latest_repair_id=str(getattr(observation, "latest_repair_id", None) or ""),
        status=status_text,
        result=result,
        asset=str(getattr(observation, "asset", None) or ""),
        partition=str(getattr(observation, "expected_partition", None) or ""),
        reconcile_result=reconcile_result,
        validation_result=validation_result,
    )


def state_after_from_state(state: object) -> TraceStateAfter:
    """Copy compact State crumbs. Not a full snapshot.

    The same crumb is taken before Execute and after Apply. It is not authoritative
    State and it does not include evidence, schemas, or payloads.
    """
    return TraceStateAfter(
        retry_side_effect=str(getattr(state, "retry_side_effect", None) or ""),
        retries=_optional_int(getattr(state, "retries", None)),
        attempt_number=_optional_int(getattr(state, "attempt_number", None)),
        latest_repair_id=str(getattr(state, "latest_repair_id", None) or ""),
        orchestrator_status=str(getattr(state, "orchestrator_status", None) or ""),
        warehouse_status=str(getattr(state, "warehouse_status", None) or ""),
        arrival_status=_arrival_status(state),
        backfill_side_effect=str(getattr(state, "backfill_side_effect", None) or ""),
        awaiting_human=bool(getattr(state, "awaiting_human", False)),
    )


def _state_after_from_dict(raw: Any) -> TraceStateAfter | None:
    if not isinstance(raw, dict):
        return None
    return TraceStateAfter(
        retry_side_effect=str(raw.get("retry_side_effect") or ""),
        retries=_optional_int(raw.get("retries")),
        attempt_number=_optional_int(raw.get("attempt_number")),
        latest_repair_id=str(raw.get("latest_repair_id") or ""),
        orchestrator_status=str(raw.get("orchestrator_status") or ""),
        warehouse_status=str(raw.get("warehouse_status") or ""),
        arrival_status=str(raw.get("arrival_status") or ""),
        backfill_side_effect=str(raw.get("backfill_side_effect") or ""),
        awaiting_human=bool(raw.get("awaiting_human") or False),
    )


def _observation_facts_from_dict(raw: Any) -> TraceObservationFacts | None:
    if not isinstance(raw, dict):
        return None
    return TraceObservationFacts(
        observation_type=str(raw.get("observation_type") or ""),
        side_effect=str(raw.get("side_effect") or ""),
        attempt_number=_optional_int(raw.get("attempt_number")),
        latest_repair_id=str(raw.get("latest_repair_id") or ""),
        status=str(raw.get("status") or ""),
        result=str(raw.get("result") or ""),
        asset=str(raw.get("asset") or ""),
        partition=str(raw.get("partition") or ""),
        reconcile_result=str(raw.get("reconcile_result") or ""),
        validation_result=str(raw.get("validation_result") or ""),
    )


def _step_from_dict(item: dict[str, Any]) -> TraceStep:
    return TraceStep(
        step_number=int(item["step_number"]),
        action=str(item["action"]),
        guard_allowed=bool(item["guard_allowed"]),
        guard_reason=str(item.get("guard_reason") or ""),
        observation=str(item.get("observation") or ""),
        outcome=item.get("outcome"),
        duration_ms=item.get("duration_ms"),
        timestamp=item.get("timestamp"),
        action_id=item.get("action_id"),
        state_before=_state_after_from_dict(item.get("state_before")),
        state_after=_state_after_from_dict(item.get("state_after")),
        observation_facts=_observation_facts_from_dict(item.get("observation_facts")),
        apply_reason=str(item.get("apply_reason") or ""),
        attributes=_safe_span_attributes(item.get("attributes")),
    )


def _safe_span_attributes(raw: Any) -> dict[str, Any]:
    if not isinstance(raw, dict):
        return {}
    cleaned: dict[str, Any] = {}
    for key, value in raw.items():
        if isinstance(value, (str, bool, int, float)):
            cleaned[str(key)] = value
    return cleaned


def _intelligence_event_from_dict(item: dict[str, Any]) -> IntelligenceTraceEvent:
    raw = item.get("proposal")
    proposal = None
    if isinstance(raw, dict) and "failure_type" in raw:
        proposal = FactProposal(
            failure_type=raw["failure_type"],
            facts=dict(raw.get("facts") or {}),
            confidence=float(raw.get("confidence") or 0.0),
            evidence=str(raw.get("evidence") or ""),
        )
    raw_analysis = item.get("analysis")
    analysis = (
        incident_analysis_from_dict(raw_analysis)
        if isinstance(raw_analysis, dict)
        else None
    )
    return IntelligenceTraceEvent(
        event_type=str(item.get("event_type") or ""),
        proposal=proposal,
        accepted=item.get("accepted"),
        reason=str(item.get("reason") or ""),
        analysis=analysis,
    )
