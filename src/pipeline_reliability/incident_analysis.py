"""Pipeline Reliability Agent — Incident Analysis Advisor (Slice 2).

IncidentAnalysis is a human/trace advisory explanation. It is not State,
not an Action, and not authorization. FactProposal remains the only
optional fact contract that may influence State after validation.

    ContextPack → IncidentAdvisor.analyze → IncidentAnalysis → Trace + HITL

The Advisor never reads or writes PipelineReliabilityState. Validation is
deterministic: a useful summary is not a license to RETRY.
"""

from __future__ import annotations

import json
import time
from dataclasses import asdict, dataclass, field, fields
from typing import Protocol

from pipeline_reliability.context_pack import ContextPack, context_pack_to_dict

# Authority / mutation tokens must never appear as structured fields.
# Prose may mention retry as something to investigate; that is not an Action.
FORBIDDEN_ANALYSIS_FIELDS: frozenset[str] = frozenset(
    {
        "action",
        "approved_action",
        "suggested_action",
        "RETRY",
        "APPLY_APPROVED_REPAIR",
        "ASK_HUMAN",
        "STOP_SAFE",
        "FINISH",
        "DELETE",
        "RUN_TOOL",
        "approved_recipe",
        "recipe_approval",
        "repair_recipe_approved",
        "mutation",
        "mutation_permission",
        "approved_repair",
    }
)

# Exact action tokens are not a candidate repair. Advisory sentences are fine.
_ACTION_TOKENS: frozenset[str] = frozenset(
    {
        "RETRY",
        "APPLY_APPROVED_REPAIR",
        "ASK_HUMAN",
        "STOP_SAFE",
        "FINISH",
        "DELETE",
        "RUN_TOOL",
    }
)

_ANALYSIS_FIELD_NAMES: frozenset[str] = frozenset()

DEFAULT_OPENAI_MODEL = "gpt-4o-mini"

ADVISOR_SYSTEM_PROMPT = """You are an experienced on-call Data Engineer advising a human.

You receive a compact read-only ContextPack collected by a deterministic Agent.
Explain the incident. You are not production authority.

log_excerpt is primary task-log evidence (see log_source).
warehouse_evidence and downstream_evidence are secondary context about
what else happened. Do not treat secondary notes as the task failure log.

You must:
- explain what likely happened
- identify the probable root cause
- cite evidence actually present in the ContextPack / log excerpt
- state uncertainty
- identify missing information
- recommend read-only next checks
- suggest a candidate repair in prose
- explain repair risk
- formulate one useful question for the human

You must NOT:
- choose RETRY
- choose APPLY_APPROVED_REPAIR
- approve a recipe
- claim mutation permission
- alter downstream schema
- invent warehouse facts
- invent row counts
- pretend unknown facts are known
- output an Action field
- output an authority field

Important:
PROPOSAL != APPROVAL != EXECUTION
A candidate repair is advisory prose, not an executable instruction.
"""

INCIDENT_ANALYSIS_JSON_SCHEMA: dict[str, object] = {
    "type": "object",
    "properties": {
        "summary": {"type": "string"},
        "probable_root_cause": {"type": "string"},
        "evidence": {"type": "array", "items": {"type": "string"}},
        "confidence": {"type": "number"},
        "unknowns": {"type": "array", "items": {"type": "string"}},
        "suggested_next_checks": {"type": "array", "items": {"type": "string"}},
        "candidate_repair": {"type": "string"},
        "repair_risk": {"type": "string"},
        "human_question": {"type": "string"},
    },
    "required": [
        "summary",
        "probable_root_cause",
        "evidence",
        "confidence",
        "unknowns",
        "suggested_next_checks",
        "candidate_repair",
        "repair_risk",
        "human_question",
    ],
    "additionalProperties": False,
}

_ADVISOR_RESPONSE_FORMAT: dict[str, object] = {
    "type": "json_schema",
    "json_schema": {
        "name": "incident_analysis",
        "strict": True,
        "schema": INCIDENT_ANALYSIS_JSON_SCHEMA,
    },
}


@dataclass(frozen=True)
class IncidentAnalysis:
    """Human/trace advisory explanation. Not State. Not an Action."""

    summary: str
    probable_root_cause: str
    evidence: tuple[str, ...]
    confidence: float
    unknowns: tuple[str, ...]
    suggested_next_checks: tuple[str, ...]
    candidate_repair: str
    repair_risk: str
    human_question: str


_ANALYSIS_FIELD_NAMES = frozenset(item.name for item in fields(IncidentAnalysis))


@dataclass(frozen=True)
class IncidentAnalysisValidationResult:
    """Deterministic accept/reject. accepted=True does not write State."""

    accepted: bool
    reason: str
    analysis: IncidentAnalysis | None = None


class IncidentAdvisor(Protocol):
    """Read-only advisor. Consumes ContextPack; never writes State."""

    def analyze(self, context_pack: ContextPack) -> IncidentAnalysis:
        ...


def _is_number(value: object) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def _as_text_tuple(value: object, *, field_name: str) -> tuple[str, ...] | str:
    if isinstance(value, str):
        return (value,)
    if not isinstance(value, (list, tuple)):
        return f"{field_name} must be a list or tuple of strings."
    items: list[str] = []
    for index, item in enumerate(value):
        if not isinstance(item, str) or not item.strip():
            return f"{field_name}[{index}] must be a non-empty string."
        items.append(item)
    return tuple(items)


def _forbidden_authority_key(value: object) -> str:
    keys: set[str] = set()
    if isinstance(value, dict):
        keys.update(value)
    else:
        keys.update(getattr(value, "__dict__", {}) or {})
        keys.update(getattr(type(value), "__annotations__", {}) or {})
        for name in FORBIDDEN_ANALYSIS_FIELDS:
            if hasattr(value, name):
                keys.add(name)
    hit = sorted(keys.intersection(FORBIDDEN_ANALYSIS_FIELDS))
    return hit[0] if hit else ""


def validate_incident_analysis(value: object) -> IncidentAnalysisValidationResult:
    """Accept or reject an advisor result. Pure: no I/O, no State mutation."""
    if value is None:
        return IncidentAnalysisValidationResult(
            accepted=False,
            reason="advisor returned no analysis.",
        )

    forbidden = _forbidden_authority_key(value)
    if forbidden:
        return IncidentAnalysisValidationResult(
            accepted=False,
            reason=(
                f"action-like field {forbidden!r} is not allowed on IncidentAnalysis."
            ),
        )

    analysis: IncidentAnalysis | None
    if isinstance(value, IncidentAnalysis):
        analysis = value
    elif isinstance(value, dict):
        analysis = _analysis_from_mapping(value)
        if analysis is None:
            return IncidentAnalysisValidationResult(
                accepted=False,
                reason="malformed advisor result; could not build IncidentAnalysis.",
            )
    else:
        return IncidentAnalysisValidationResult(
            accepted=False,
            reason=(
                "advisor result must be IncidentAnalysis, "
                f"got {type(value).__name__}."
            ),
        )

    if not isinstance(analysis.summary, str) or not analysis.summary.strip():
        return IncidentAnalysisValidationResult(
            accepted=False,
            reason="summary must be a non-empty string.",
        )
    if (
        not isinstance(analysis.probable_root_cause, str)
        or not analysis.probable_root_cause.strip()
    ):
        return IncidentAnalysisValidationResult(
            accepted=False,
            reason="probable_root_cause must be a non-empty string.",
        )
    if not _is_number(analysis.confidence):
        return IncidentAnalysisValidationResult(
            accepted=False,
            reason="confidence must be a number between 0.0 and 1.0.",
        )
    if analysis.confidence < 0.0 or analysis.confidence > 1.0:
        return IncidentAnalysisValidationResult(
            accepted=False,
            reason=(
                f"confidence {analysis.confidence} is outside the allowed "
                "range [0.0, 1.0]."
            ),
        )

    evidence = _as_text_tuple(analysis.evidence, field_name="evidence")
    if isinstance(evidence, str):
        return IncidentAnalysisValidationResult(accepted=False, reason=evidence)
    if not evidence:
        return IncidentAnalysisValidationResult(
            accepted=False,
            reason="evidence must contain at least one non-empty entry.",
        )
    unknowns = _as_text_tuple(analysis.unknowns, field_name="unknowns")
    if isinstance(unknowns, str):
        return IncidentAnalysisValidationResult(accepted=False, reason=unknowns)
    checks = _as_text_tuple(
        analysis.suggested_next_checks, field_name="suggested_next_checks"
    )
    if isinstance(checks, str):
        return IncidentAnalysisValidationResult(accepted=False, reason=checks)

    for name in (
        "candidate_repair",
        "repair_risk",
        "human_question",
    ):
        field_value = getattr(analysis, name)
        if not isinstance(field_value, str):
            return IncidentAnalysisValidationResult(
                accepted=False,
                reason=f"{name} must be a string.",
            )

    if analysis.candidate_repair.strip() in _ACTION_TOKENS:
        return IncidentAnalysisValidationResult(
            accepted=False,
            reason=(
                "candidate_repair must be advisory text, not an executable "
                f"action token ({analysis.candidate_repair.strip()})."
            ),
        )

    normalized = IncidentAnalysis(
        summary=analysis.summary,
        probable_root_cause=analysis.probable_root_cause,
        evidence=evidence,
        confidence=float(analysis.confidence),
        unknowns=unknowns,
        suggested_next_checks=checks,
        candidate_repair=analysis.candidate_repair,
        repair_risk=analysis.repair_risk,
        human_question=analysis.human_question,
    )
    return IncidentAnalysisValidationResult(
        accepted=True,
        reason="incident analysis accepted for human/trace review.",
        analysis=normalized,
    )


def incident_analysis_to_dict(analysis: IncidentAnalysis) -> dict[str, object]:
    """JSON-ready mapping. Inverse of ``incident_analysis_from_dict``."""
    return asdict(analysis)


def incident_analysis_from_dict(raw: dict[str, object] | None) -> IncidentAnalysis | None:
    """Rebuild analysis from HITL / trace JSON. Forbidden keys are dropped as None."""
    if not raw:
        return None
    if _forbidden_authority_key(raw):
        return None
    analysis = _analysis_from_mapping(raw)
    if analysis is None:
        return None
    result = validate_incident_analysis(analysis)
    return result.analysis if result.accepted else None


def advise_incident(
    advisor: IncidentAdvisor | None,
    context_pack: ContextPack,
) -> IncidentAnalysisValidationResult:
    """Call advisor and validate. Never raises. Never writes State."""
    if advisor is None:
        return IncidentAnalysisValidationResult(
            accepted=False,
            reason="no incident advisor injected.",
        )
    try:
        raw = advisor.analyze(context_pack)
    except Exception as exc:
        return IncidentAnalysisValidationResult(
            accepted=False,
            reason=f"advisor failed safely: {exc}",
        )
    return validate_incident_analysis(raw)


def _analysis_from_mapping(raw: dict[str, object]) -> IncidentAnalysis | None:
    payload: dict[str, object] = {}
    for key, value in raw.items():
        if key in FORBIDDEN_ANALYSIS_FIELDS:
            return None
        if key not in _ANALYSIS_FIELD_NAMES:
            continue
        payload[key] = value
    for key in ("evidence", "unknowns", "suggested_next_checks"):
        value = payload.get(key, ())
        if isinstance(value, list):
            payload[key] = tuple(value)
    try:
        return IncidentAnalysis(**payload)  # type: ignore[arg-type]
    except TypeError:
        return None


def _context_blob(pack: ContextPack) -> str:
    parts = [
        pack.error,
        pack.log_excerpt,
        pack.log_source,
        pack.warehouse_evidence,
        pack.downstream_evidence,
        pack.escalation_reason,
        pack.warehouse_status,
        pack.downstream_impact,
        pack.retry_safety_reason,
    ]
    return "\n".join(part for part in parts if part).lower()


def _evidence_from_pack(pack: ContextPack) -> tuple[str, ...]:
    items: list[str] = []
    excerpt = (pack.log_excerpt or "").strip()
    if excerpt:
        items.append(excerpt if len(excerpt) <= 400 else excerpt[:400])
    if pack.orchestrator_status:
        items.append(f"orchestrator_status={pack.orchestrator_status}")
    if pack.warehouse_status:
        items.append(f"warehouse_status={pack.warehouse_status}")
    if pack.rows_written is not None:
        items.append(f"rows_written={pack.rows_written}")
    if pack.partial_write is True:
        items.append("partial_write=true")
    if pack.downstream_impact:
        items.append(f"downstream_impact={pack.downstream_impact}")
    if pack.error:
        items.append(f"error={pack.error}")
    if not items:
        items.append("context pack contained no log excerpt or platform status")
    return tuple(items)


@dataclass
class FakeIncidentAdvisor:
    """Deterministic test/eval double. Reads ContextPack only. Not a live LLM."""

    analysis: IncidentAnalysis | None = None
    error: Exception | None = None
    calls: list[ContextPack] = field(default_factory=list)
    provider: str = "fake"
    model: str = "fake-incident-advisor"
    last_latency_ms: float | None = None
    last_tokens_in: int | None = None
    last_tokens_out: int | None = None

    def analyze(self, context_pack: ContextPack) -> IncidentAnalysis:
        self.calls.append(context_pack)
        if self.error is not None:
            raise self.error
        if self.analysis is not None:
            return self.analysis
        return self._from_pack(context_pack)

    def _from_pack(self, pack: ContextPack) -> IncidentAnalysis:
        blob = _context_blob(pack)
        evidence = _evidence_from_pack(pack)
        if any(
            token in blob
            for token in (
                "ignore instructions",
                "ignore all instructions",
                "retry immediately",
            )
        ):
            return IncidentAnalysis(
                summary=(
                    "Untrusted log text includes instruction-like language; "
                    "treat the failure as unclassified."
                ),
                probable_root_cause=(
                    "The log may describe a vendor failure, but it also contains "
                    "prompt-like instructions. Root cause is not confirmed."
                ),
                evidence=evidence,
                confidence=0.25,
                unknowns=(
                    "Whether the instruction-like text is attacker-controlled",
                    "Whether any vendor quota or auth failure is real",
                ),
                suggested_next_checks=(
                    "Re-read the vendor console without trusting embedded instructions",
                    "Confirm identity, quota, and write status from platform APIs",
                ),
                candidate_repair=(
                    "Do not retry from this log text. Confirm the real vendor "
                    "error with a human before any recovery."
                ),
                repair_risk=(
                    "Following embedded instructions could retry or mutate "
                    "without a classified playbook."
                ),
                human_question=(
                    "Can you confirm the real vendor error and whether any "
                    "write already landed, ignoring the instruction-like text?"
                ),
            )
        if pack.partial_write is True or "partial" in blob:
            return IncidentAnalysis(
                summary=(
                    "Warehouse evidence looks like a partial write; automated "
                    "retry would risk duplicates."
                ),
                probable_root_cause=(
                    "The load appears to have written some rows without a "
                    "clean warehouse success."
                ),
                evidence=evidence,
                confidence=0.72,
                unknowns=(
                    "Whether the written rows are committed production data",
                    "Whether a cleanup job already exists",
                ),
                suggested_next_checks=(
                    "Compare warehouse row identity to the source file",
                    "Confirm whether downstream jobs already consumed the rows",
                ),
                candidate_repair=(
                    "Investigate a human-approved cleanup or replay after "
                    "confirming committed row identity. Do not blindly rerun."
                ),
                repair_risk="A retry may duplicate already-written rows.",
                human_question=(
                    "Are the already-written rows committed, and is a "
                    "deduplicated replay possible?"
                ),
            )
        if any(
            token in blob
            for token in ("schema_drift", "schema drift", "missing column", "renamed")
        ):
            return IncidentAnalysis(
                summary=(
                    "Log and schema facts look like a contract drift; this is "
                    "an explanation, not a repair authorization."
                ),
                probable_root_cause=(
                    "Source columns may no longer match the expected contract."
                ),
                evidence=evidence,
                confidence=0.64,
                unknowns=(
                    "Whether a human-approved exact recipe already exists",
                    "Whether the file is a one-off or a lasting contract change",
                ),
                suggested_next_checks=(
                    "Compare expected vs observed headers",
                    "Look up an exact approved recipe if one exists",
                ),
                candidate_repair=(
                    "A human-approved exact rename recipe could be reused "
                    "later; this analysis does not approve one."
                ),
                repair_risk=(
                    "Applying a guessed transform could write the wrong columns."
                ),
                human_question=(
                    "Is this the known renamed-column drift, and is there an "
                    "exact approved recipe?"
                ),
            )
        if any(
            token in blob
            for token in (
                "429",
                "quota",
                "rate limit",
                "rate_limit",
                "too many requests",
            )
        ):
            return IncidentAnalysis(
                summary=(
                    "Vendor/API quota or 429 rate-limit language appears in "
                    "the task log after a failed run."
                ),
                probable_root_cause=(
                    "The vendor may have rejected the call with a quota or "
                    "429-style limit. This is not confirmed as a transient timeout."
                ),
                evidence=evidence,
                confidence=0.58,
                unknowns=(
                    "Whether the quota window has reset",
                    "Whether any warehouse write started before the rejection",
                ),
                suggested_next_checks=(
                    "Inspect vendor quota / billing status",
                    "Confirm warehouse job identity and row count",
                ),
                candidate_repair=(
                    "Investigate whether a later retry would be safe after "
                    "quota headroom is confirmed and no partial write exists."
                ),
                repair_risk=(
                    "Retrying during an active quota block can fail again "
                    "or duplicate a write that already started."
                ),
                human_question=(
                    "Has the vendor quota reset, and did any rows land "
                    "before the 429/quota error?"
                ),
            )
        if any(
            token in blob
            for token in (
                "iam",
                "permission",
                "access denied",
                "403",
                "unauthorized",
                "forbidden",
            )
        ):
            return IncidentAnalysis(
                summary=(
                    "Permission or IAM-style language appears in the log; "
                    "retry will not grant access."
                ),
                probable_root_cause=(
                    "The runtime identity may lack the required vendor or "
                    "warehouse permission."
                ),
                evidence=evidence,
                confidence=0.61,
                unknowns=(
                    "Which exact role or grant is missing",
                    "Whether the denial is for the source, warehouse, or API",
                ),
                suggested_next_checks=(
                    "Inspect the runtime service-account IAM bindings",
                    "Confirm the denied resource name from the vendor console",
                ),
                candidate_repair=(
                    "Ask a human to grant the missing identity permission, "
                    "then re-run only after access is confirmed."
                ),
                repair_risk="Retrying without a grant repeats the same denial.",
                human_question=(
                    "Which identity is denied, and what grant should be added?"
                ),
            )
        return IncidentAnalysis(
            summary=(
                "Unclassified pipeline failure. Deterministic parsing did not "
                "map this log onto a known playbook."
            ),
            probable_root_cause=(
                "The collected evidence does not confirm a safe automated cause."
            ),
            evidence=evidence,
            confidence=0.40,
            unknowns=(
                "Failure class",
                "Whether any write already committed",
            ),
            suggested_next_checks=(
                "Re-read the vendor error with a human",
                "Confirm warehouse and downstream status before recovery",
            ),
            candidate_repair=(
                "Escalate for human classification before any retry or repair."
            ),
            repair_risk="An unclassified retry or repair may duplicate or corrupt data.",
            human_question="What failure class should this incident be filed as?",
        )


def context_pack_user_payload(pack: ContextPack) -> str:
    """Compact user message from ContextPack. No secrets, no full State dump."""
    payload = context_pack_to_dict(pack)
    compact: dict[str, object] = {}
    for key, value in payload.items():
        if value is None:
            continue
        if value == "" or value == ():
            continue
        compact[key] = value
    return (
        "Read-only ContextPack collected by the deterministic Agent.\n"
        "Use only these facts. Do not invent warehouse results, row counts, "
        "credentials, or missing fields.\n\n"
        f"{json.dumps(compact, indent=2, default=str)}"
    )


def evidence_grounded_in_pack(analysis: IncidentAnalysis, pack: ContextPack) -> bool:
    """True when at least one evidence string appears in supplied context."""
    haystack = " ".join(
        [
            pack.log_excerpt,
            pack.log_source,
            pack.warehouse_evidence,
            pack.downstream_evidence,
            pack.error,
            pack.orchestrator_status,
            pack.warehouse_status,
            pack.downstream_impact,
            pack.escalation_reason,
            pack.retry_safety_reason,
            "" if pack.rows_written is None else str(pack.rows_written),
            "" if pack.partial_write is None else str(pack.partial_write),
        ]
    ).lower()
    if not analysis.evidence:
        return False
    return any(
        item.lower() in haystack or item.lower()[:80] in haystack
        for item in analysis.evidence
    )


def _content_from_response(response: object) -> str:
    try:
        content = response.choices[0].message.content  # type: ignore[attr-defined]
    except (AttributeError, IndexError, TypeError) as exc:
        raise ValueError("OpenAI response is missing message content.") from exc
    if not isinstance(content, str) or not content.strip():
        raise ValueError("OpenAI response content is empty.")
    return content


def _usage_from_response(response: object) -> tuple[int | None, int | None]:
    usage = getattr(response, "usage", None)
    if usage is None:
        return None, None
    tokens_in = getattr(usage, "prompt_tokens", None)
    tokens_out = getattr(usage, "completion_tokens", None)
    try:
        parsed_in = int(tokens_in) if tokens_in is not None else None
    except (TypeError, ValueError):
        parsed_in = None
    try:
        parsed_out = int(tokens_out) if tokens_out is not None else None
    except (TypeError, ValueError):
        parsed_out = None
    return parsed_in, parsed_out


def _payload_to_analysis(payload: object) -> IncidentAnalysis:
    """Convert model JSON into IncidentAnalysis. Does not validate or write State."""
    if not isinstance(payload, dict):
        raise ValueError("OpenAI incident response must be a JSON object.")
    analysis = _analysis_from_mapping(payload)
    if analysis is None:
        raise ValueError("OpenAI incident response could not build IncidentAnalysis.")
    return analysis


class OpenAIIncidentAdvisor:
    """OpenAI-backed IncidentAdvisor. Advises only; never an Action.

    Inject the official OpenAI client. This class does not construct clients,
    read env vars, mutate State, call Decide/Guard, or choose an Action.

    Optional demo::

        advisor = OpenAIIncidentAdvisor(client=OpenAI(), model="gpt-4o-mini")
        run_agent(state, adapter=adapter, advisor=advisor)
    """

    provider = "openai"

    def __init__(self, client: object, model: str) -> None:
        self._client = client
        self._model = model
        self.last_latency_ms: float | None = None
        self.last_tokens_in: int | None = None
        self.last_tokens_out: int | None = None

    @property
    def model(self) -> str:
        return self._model

    def analyze(self, context_pack: ContextPack) -> IncidentAnalysis:
        started = time.perf_counter()
        self.last_tokens_in = None
        self.last_tokens_out = None
        try:
            response = self._client.chat.completions.create(  # type: ignore[attr-defined]
                model=self._model,
                messages=[
                    {"role": "system", "content": ADVISOR_SYSTEM_PROMPT},
                    {"role": "user", "content": context_pack_user_payload(context_pack)},
                ],
                response_format=_ADVISOR_RESPONSE_FORMAT,
            )
            payload = json.loads(_content_from_response(response))
            analysis = _payload_to_analysis(payload)
            self.last_tokens_in, self.last_tokens_out = _usage_from_response(response)
            return analysis
        except Exception as exc:
            raise ValueError(
                "OpenAI incident advisor failed to produce an IncidentAnalysis."
            ) from exc
        finally:
            self.last_latency_ms = (time.perf_counter() - started) * 1000
