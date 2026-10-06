"""Pipeline Reliability Agent — optional fact intelligence.

Intelligence may *propose* facts from a raw TaskLogResult. It must not
mutate State, choose RETRY, or authorize any Action. Apply remains the
only writer; Decide and Guard remain the only action authorities.

V1 wiring is fallback-only and injection-only: nothing runs unless a
FactIntelligence is supplied, and even then only when deterministic
Apply cannot already structure the log. Completeness rejections get
exactly one repair_facts call; Intelligence still cannot choose an Action.
"""

from __future__ import annotations

import json
from dataclasses import asdict
from typing import Protocol

from pipeline_reliability.adapters import TaskLogResult
from pipeline_reliability.facts import (
    ALLOWED_FAILURE_TYPES,
    FactProposal,
)

_SYSTEM_PROMPT = """Classify this pipeline task failure and propose facts only.

You must:
- classify the failure
- propose facts only
- never propose an Action
- never output RETRY / APPLY_APPROVED_REPAIR / DELETE / RUN_TOOL / approved_action / suggested_action

failure_type must be one of:
SOURCE_MISSING
LOW_VOLUME
FORMAT_ERROR
TIMEOUT
PERMISSION_ERROR
DATA_CONFLICT
UNKNOWN

facts keys only:
file_present
expected_object
observed_rows
volume_status
expected_delimiter
observed_delimiter
error

Also return confidence (0.0 to 1.0) and evidence.
"""

_REPAIR_SYSTEM_PROMPT = """Your previous FactProposal was rejected by the deterministic validator.

Return a corrected FactProposal using ONLY evidence supported by the original log.

Do NOT invent facts merely to satisfy the validator.
If the evidence does not support the missing facts, return UNKNOWN or another evidence-supported proposal.

You must:
- propose facts only
- never propose an Action
- never output RETRY / APPLY_APPROVED_REPAIR / ASK_HUMAN / STOP_SAFE / approved_action / suggested_action

failure_type must be one of:
SOURCE_MISSING
LOW_VOLUME
FORMAT_ERROR
TIMEOUT
PERMISSION_ERROR
DATA_CONFLICT
UNKNOWN

facts keys only:
file_present
expected_object
observed_rows
volume_status
expected_delimiter
observed_delimiter
error

Also return confidence (0.0 to 1.0) and evidence.
"""

_FACT_PROPERTY_SCHEMA: dict[str, dict[str, object]] = {
    "file_present": {"type": ["boolean", "null"]},
    "expected_object": {"type": ["string", "null"]},
    "observed_rows": {"type": ["integer", "null"]},
    "volume_status": {"type": ["string", "null"]},
    "expected_delimiter": {"type": ["string", "null"]},
    "observed_delimiter": {"type": ["string", "null"]},
    "error": {"type": ["string", "null"]},
}

FACT_PROPOSAL_JSON_SCHEMA: dict[str, object] = {
    "type": "object",
    "properties": {
        "failure_type": {
            "type": "string",
            "enum": sorted(ALLOWED_FAILURE_TYPES),
        },
        "facts": {
            "type": "object",
            "properties": _FACT_PROPERTY_SCHEMA,
            "required": list(_FACT_PROPERTY_SCHEMA),
            "additionalProperties": False,
        },
        "confidence": {"type": "number"},
        "evidence": {"type": "string"},
    },
    "required": ["failure_type", "facts", "confidence", "evidence"],
    "additionalProperties": False,
}

_RESPONSE_FORMAT: dict[str, object] = {
    "type": "json_schema",
    "json_schema": {
        "name": "fact_proposal",
        "strict": True,
        "schema": FACT_PROPOSAL_JSON_SCHEMA,
    },
}


class FactIntelligence(Protocol):
    """Optional fallback that proposes facts from a raw TaskLogResult."""

    def propose_facts(self, observation: TaskLogResult) -> FactProposal:
        ...

    def repair_facts(
        self,
        observation: TaskLogResult,
        rejected_proposal: FactProposal,
        rejection_reason: str,
    ) -> FactProposal:
        ...


def _observation_payload(observation: TaskLogResult) -> str:
    """Send only the raw log/error fields needed for diagnosis."""
    return (
        f"error_type: {observation.error_type}\n"
        f"message: {observation.message}\n"
        f"detail: {observation.detail}"
    )


def _content_from_response(response: object) -> str:
    try:
        content = response.choices[0].message.content  # type: ignore[attr-defined]
    except (AttributeError, IndexError, TypeError) as exc:
        raise ValueError("OpenAI response is missing message content.") from exc
    if not isinstance(content, str) or not content.strip():
        raise ValueError("OpenAI response content is empty.")
    return content


def _payload_to_proposal(payload: object) -> FactProposal:
    """Convert model JSON into FactProposal. Does not validate or write State."""
    if not isinstance(payload, dict):
        raise ValueError("OpenAI fact response must be a JSON object.")
    try:
        failure_type = payload["failure_type"]
        facts = payload["facts"]
        confidence = payload["confidence"]
        evidence = payload["evidence"]
    except KeyError as exc:
        raise ValueError(f"OpenAI fact response missing {exc.args[0]}.") from exc
    if not isinstance(facts, dict):
        raise ValueError("OpenAI fact response facts must be an object.")
    # Strict schema uses null for unused keys; drop those so they are absent,
    # not typed as None. Extra keys from a non-schema client are kept so the
    # existing validator can reject them.
    cleaned = {key: value for key, value in facts.items() if value is not None}
    return FactProposal(
        failure_type=failure_type,  # type: ignore[arg-type]
        facts=cleaned,
        confidence=confidence,  # type: ignore[arg-type]
        evidence=evidence,  # type: ignore[arg-type]
    )


class OpenAIFactIntelligence:
    """OpenAI-backed FactIntelligence. Proposes facts only; never an Action.

    Inject the official OpenAI client. This class does not construct clients,
    read env vars, mutate State, call Decide/Guard, or choose an Action.

    Optional demo::

        intelligence = OpenAIFactIntelligence(client=OpenAI(), model="gpt-4o-mini")
        run_agent(state, adapter=adapter, intelligence=intelligence)
    """

    def __init__(self, client: object, model: str) -> None:
        self._client = client
        self._model = model

    def propose_facts(self, observation: TaskLogResult) -> FactProposal:
        return self._request_proposal(
            [
                {"role": "system", "content": _SYSTEM_PROMPT},
                {"role": "user", "content": _observation_payload(observation)},
            ]
        )

    def repair_facts(
        self,
        observation: TaskLogResult,
        rejected_proposal: FactProposal,
        rejection_reason: str,
    ) -> FactProposal:
        user_content = (
            f"{_observation_payload(observation)}\n\n"
            "Your previous FactProposal was rejected by the deterministic validator.\n\n"
            f"Reason:\n{rejection_reason}\n\n"
            "Previous FactProposal:\n"
            f"{json.dumps(asdict(rejected_proposal), indent=2)}\n\n"
            "Return a corrected FactProposal using ONLY evidence supported by the "
            "original log.\n"
            "Do NOT invent facts merely to satisfy the validator.\n"
            "If the evidence does not support the missing facts, return UNKNOWN or "
            "another evidence-supported proposal."
        )
        return self._request_proposal(
            [
                {"role": "system", "content": _REPAIR_SYSTEM_PROMPT},
                {"role": "user", "content": user_content},
            ]
        )

    def _request_proposal(
        self, messages: list[dict[str, str]]
    ) -> FactProposal:
        try:
            response = self._client.chat.completions.create(  # type: ignore[attr-defined]
                model=self._model,
                messages=messages,
                response_format=_RESPONSE_FORMAT,
            )
            payload = json.loads(_content_from_response(response))
            return _payload_to_proposal(payload)
        except Exception as exc:
            raise ValueError(
                "OpenAI fact intelligence failed to produce a FactProposal."
            ) from exc
