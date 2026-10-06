"""Bedrock fact adapter. Proposes facts only; never an action.

Inject a Converse-shaped client. This module does not construct an AWS
client, read credentials, or choose RETRY / BACKFILL. Accepted facts are
evidence. Decide and Guard in agent.py stay the only authorities.
"""

from __future__ import annotations

import json
from dataclasses import dataclass

from pipeline_reliability.model import IncidentState

_FAILURE_TYPES = frozenset(
    {
        "SOURCE_MISSING",
        "LOW_VOLUME",
        "FORMAT_ERROR",
        "TIMEOUT",
        "PERMISSION_ERROR",
        "DATA_CONFLICT",
        "UNKNOWN",
    }
)

_FACT_TYPES: dict[str, type] = {
    "file_present": bool,
    "expected_object": str,
    "observed_rows": int,
    "volume_status": str,
    "expected_delimiter": str,
    "observed_delimiter": str,
    "error": str,
}

# Action words are not facts. A schema match does not grant authority.
_AUTHORITY_KEYS = frozenset(
    {
        "RETRY",
        "BACKFILL",
        "BACKFILL_PARTITION",
        "EXECUTE",
        "APPLY_APPROVED_REPAIR",
        "DELETE",
        "RUN_TOOL",
        "approved_action",
        "suggested_action",
        "authorization",
        "execute",
    }
)

_MIN_CONFIDENCE = 0.80
_MAX_TOKENS = 800

_SYSTEM_PROMPT = """Classify this pipeline task failure and propose facts only.

You must:
- propose facts only
- never propose an action
- never output RETRY, BACKFILL, BACKFILL_PARTITION, or suggested_action

failure_type must be one of:
SOURCE_MISSING, LOW_VOLUME, FORMAT_ERROR, TIMEOUT, PERMISSION_ERROR, DATA_CONFLICT, UNKNOWN

facts keys only:
file_present, expected_object, observed_rows, volume_status,
expected_delimiter, observed_delimiter, error

Also return confidence (0.0 to 1.0) and evidence.
"""

_FACT_SCHEMA: dict[str, object] = {
    "type": "object",
    "properties": {
        "failure_type": {"type": "string", "enum": sorted(_FAILURE_TYPES)},
        "facts": {
            "type": "object",
            "properties": {
                key: {"type": ["boolean", "null"]}
                if kind is bool
                else {"type": ["integer", "null"]}
                if kind is int
                else {"type": ["string", "null"]}
                for key, kind in _FACT_TYPES.items()
            },
            "required": sorted(_FACT_TYPES),
            "additionalProperties": False,
        },
        "confidence": {"type": "number"},
        "evidence": {"type": "string"},
    },
    "required": ["failure_type", "facts", "confidence", "evidence"],
    "additionalProperties": False,
}


def fact_proposal_output_config() -> dict[str, object]:
    """Converse outputConfig. Enforcement here does not replace the validator."""
    return {
        "textFormat": {
            "type": "json_schema",
            "structure": {
                "jsonSchema": {
                    "schema": json.dumps(
                        _FACT_SCHEMA, separators=(",", ":"), sort_keys=True
                    ),
                    "name": "fact_proposal",
                    "description": (
                        "Pipeline failure facts only. Never an action, "
                        "RETRY, or BACKFILL."
                    ),
                }
            },
        }
    }


@dataclass(frozen=True)
class TaskLog:
    """Raw failure text. Not State and not an action."""

    error_type: str
    message: str
    detail: str


@dataclass(frozen=True)
class FactProposal:
    """Model-shaped fact bundle. Not State and not an action."""

    failure_type: str
    facts: dict[str, object]
    confidence: float
    evidence: str


@dataclass(frozen=True)
class FactValidation:
    accepted: bool
    reason: str


def _number(value: object) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def _fact_type_ok(key: str, value: object) -> bool:
    expected = _FACT_TYPES[key]
    if expected is int:
        return isinstance(value, int) and not isinstance(value, bool)
    if expected is bool:
        return isinstance(value, bool)
    return isinstance(value, expected)


def validate_fact_proposal(proposal: FactProposal) -> FactValidation:
    """Accept or reject a proposal. Does not write State or choose an action."""
    if proposal.failure_type not in _FAILURE_TYPES:
        return FactValidation(False, f"failure_type {proposal.failure_type!r} is not allowed.")
    if not isinstance(proposal.facts, dict):
        return FactValidation(False, "facts must be a dict.")

    for key, value in proposal.facts.items():
        if key in _AUTHORITY_KEYS:
            return FactValidation(
                False,
                f"action-like field {key!r} is not allowed on a fact proposal.",
            )
        if key not in _FACT_TYPES:
            return FactValidation(False, f"fact key {key!r} is not allowed.")
        if not _fact_type_ok(key, value):
            return FactValidation(False, f"fact {key!r} has an invalid type.")

    if not _number(proposal.confidence) or not 0.0 <= proposal.confidence <= 1.0:
        return FactValidation(False, "confidence must be a number from 0.0 to 1.0.")
    if proposal.confidence < _MIN_CONFIDENCE:
        return FactValidation(
            False,
            f"confidence {proposal.confidence} is below {_MIN_CONFIDENCE}.",
        )
    if not isinstance(proposal.evidence, str) or not proposal.evidence.strip():
        return FactValidation(False, "evidence must be a non-empty string.")
    return FactValidation(True, "fact proposal accepted.")


def record_fact_evidence(state: IncidentState, proposal: FactProposal) -> None:
    """Append accepted fact text. Does not set an action or clear UNKNOWN."""
    validation = validate_fact_proposal(proposal)
    if not validation.accepted:
        raise ValueError(validation.reason)
    state.evidence.append(
        f"bedrock fact {proposal.failure_type}: {proposal.evidence}"
    )


def _observation_text(observation: TaskLog) -> str:
    return (
        f"error_type: {observation.error_type}\n"
        f"message: {observation.message}\n"
        f"detail: {observation.detail}"
    )


def _text_from_converse(response: object) -> str:
    if not isinstance(response, dict):
        raise ValueError("Bedrock response is not a Converse payload.")
    try:
        content = response["output"]["message"]["content"]
    except (KeyError, TypeError) as exc:
        raise ValueError("Bedrock response is missing message content.") from exc
    if not isinstance(content, list):
        raise ValueError("Bedrock response content is not a list.")
    texts: list[str] = []
    for block in content:
        if not isinstance(block, dict):
            raise ValueError("Bedrock response content block is not an object.")
        if "toolUse" in block or "toolResult" in block:
            raise ValueError(
                "Bedrock response included tool use. "
                "Fact intelligence does not accept tool calls."
            )
        text = block.get("text")
        if isinstance(text, str):
            texts.append(text)
    combined = "".join(texts).strip()
    if not combined:
        raise ValueError("Bedrock response content is empty.")
    return combined


def _facts_for_validator(payload: dict[str, object]) -> dict[str, object]:
    raw_facts = payload.get("facts")
    if not isinstance(raw_facts, dict):
        raise ValueError("Bedrock fact response facts must be an object.")
    facts = {key: value for key, value in raw_facts.items() if value is not None}
    for key, value in payload.items():
        if key in {"failure_type", "facts", "confidence", "evidence"}:
            continue
        if key in _AUTHORITY_KEYS:
            facts[key] = value
    return facts


def _payload_to_proposal(payload: object) -> FactProposal:
    if not isinstance(payload, dict):
        raise ValueError("Bedrock fact response must be a JSON object.")
    try:
        failure_type = payload["failure_type"]
        confidence = payload["confidence"]
        evidence = payload["evidence"]
    except KeyError as exc:
        raise ValueError(f"Bedrock fact response missing {exc.args[0]}.") from exc
    if not isinstance(failure_type, str) or not isinstance(evidence, str):
        raise ValueError("Bedrock fact response fields have the wrong type.")
    if not _number(confidence):
        raise ValueError("Bedrock fact response confidence must be a number.")
    return FactProposal(
        failure_type=failure_type,
        facts=_facts_for_validator(payload),
        confidence=float(confidence),
        evidence=evidence,
    )


class BedrockFactIntelligence:
    """Converse client in, FactProposal out. No tools and no action authority."""

    provider = "bedrock"

    def __init__(self, client: object, model: str) -> None:
        if not model.strip():
            raise ValueError("Bedrock model id is required.")
        self._client = client
        self._model = model.strip()

    @property
    def model(self) -> str:
        return self._model

    def propose_facts(self, observation: TaskLog) -> FactProposal:
        try:
            response = self._client.converse(  # type: ignore[attr-defined]
                modelId=self._model,
                system=[{"text": _SYSTEM_PROMPT}],
                messages=[
                    {
                        "role": "user",
                        "content": [{"text": _observation_text(observation)}],
                    }
                ],
                inferenceConfig={"maxTokens": _MAX_TOKENS, "temperature": 0},
                outputConfig=fact_proposal_output_config(),
            )
            return _payload_to_proposal(json.loads(_text_from_converse(response)))
        except Exception as exc:
            raise ValueError(
                "Bedrock fact intelligence failed to produce a FactProposal."
            ) from exc
