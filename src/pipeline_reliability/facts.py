"""Pipeline Reliability Agent — V1 Structured Fact Contract.

Core principle
--------------
An LLM may *propose* facts. It may not modify State, choose RETRY, or
authorize any Action. Only deterministic code may accept a proposal, and
acceptance is not State mutation — Apply still owns writes.

This module is the contract + gate only. It does not call an LLM, does not
touch Decide/Guard/adapters/the agent loop, and does not write State.
The validator checks legality, then completeness; it never fills facts.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

# ---------------------------------------------------------------------------
# V1 failure types — closed set, not a taxonomy framework
# ---------------------------------------------------------------------------

FailureType = Literal[
    "SOURCE_MISSING",
    "LOW_VOLUME",
    "FORMAT_ERROR",
    "TIMEOUT",
    "PERMISSION_ERROR",
    "DATA_CONFLICT",
    "UNKNOWN",
]

SOURCE_MISSING: FailureType = "SOURCE_MISSING"
LOW_VOLUME: FailureType = "LOW_VOLUME"
FORMAT_ERROR: FailureType = "FORMAT_ERROR"
TIMEOUT: FailureType = "TIMEOUT"
PERMISSION_ERROR: FailureType = "PERMISSION_ERROR"
DATA_CONFLICT: FailureType = "DATA_CONFLICT"
UNKNOWN: FailureType = "UNKNOWN"

ALLOWED_FAILURE_TYPES: frozenset[str] = frozenset(
    {
        SOURCE_MISSING,
        LOW_VOLUME,
        FORMAT_ERROR,
        TIMEOUT,
        PERMISSION_ERROR,
        DATA_CONFLICT,
        UNKNOWN,
    }
)

# Keys that already map onto PipelineReliabilityState fact fields.
# Anything else is a new channel into State and is rejected.
ALLOWED_FACT_KEYS: frozenset[str] = frozenset(
    {
        "file_present",
        "expected_object",
        "observed_rows",
        "volume_status",
        "expected_delimiter",
        "observed_delimiter",
        "error",
    }
)

# Completeness is presence-only. The validator never infers, guesses, or
# fills missing keys. Types without a mapping have no extra required facts.
# TIMEOUT / PERMISSION_ERROR / DATA_CONFLICT / UNKNOWN are omitted on purpose.
REQUIRED_FACTS_BY_FAILURE_TYPE: dict[str, frozenset[str]] = {
    SOURCE_MISSING: frozenset({"file_present", "expected_object"}),
    LOW_VOLUME: frozenset({"observed_rows", "volume_status"}),
    FORMAT_ERROR: frozenset({"expected_delimiter", "observed_delimiter"}),
}

# Action / authority tokens must never ride in on a fact proposal.
# Unknown keys are already rejected; this set is the explicit Authority fence.
FORBIDDEN_FIELDS: frozenset[str] = frozenset(
    {
        "RETRY",
        "APPLY_APPROVED_REPAIR",
        "DELETE",
        "RUN_TOOL",
        "approved_action",
        "suggested_action",
    }
)

# Below this, a structurally valid proposal is still not accepted for State.
MIN_FACT_CONFIDENCE = 0.80

_FACT_VALUE_TYPES: dict[str, type] = {
    "file_present": bool,
    "expected_object": str,
    "observed_rows": int,
    "volume_status": str,
    "expected_delimiter": str,
    "observed_delimiter": str,
    "error": str,
}


@dataclass(frozen=True)
class FactProposal:
    """LLM-shaped (or test-shaped) fact bundle. Not State. Not an Action."""

    failure_type: FailureType
    facts: dict[str, object]
    confidence: float
    evidence: str


@dataclass(frozen=True)
class FactProposalValidationResult:
    """Deterministic accept/reject. accepted=True does not write State.

    ``incomplete`` is True only when legality/confidence/evidence already
    passed and required facts for the failure_type are missing. That is the
    sole signal Apply may use for one-shot intelligence repair.
    """

    accepted: bool
    reason: str
    incomplete: bool = False


def _is_number(value: object) -> bool:
    """True for int/float confidence scores; bool is not a number here."""
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def _value_type_ok(key: str, value: object) -> bool:
    """bool is a subclass of int — do not let True/False pass as observed_rows."""
    expected = _FACT_VALUE_TYPES[key]
    if expected is int:
        return isinstance(value, int) and not isinstance(value, bool)
    if expected is bool:
        return isinstance(value, bool)
    return isinstance(value, expected)


def validate_fact_proposal(proposal: FactProposal) -> FactProposalValidationResult:
    """Accept or reject a FactProposal. Pure: no I/O, no State mutation.

    Checks legality first, then completeness. Never infers, guesses, or
    adds missing facts. Incomplete proposals are rejected as-is.
    """
    if proposal.failure_type not in ALLOWED_FAILURE_TYPES:
        return FactProposalValidationResult(
            accepted=False,
            reason=f"failure_type {proposal.failure_type!r} is not an allowed V1 type.",
        )

    if not isinstance(proposal.facts, dict):
        return FactProposalValidationResult(
            accepted=False,
            reason="facts must be a dict.",
        )

    for key in proposal.facts:
        if key in FORBIDDEN_FIELDS:
            return FactProposalValidationResult(
                accepted=False,
                reason=f"action-like field {key!r} is not allowed on a fact proposal.",
            )
        if key not in ALLOWED_FACT_KEYS:
            return FactProposalValidationResult(
                accepted=False,
                reason=f"fact key {key!r} is not in the V1 whitelist.",
            )
        if not _value_type_ok(key, proposal.facts[key]):
            expected = _FACT_VALUE_TYPES[key].__name__
            return FactProposalValidationResult(
                accepted=False,
                reason=(
                    f"fact {key!r} has invalid type "
                    f"{type(proposal.facts[key]).__name__}; expected {expected}."
                ),
            )

    if not _is_number(proposal.confidence):
        return FactProposalValidationResult(
            accepted=False,
            reason="confidence must be a number between 0.0 and 1.0.",
        )

    if proposal.confidence < 0.0 or proposal.confidence > 1.0:
        return FactProposalValidationResult(
            accepted=False,
            reason=(
                f"confidence {proposal.confidence} is outside the allowed "
                "range [0.0, 1.0]."
            ),
        )

    if not isinstance(proposal.evidence, str) or not proposal.evidence.strip():
        return FactProposalValidationResult(
            accepted=False,
            reason="evidence must be a non-empty string.",
        )

    if proposal.confidence < MIN_FACT_CONFIDENCE:
        return FactProposalValidationResult(
            accepted=False,
            reason=(
                f"confidence {proposal.confidence} is below MIN_FACT_CONFIDENCE "
                f"({MIN_FACT_CONFIDENCE}); not accepted for State mutation."
            ),
        )

    required = REQUIRED_FACTS_BY_FAILURE_TYPE.get(proposal.failure_type, frozenset())
    missing = sorted(required.difference(proposal.facts))
    if missing:
        return FactProposalValidationResult(
            accepted=False,
            incomplete=True,
            reason=(
                f"missing required {proposal.failure_type} facts: "
                f"{', '.join(missing)}."
            ),
        )

    return FactProposalValidationResult(accepted=True, reason="fact proposal accepted.")
