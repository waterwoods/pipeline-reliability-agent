"""Small data model for the State → Decide → Guard → Execute loop."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any


class Action(StrEnum):
    INSPECT = "INSPECT"
    RETRY = "RETRY"
    RECONCILE = "RECONCILE"
    ASK_HUMAN = "ASK_HUMAN"
    STOP_SAFE = "STOP_SAFE"


class CommitStatus(StrEnum):
    UNKNOWN = "UNKNOWN"
    EMPTY = "EMPTY"
    COMMITTED = "COMMITTED"
    PARTIAL = "PARTIAL"


class EffectStatus(StrEnum):
    NONE = "NONE"
    UNKNOWN = "UNKNOWN"
    CONFIRMED = "CONFIRMED"


@dataclass
class IncidentState:
    """Only the facts needed to authorize the next recovery step."""

    incident_id: str
    pipeline: str
    run_id: str
    epoch: int = 0
    commit_status: CommitStatus = CommitStatus.UNKNOWN
    commit_evidence_epoch: int = -1
    retry_effect: EffectStatus = EffectStatus.NONE
    retry_count: int = 0
    reconcile_required: bool = False
    lease_generation: int | None = None
    outcome: str | None = None
    evidence: list[str] = field(default_factory=list)

    @property
    def commit_evidence_is_fresh(self) -> bool:
        return self.commit_evidence_epoch == self.epoch


@dataclass(frozen=True)
class Decision:
    action: Action
    reason: str


@dataclass(frozen=True)
class GuardResult:
    allowed: bool
    reason: str


@dataclass(frozen=True)
class Observation:
    action: Action
    detail: str
    commit_status: CommitStatus | None = None
    retry_effect: EffectStatus | None = None


@dataclass(frozen=True)
class TraceEvent:
    phase: str
    action: str
    detail: str
    fields: dict[str, Any] = field(default_factory=dict)

