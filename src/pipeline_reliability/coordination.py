"""Atomic lease claims with fencing tokens.

The in-memory store makes the concurrency contract executable without exposing
any production datastore or account-specific code. A real deployment would put
the compare-and-set operation in a transactional shared store.
"""

from __future__ import annotations

from dataclasses import dataclass
from threading import Lock


@dataclass(frozen=True)
class Claim:
    incident_id: str
    owner: str
    generation: int
    expires_at: float
    takeover: bool


class LeaseStore:
    def __init__(self) -> None:
        self._claims: dict[str, Claim] = {}
        self._lock = Lock()

    def claim(
        self, incident_id: str, owner: str, *, now: float, ttl: float
    ) -> Claim | None:
        """Atomically acquire, renew, or take over an expired lease."""
        with self._lock:
            current = self._claims.get(incident_id)
            if current and current.expires_at > now and current.owner != owner:
                return None

            takeover = bool(current and current.owner != owner)
            generation = 1 if current is None else current.generation + 1
            claim = Claim(
                incident_id=incident_id,
                owner=owner,
                generation=generation,
                expires_at=now + ttl,
                takeover=takeover,
            )
            self._claims[incident_id] = claim
            return claim

    def is_current(
        self, incident_id: str, owner: str, generation: int, *, now: float
    ) -> bool:
        """Fence a stale worker before it can perform a side effect."""
        with self._lock:
            current = self._claims.get(incident_id)
            return bool(
                current
                and current.owner == owner
                and current.generation == generation
                and current.expires_at > now
            )
