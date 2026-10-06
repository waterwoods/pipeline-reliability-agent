"""Pipeline Reliability Agent — Guard.

Loop position:

    State -> Decide -> Guard -> Execute

    Decide  proposes an action ("I think we should RETRY").
    Guard   allows or rejects that proposal ("RETRY is not authorized here").
    Execute performs the side effect only when Guard says allowed=True.

Why Decide and Guard are separate
---------------------------------
Decide is a policy brain: given symptoms, what is the *best next move*?
Guard is a safety gate: given the proposal and current facts, is execution
*permitted*?

In production Data Engineering, these must not collapse into one step:

- Decide may later be an LLM or rules engine that can be wrong, drift, or
  hallucinate a plausible-sounding recovery plan.
- Guard is deterministic code with hard limits: retry caps, blast-radius
  checks, and evidence flags. It does not "think" — it enforces.

"Decide thinks RETRY is good" does NOT mean execution is authorized.
Authorization happens only when Guard returns allowed=True.

Session 4: Guard consults the recovery policy independently of Decide.
A dangerous or ambiguous case never falls through to an allowed RETRY.
BACKFILL_PARTITION uses the same pattern: evaluate_backfill_safety() is
the rulebook; Guard only maps allowed/reason onto GuardResult.

Guard is pure: no State mutation, no API calls, no logging side effects.
Same (State, action) in -> same GuardResult out.
"""

from __future__ import annotations

from dataclasses import dataclass

from pipeline_reliability.decide import APPLY_APPROVED_REPAIR, BACKFILL_PARTITION, RETRY
from pipeline_reliability.recovery import evaluate_backfill_safety, evaluate_retry_safety
from pipeline_reliability.repair_recipes import evaluate_repair_safety
from pipeline_reliability.state import PipelineReliabilityState


@dataclass(frozen=True)
class GuardResult:
    """Structured allow/deny answer for one proposed action."""

    allowed: bool
    reason: str


def guard(state: PipelineReliabilityState, action: str) -> GuardResult:
    """Check whether `action` is safe and allowed to execute right now."""
    # APPLY_APPROVED_REPAIR is a mutation. Re-read the human-approved store
    # and require an exact signature match before Execute may run.
    if action == APPLY_APPROVED_REPAIR:
        safety = evaluate_repair_safety(state)
        return GuardResult(allowed=safety.allowed, reason=safety.reason)

    # BACKFILL_PARTITION writes a partition. Policy lives in recovery.py;
    # Guard only maps BackfillSafety onto GuardResult. Occupancy False/None,
    # partial write, duplicate risk, and critical downstream all deny.
    if action == BACKFILL_PARTITION:
        safety = evaluate_backfill_safety(state)
        return GuardResult(allowed=safety.allowed, reason=safety.reason)

    # Rule 1 — non-RETRY actions pass for now
    # WHY: This Guard version focuses on the riskiest side effect (RETRY).
    # CHECK_LOG, ASK_HUMAN, STOP_SAFE, and FINISH will get stricter checks later.
    if action != RETRY:
        return GuardResult(
            allowed=True,
            reason=f"{action} passes Guard (no RETRY-specific checks in v1).",
        )

    safety = evaluate_retry_safety(state)
    return GuardResult(allowed=safety.allowed, reason=safety.reason)
