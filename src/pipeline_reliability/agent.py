"""Safety-first pipeline recovery loop.

The policy is deliberately deterministic. An LLM may enrich evidence outside
this module, but it cannot authorize RETRY or call the adapter.
"""

from __future__ import annotations

import json
from dataclasses import asdict
from typing import Protocol

from pipeline_reliability.coordination import Claim, LeaseStore
from pipeline_reliability.model import (
    Action,
    CommitStatus,
    Decision,
    EffectStatus,
    GuardResult,
    IncidentState,
    Observation,
    TraceEvent,
)


class PipelineAdapter(Protocol):
    def inspect_commit(self, state: IncidentState) -> CommitStatus: ...

    def retry(self, state: IncidentState) -> None: ...


def decide(state: IncidentState) -> Decision:
    """Pure policy: State in, proposed action out."""
    if state.outcome:
        return Decision(Action.STOP_SAFE, "incident already has a terminal outcome")
    if state.reconcile_required or state.retry_effect == EffectStatus.UNKNOWN:
        return Decision(Action.RECONCILE, "a prior mutation may have happened")
    if not state.commit_evidence_is_fresh:
        return Decision(Action.INSPECT, "commit evidence is missing or stale")
    if state.commit_status in {CommitStatus.COMMITTED, CommitStatus.PARTIAL}:
        return Decision(Action.STOP_SAFE, "data may already exist; retry is unsafe")
    if state.commit_status == CommitStatus.EMPTY and state.retry_count == 0:
        return Decision(Action.RETRY, "fresh evidence shows no committed rows")
    return Decision(Action.ASK_HUMAN, "automatic recovery cannot prove safety")


def guard(
    state: IncidentState,
    decision: Decision,
    *,
    claim: Claim,
    leases: LeaseStore,
    now: float,
) -> GuardResult:
    """Authorize an action immediately before execution."""
    if not leases.is_current(
        state.incident_id, claim.owner, claim.generation, now=now
    ):
        return GuardResult(False, "stale worker fenced by a newer lease generation")

    if decision.action != Action.RETRY:
        return GuardResult(True, "read-only or terminal action")

    blockers: list[str] = []
    if state.reconcile_required or state.retry_effect == EffectStatus.UNKNOWN:
        blockers.append("unreconciled prior side effect")
    if not state.commit_evidence_is_fresh:
        blockers.append("stale commit evidence")
    if state.commit_status != CommitStatus.EMPTY:
        blockers.append(f"commit status is {state.commit_status}")
    if state.retry_count >= 1:
        blockers.append("retry budget exhausted")

    if blockers:
        return GuardResult(False, "; ".join(blockers))
    return GuardResult(True, "fresh EMPTY evidence and retry budget available")


def execute(
    state: IncidentState, action: Action, adapter: PipelineAdapter
) -> Observation:
    """Perform one adapter call. RETRY records uncertainty before dispatch."""
    if action in {Action.INSPECT, Action.RECONCILE}:
        status = adapter.inspect_commit(state)
        return Observation(action, f"warehouse reported {status}", commit_status=status)

    if action == Action.RETRY:
        state.epoch += 1
        state.retry_count += 1
        state.retry_effect = EffectStatus.UNKNOWN
        state.reconcile_required = True
        state.commit_status = CommitStatus.UNKNOWN
        state.commit_evidence_epoch = -1
        state.evidence.append("retry intent persisted before dispatch")
        try:
            adapter.retry(state)
        except TimeoutError:
            return Observation(
                action,
                "retry response timed out; side effect remains UNKNOWN",
                retry_effect=EffectStatus.UNKNOWN,
            )
        return Observation(
            action,
            "retry accepted; terminal result still requires reconciliation",
            retry_effect=EffectStatus.CONFIRMED,
        )

    return Observation(action, decision_detail(action))


def decision_detail(action: Action) -> str:
    if action == Action.ASK_HUMAN:
        return "paused for human review"
    if action == Action.STOP_SAFE:
        return "stopped without another mutation"
    raise ValueError(f"unsupported action: {action}")


def apply(state: IncidentState, observation: Observation) -> None:
    """Merge only observed facts into State."""
    state.evidence.append(observation.detail)
    if observation.action in {Action.INSPECT, Action.RECONCILE}:
        assert observation.commit_status is not None
        state.commit_status = observation.commit_status
        state.commit_evidence_epoch = state.epoch
        if observation.action == Action.RECONCILE:
            state.retry_effect = EffectStatus.CONFIRMED
            state.reconcile_required = False
    elif observation.action == Action.RETRY:
        state.retry_effect = observation.retry_effect or EffectStatus.UNKNOWN
    elif observation.action == Action.ASK_HUMAN:
        state.outcome = "HITL_REQUIRED"
    elif observation.action == Action.STOP_SAFE:
        state.outcome = "STOP_SAFE"


class Agent:
    def __init__(self, adapter: PipelineAdapter, leases: LeaseStore) -> None:
        self.adapter = adapter
        self.leases = leases

    def run(
        self,
        state: IncidentState,
        claim: Claim,
        *,
        now: float,
        max_steps: int = 8,
    ) -> list[TraceEvent]:
        trace: list[TraceEvent] = []
        state.lease_generation = claim.generation
        if claim.takeover:
            state.reconcile_required = True
            state.retry_effect = EffectStatus.UNKNOWN
            state.commit_status = CommitStatus.UNKNOWN
            state.commit_evidence_epoch = -1
            trace.append(
                TraceEvent(
                    "State",
                    "TAKEOVER",
                    "expired lease taken over; reconcile first",
                )
            )

        for _ in range(max_steps):
            trace.append(
                TraceEvent("State", "READ", "snapshot loaded", _state_fields(state))
            )
            decision = decide(state)
            trace.append(TraceEvent("Decide", decision.action, decision.reason))
            verdict = guard(
                state, decision, claim=claim, leases=self.leases, now=now
            )
            trace.append(
                TraceEvent(
                    "Guard",
                    decision.action,
                    verdict.reason,
                    {"allowed": verdict.allowed},
                )
            )
            if not verdict.allowed:
                state.outcome = "HITL_REQUIRED"
                break
            observation = execute(state, decision.action, self.adapter)
            execution_detail = (
                "adapter call completed"
                if decision.action in {Action.INSPECT, Action.RECONCILE, Action.RETRY}
                else "no external call for terminal action"
            )
            trace.append(TraceEvent("Execute", decision.action, execution_detail))
            trace.append(
                TraceEvent("Observation", decision.action, observation.detail)
            )
            apply(state, observation)
            trace.append(
                TraceEvent(
                    "Apply",
                    decision.action,
                    "observation merged",
                    _state_fields(state),
                )
            )
            if state.outcome:
                break
        else:
            state.outcome = "HITL_REQUIRED"
            state.evidence.append("step budget exhausted")
        return trace


def _state_fields(state: IncidentState) -> dict[str, object]:
    return {
        "epoch": state.epoch,
        "commit_status": state.commit_status,
        "evidence_epoch": state.commit_evidence_epoch,
        "retry_effect": state.retry_effect,
        "retry_count": state.retry_count,
        "reconcile_required": state.reconcile_required,
        "outcome": state.outcome,
    }


def trace_as_jsonl(trace: list[TraceEvent]) -> str:
    return "\n".join(
        json.dumps(asdict(event), default=str, sort_keys=True) for event in trace
    )


class SyntheticAdapter:
    """Demo adapter: the retry commits, but its response is lost."""

    def __init__(self) -> None:
        self.committed = False
        self.retry_calls = 0

    def inspect_commit(self, state: IncidentState) -> CommitStatus:
        return CommitStatus.COMMITTED if self.committed else CommitStatus.EMPTY

    def retry(self, state: IncidentState) -> None:
        self.retry_calls += 1
        self.committed = True
        raise TimeoutError("synthetic transport timeout after dispatch")
