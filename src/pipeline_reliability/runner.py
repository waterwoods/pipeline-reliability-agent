"""Pipeline Reliability Agent — Runner.

Connects every layer into one loop:

    State -> Decide -> Guard -> Execute -> Observation -> Apply -> New State

Why runner owns the while loop
------------------------------
Runner is orchestration: it calls each step in order and wires outputs to inputs.
It does not choose actions (Decide), enforce limits (Guard), run tools (Execute),
or interpret results (Apply). One place controls *when* each step runs.

Why keep Decide, Guard, Execute, and Apply separate
----------------------------------------------------
Each piece has one job and can be tested alone. Runner only sequences them.
If you merge them, you cannot test "what if Guard blocks RETRY?" without
running fake pipelines, and you cannot swap Decide for an LLM later without
touching safety rules.

What stops the loop
-------------------
The loop runs while state.outcome is None and state.awaiting_human is False.
Apply sets outcome when the incident reaches a terminal phase (STOP_SAFE, FINISH)
or pauses for HITL (ASK_HUMAN + awaiting_human=True). While awaiting human input,
runner returns without running Decide, Guard, or Execute.

Human-approved actions
----------------------
When state.approved_action is set, runner executes that action once through
Guard -> Execute -> Apply (Guard is not bypassed), clears approved_action, then
continues the normal Decide loop from the updated State.

HITL V1
-------
When ``hitl_dir`` is set (or derived next to ``checkpoint_path``), ASK_HUMAN
persists a HumanRequest. Humans submit decisions through hitl.py; this runner
does not let humans call tools.

Guard rejection must skip Execute
---------------------------------
If Guard says allowed=False, Execute must not run — otherwise side effects happen
without authorization. Runner uses observation="STOP_SAFE" and Apply records it.
"""

from __future__ import annotations

import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

from pipeline_reliability.adapters import PipelineAdapter
from pipeline_reliability.apply import apply
from pipeline_reliability.checkpoint import (
    RUNNING,
    WAITING_EXTERNAL,
    CheckpointRecord,
    CheckpointStore,
    external_status,
    load_trace,
    require_mutation_lease,
    save_checkpoint,
)
from pipeline_reliability.context_pack import build_context_pack
from pipeline_reliability.decide import (
    ASK_HUMAN,
    CHECK_ORCHESTRATOR_RUN,
    CHECK_WAREHOUSE_JOB,
    MAX_POLLS,
    RETRY,
    STOP_SAFE,
    WAIT,
    decide,
)
from pipeline_reliability.guard import guard
from pipeline_reliability.incident_analysis import IncidentAdvisor, advise_incident
from pipeline_reliability.intelligence import FactIntelligence
from pipeline_reliability.hitl import describe_escalation
from pipeline_reliability import structured_log
from pipeline_reliability.observability import (
    ANALYSIS_REJECTED,
    INCIDENT_ANALYSIS,
    LLM_ANALYZE_INCIDENT,
    IntelligenceTraceEvent,
    RunMetrics,
    RunTrace,
    TraceStep,
    calculate_metrics,
    observation_facts_from_result,
    state_after_from_state,
)
from pipeline_reliability.state import PipelineReliabilityState
from pipeline_reliability.tools import execute

# Investigation + RETRY + up to MAX_POLLS (CHECK+WAIT) pairs + final CHECK + FINISH/ASK.
MAX_STEPS = 10 + (2 * MAX_POLLS)


def _post_retry_attempt_advanced(state: PipelineReliabilityState) -> bool:
    """True when orchestrator identity shows a later attempt than the clear."""
    baseline = state.retry_baseline_attempt_number
    observed = state.attempt_number
    if baseline is not None and observed is not None and observed > baseline:
        return True
    observed_repair = (state.latest_repair_id or "").strip()
    baseline_repair = (state.retry_baseline_repair_id or "").strip()
    return bool(observed_repair) and observed_repair != baseline_repair


def _hold_for_pending_external_evidence(
    action: str,
    state: PipelineReliabilityState,
) -> bool:
    """True when this step still lacks authoritative post-retry evidence.

    The run stays WAITING_EXTERNAL. This does not apply before a RETRY has
    been dispatched, and it does not hold a SUCCESS read that still needs
    its warehouse check. A terminal warehouse fact and a new failed attempt
    fall through so Decide can FINISH, ASK_HUMAN, or STOP_SAFE.
    """
    if state.outcome is not None or state.awaiting_human:
        return False
    dispatched = (
        state.retry_side_effect == "UNKNOWN"
        or state.warehouse_stale
        or state.retries >= 1
    )
    if not dispatched:
        return False
    # This observation did not prove the clear. Stay resumable. Do not ask
    # a human and do not dispatch another RETRY.
    if state.retry_side_effect == "UNKNOWN" and action in {
        CHECK_ORCHESTRATOR_RUN,
        RETRY,
        WAIT,
    }:
        return True
    status = (state.orchestrator_status or "").strip()
    if action == CHECK_ORCHESTRATOR_RUN and (state.retries >= 1 or state.warehouse_stale):
        if status in {"RUNNING", "UNKNOWN"}:
            return True
        if status == "FAILED" and not _post_retry_attempt_advanced(state):
            return True
        return False
    if action == CHECK_WAREHOUSE_JOB and (state.warehouse_stale or state.retries >= 1):
        if state.warehouse_stale:
            return True
        if state.warehouse_status in {"UNKNOWN", "RUNNING"}:
            return True
    return False


def _accepted_retry_ready_to_wait(
    action: str,
    guard_allowed: bool,
    state: PipelineReliabilityState,
) -> bool:
    """True only after Apply has accepted the first RETRY.

    The pre-mutation intent checkpoint is not this point. A RETRY whose side
    effect is still UNKNOWN does not set warehouse_stale. That case stays
    resumable through ``_hold_for_pending_external_evidence`` instead.
    """
    return (
        guard_allowed
        and action == RETRY
        and state.outcome is None
        and state.warehouse_stale
        and not (state.orchestrator_status or "").strip()
        and state.retries == 1
    )


@dataclass
class AgentRunResult:
    """Completed or paused agent execution — State + Trace + Metrics.

    ``agent_run_id`` uniquely identifies *this Agent execution*. It is distinct
    from ``state.run_id``, which is an orchestrator run id used for platform
    inspection. Persistence (run history) keys on ``agent_run_id``.
    """

    state: PipelineReliabilityState
    trace: RunTrace
    metrics: RunMetrics
    agent_run_id: str = field(default_factory=lambda: str(uuid.uuid4()))


def _worker_id(record: CheckpointRecord | None) -> str:
    if record is None:
        return ""
    return (record.lease_owner or "").strip()


def _open_side_effect(state: PipelineReliabilityState) -> str:
    if state.retry_side_effect == "UNKNOWN" or state.backfill_side_effect == "UNKNOWN":
        return "unknown"
    return ""


def _emit(level: str, event: str, **fields: object) -> None:
    """Logging is observation. A failure here must not change the run."""
    try:
        structured_log.emit(level, event, **fields)
    except Exception:
        return


def _export_completed(result: AgentRunResult) -> None:
    """Opt-in OTLP send. Exporter failures stay inside the exporter."""
    try:
        from pipeline_reliability.otel_export import export_completed_run

        export_completed_run(result)
    except Exception:
        return


def run_agent(
    state: PipelineReliabilityState,
    adapter: PipelineAdapter | None = None,
    trace: RunTrace | None = None,
    checkpoint_path: str | Path | None = None,
    intelligence: FactIntelligence | None = None,
    advisor: IncidentAdvisor | None = None,
    hitl_dir: str | Path | None = None,
    agent_run_id: str | None = None,
    store: CheckpointStore | None = None,
    checkpoint_record: CheckpointRecord | None = None,
    pause_after_accepted_retry: bool = False,
    verbose: bool = False,
) -> AgentRunResult:
    """Run the agent loop until state.outcome is set or HITL pause; return State and trace.

    When ``checkpoint_path`` is set, Runner persists State **and** RunTrace
    after each Apply (so a crash cannot lose the latest TraceStep) and
    passes the path into Execute so a RETRY intent is written *before* the
    orchestrator mutation. If ``trace`` is omitted and the file already has
    history, that Trace is reloaded so resume continues the same incident.
    Resume from that file must reconcile, not replay RETRY.

    ``intelligence`` is optional Apply fallback only. It is never an Action
    authority. Omit it to keep current deterministic behavior.

    ``advisor`` is optional Slice 2 incident analysis. It reads ContextPack
    only, writes Trace + HITL, and never chooses an Action.

    ``hitl_dir`` persists HumanRequest records on ASK_HUMAN. When omitted but
    ``checkpoint_path`` is set, defaults to ``<checkpoint_dir>/hitl``.

    ``store`` plus ``pause_after_accepted_retry`` returns after the accepted
    RETRY result has been saved, with store status WAITING_EXTERNAL. Callers
    that omit the store keep the existing loop, including ``/run``.

    Structured logs around this call are fail-open. They do not choose an
    action or change the checkpoint.
    """
    execution_id = agent_run_id or str(uuid.uuid4())
    token = None
    try:
        token = structured_log.bind_agent_run_id(execution_id)
    except Exception:
        token = None
    try:
        return _run_agent(
            state,
            adapter=adapter,
            trace=trace,
            checkpoint_path=checkpoint_path,
            intelligence=intelligence,
            advisor=advisor,
            hitl_dir=hitl_dir,
            agent_run_id=execution_id,
            store=store,
            checkpoint_record=checkpoint_record,
            pause_after_accepted_retry=pause_after_accepted_retry,
            verbose=verbose,
        )
    except Exception as exc:
        _emit(
            "error",
            "agent.failed",
            agent_run_id=execution_id,
            error_type=type(exc).__name__,
        )
        raise
    finally:
        if token is not None:
            structured_log.reset_agent_run_id(token)


def _run_agent(
    state: PipelineReliabilityState,
    adapter: PipelineAdapter | None = None,
    trace: RunTrace | None = None,
    checkpoint_path: str | Path | None = None,
    intelligence: FactIntelligence | None = None,
    advisor: IncidentAdvisor | None = None,
    hitl_dir: str | Path | None = None,
    agent_run_id: str | None = None,
    store: CheckpointStore | None = None,
    checkpoint_record: CheckpointRecord | None = None,
    pause_after_accepted_retry: bool = False,
    verbose: bool = False,
) -> AgentRunResult:
    """Loop body. ``run_agent`` owns the log scope around this call."""
    if trace is None:
        if checkpoint_path is not None and Path(checkpoint_path).is_file():
            trace = load_trace(checkpoint_path)
        else:
            trace = RunTrace()

    execution_id = agent_run_id or str(uuid.uuid4())
    if hitl_dir is None and checkpoint_path is not None:
        hitl_dir = Path(checkpoint_path).parent / "hitl"

    # Checkpoint may lag behind the orchestrator. UNKNOWN means a RETRY may already
    # have been dispatched; stale orchestrator_status is not its result.
    if state.retry_side_effect == "UNKNOWN":
        state.orchestrator_status = ""

    run_started = time.perf_counter()
    if state.retry_side_effect == "UNKNOWN" or state.backfill_side_effect == "UNKNOWN":
        # Resume must reconcile before another mutation. This line is the
        # operator's marker that the open side effect is still unknown.
        _emit(
            "info",
            "agent.reconcile_required",
            agent_run_id=execution_id,
            side_effect_status="unknown",
            reconcile_result="pending",
            worker_id=_worker_id(checkpoint_record),
        )

    advisory_analysis = None
    hold_for_external = False

    def _llm_span_attributes(result: object) -> dict[str, object]:
        accepted = bool(getattr(result, "accepted", False))
        analysis = getattr(result, "analysis", None)
        attributes: dict[str, object] = {
            "llm.provider": getattr(advisor, "provider", "") or "unknown",
            "llm.model": getattr(advisor, "model", "") or "",
            "llm.success": accepted,
            "llm.validation": "accepted" if accepted else "rejected",
        }
        latency = getattr(advisor, "last_latency_ms", None)
        if isinstance(latency, (int, float)):
            attributes["llm.latency_ms"] = float(latency)
        tokens_in = getattr(advisor, "last_tokens_in", None)
        tokens_out = getattr(advisor, "last_tokens_out", None)
        if isinstance(tokens_in, int):
            attributes["llm.tokens_in"] = tokens_in
        if isinstance(tokens_out, int):
            attributes["llm.tokens_out"] = tokens_out
        confidence = getattr(analysis, "confidence", None)
        if isinstance(confidence, (int, float)) and not isinstance(confidence, bool):
            attributes["llm.confidence"] = float(confidence)
        return attributes

    def _record_advisory_analysis() -> TraceStep | None:
        nonlocal advisory_analysis
        if advisor is None:
            return None
        if any(
            event.event_type in {INCIDENT_ANALYSIS, ANALYSIS_REJECTED}
            for event in trace.intelligence_events
        ):
            return None
        started = time.perf_counter()
        try:
            pack = build_context_pack(
                state,
                escalation_reason=state.escalation_reason or describe_escalation(state),
            )
            result = advise_incident(advisor, pack)
        except Exception as exc:
            result = None
            reason = f"advisor failed safely: {exc}"
            trace.intelligence_events.append(
                IntelligenceTraceEvent(
                    event_type=ANALYSIS_REJECTED,
                    accepted=False,
                    reason=reason,
                )
            )
            duration_ms = (time.perf_counter() - started) * 1000
            return TraceStep(
                step_number=len(trace.steps) + 1,
                action=LLM_ANALYZE_INCIDENT,
                guard_allowed=True,
                guard_reason="advisory analysis only; not an Action.",
                observation=reason,
                outcome=None,
                duration_ms=duration_ms,
                timestamp=datetime.now(timezone.utc).isoformat(),
                attributes={
                    "llm.provider": getattr(advisor, "provider", "") or "unknown",
                    "llm.model": getattr(advisor, "model", "") or "",
                    "llm.success": False,
                    "llm.validation": "rejected",
                    "llm.latency_ms": duration_ms,
                },
            )
        if result.accepted and result.analysis is not None:
            advisory_analysis = result.analysis
            trace.intelligence_events.append(
                IntelligenceTraceEvent(
                    event_type=INCIDENT_ANALYSIS,
                    analysis=result.analysis,
                    accepted=True,
                    reason=result.reason,
                )
            )
            observation = result.reason
        else:
            trace.intelligence_events.append(
                IntelligenceTraceEvent(
                    event_type=ANALYSIS_REJECTED,
                    accepted=False,
                    reason=result.reason,
                )
            )
            observation = result.reason
        duration_ms = (time.perf_counter() - started) * 1000
        attributes = _llm_span_attributes(result)
        if "llm.latency_ms" not in attributes:
            attributes["llm.latency_ms"] = duration_ms
        return TraceStep(
            step_number=len(trace.steps) + 1,
            action=LLM_ANALYZE_INCIDENT,
            guard_allowed=True,
            guard_reason="advisory analysis only; not an Action.",
            observation=observation,
            outcome=None,
            duration_ms=duration_ms,
            timestamp=datetime.now(timezone.utc).isoformat(),
            attributes=attributes,
        )

    def _finish() -> AgentRunResult:
        trace.final_outcome = state.outcome
        # Read the owner before a terminal save clears the lease.
        worker_id = _worker_id(checkpoint_record)
        if store is not None and checkpoint_record is not None:
            previous_status = checkpoint_record.status
            # A lease that expired, or a version another worker already
            # advanced, must not be overwritten with this worker's memory.
            require_mutation_lease(store, checkpoint_record)
            checkpoint_record.state = state
            checkpoint_record.trace = trace
            checkpoint_record.status = external_status(
                state, paused_for_external=hold_for_external
            )
            # Scheduling metadata only. Decide did not choose a delay, and
            # this does not increment wake_attempt or retry_count. The same
            # save serves /resume and the wake worker, so there is one place
            # a still-waiting run is armed again.
            if checkpoint_record.status == WAITING_EXTERNAL:
                from pipeline_reliability.wake import schedule_waiting_external

                schedule_waiting_external(checkpoint_record)
            elif checkpoint_record.status != RUNNING:
                # Terminal snapshot. Drop the alarm and the lease. wake_attempt
                # stays; it is history, not ownership.
                checkpoint_record.lease_owner = None
                checkpoint_record.lease_until = None
                checkpoint_record.next_check_at = None
            try:
                store.save(checkpoint_record)
            except Exception as exc:
                _emit(
                    "error",
                    "checkpoint.save_failed",
                    agent_run_id=execution_id,
                    status_from=previous_status,
                    status_to=checkpoint_record.status,
                    error_type=type(exc).__name__,
                    worker_id=worker_id,
                )
                raise
            _emit(
                "info",
                "checkpoint.saved",
                agent_run_id=execution_id,
                status_from=previous_status,
                status_to=checkpoint_record.status,
                outcome=state.outcome or "",
                side_effect_status=_open_side_effect(state),
                worker_id=worker_id,
            )
        metrics = calculate_metrics(trace)
        result = AgentRunResult(
            state=state,
            trace=trace,
            metrics=metrics,
            agent_run_id=execution_id,
        )
        _emit(
            "info",
            "agent.finished",
            agent_run_id=execution_id,
            outcome=state.outcome or "",
            side_effect_status=_open_side_effect(state),
            duration_ms=(time.perf_counter() - run_started) * 1000,
            status_to=checkpoint_record.status if checkpoint_record is not None else "",
            worker_id=worker_id,
        )
        _export_completed(result)
        return result

    def _persist_hitl_if_paused() -> None:
        if not state.awaiting_human or hitl_dir is None:
            return
        from pipeline_reliability.hitl import open_human_request

        opened = open_human_request(
            state,
            agent_run_id=execution_id,
            hitl_dir=hitl_dir,
            checkpoint_path=checkpoint_path,
            incident_analysis=advisory_analysis,
        )
        state.human_request_id = opened.request_id

    def _persist() -> None:
        if checkpoint_path is None:
            return
        trace.final_outcome = state.outcome
        save_checkpoint(state, checkpoint_path, trace=trace)

    # Runner owns the loop — not Decide, not Apply.
    # outcome=None means "incident still open, keep going."
    if state.awaiting_human:
        _persist_hitl_if_paused()
        _persist()
        return _finish()

    step = 0

    while state.outcome is None and not state.awaiting_human:
        step += 1
        if step > MAX_STEPS:
            apply(state, STOP_SAFE, STOP_SAFE, trace=trace)
            max_steps_note = f"max_steps_exceeded: stopped after {MAX_STEPS} steps"
            state.observation = max_steps_note
            state.evidence.append(max_steps_note)
            _persist()
            break

        step_started = time.perf_counter()

        # --- 1. Show what Decide will read this turn -------------------------
        if verbose:
            print("CURRENT STATE:", state)

        # --- 2. Action: human-approved once, otherwise Decide proposes ------
        human_approved = state.approved_action
        if human_approved is not None:
            action = human_approved
        else:
            action = decide(state)

        llm_step = None
        if action == ASK_HUMAN and advisor is not None:
            try:
                llm_step = _record_advisory_analysis()
            except Exception:
                llm_step = None

        # --- 3. Show the proposal --------------------------------------------
        if verbose:
            print("ACTION:", action)

        # --- 4. Guard: allow or reject (pure, no side effects) ----------------
        guard_result = guard(state, action)

        # --- 5. Show authorization result ------------------------------------
        if verbose:
            print("GUARD RESULT:", guard_result)

        # Record the action before Execute. A crash inside the tool still
        # leaves the operator the run id, the action, and the Guard result.
        _emit(
            "info",
            "agent.step_started",
            agent_run_id=execution_id,
            action=action,
            guard_allowed=guard_result.allowed,
            guard_reason=guard_result.reason,
            step_number=step,
            worker_id=_worker_id(checkpoint_record),
        )

        # Crumb Decide and Guard just read. Execute may record intent before Apply.
        state_before = state_after_from_state(state)

        # --- Execute only if Guard allows ------------------------------------
        # Guard rejection must skip Execute — no unauthorized side effects.
        if guard_result.allowed:
            observation = execute(
                state,
                action,
                adapter=adapter,
                checkpoint_path=checkpoint_path,
                store=store,
                checkpoint_record=checkpoint_record,
            )
        else:
            # Tool did not run; synthesize a safe-stop observation for Apply.
            observation = STOP_SAFE

        # --- 6. Show what happened (or what we recorded instead) --------------
        if verbose:
            print("OBSERVATION:", observation)

        # --- 7. Apply: merge action + observation into State ------------------
        apply(
            state,
            action,
            observation,
            intelligence=intelligence,
            trace=trace,
        )

        if human_approved is not None:
            state.approved_action = None

        # Incomplete post-retry evidence is not a terminal outcome. Drop the
        # orchestrator status from this read so the next resume checks again
        # instead of treating it as the new attempt or spending the poll budget.
        # The observation text stays in evidence.
        hold_pending = pause_after_accepted_retry and _hold_for_pending_external_evidence(
            action, state
        )
        if hold_pending and action in {CHECK_ORCHESTRATOR_RUN, RETRY, WAIT}:
            state.orchestrator_status = ""

        if llm_step is not None:
            llm_step.step_number = len(trace.steps) + 1
            trace.steps.append(llm_step)

        duration_ms = (time.perf_counter() - step_started) * 1000
        facts = observation_facts_from_result(observation)
        trace.steps.append(
            TraceStep(
                step_number=len(trace.steps) + 1,
                action=action,
                guard_allowed=guard_result.allowed,
                guard_reason=guard_result.reason,
                observation=state.observation,
                outcome=state.outcome,
                duration_ms=duration_ms,
                timestamp=datetime.now(timezone.utc).isoformat(),
                action_id=(state.action_id or None) if action == RETRY else None,
                observation_facts=facts,
                state_before=state_before,
                state_after=state_after_from_state(state),
                apply_reason=trace.last_apply_reason,
            )
        )
        _emit(
            "info",
            "agent.step",
            agent_run_id=execution_id,
            action=action,
            guard_allowed=guard_result.allowed,
            guard_reason=guard_result.reason,
            side_effect_status=structured_log.side_effect_status(
                action=action,
                guard_allowed=guard_result.allowed,
                observation=observation,
                retry_side_effect=state.retry_side_effect,
                backfill_side_effect=state.backfill_side_effect,
            ),
            reconcile_result=structured_log.reconcile_result(action, facts),
            outcome=state.outcome or "",
            duration_ms=duration_ms,
            step_number=trace.steps[-1].step_number,
            worker_id=_worker_id(checkpoint_record),
        )

        # Persist HumanRequest before the post-Apply checkpoint so a restart
        # can reload both the pause and the HITL ticket.
        _persist_hitl_if_paused()

        # Result checkpoint — after Apply + TraceStep. A crash immediately
        # after this write must still see the latest step. Does not close the
        # RETRY or BACKFILL dispatch window; record_retry_intent /
        # record_backfill_intent inside Execute do that.
        _persist()

        # Pause only after the accepted RETRY has been applied and the legacy
        # result checkpoint above has been written. The intent file written
        # before clearTaskInstances is not a pause.
        if pause_after_accepted_retry and _accepted_retry_ready_to_wait(
            action, guard_result.allowed, state
        ):
            hold_for_external = True
            return _finish()

        if hold_pending:
            hold_for_external = True
            return _finish()

        # --- 8. Show updated State for the next loop iteration ----------------
        if verbose:
            print("NEW STATE:", state)

    return _finish()


if __name__ == "__main__":
    from pipeline_reliability.__main__ import main

    main()
