"""Run a local mock-adapter demo. No cloud account required.

    python -m pipeline_reliability
"""

from __future__ import annotations

from pipeline_reliability.adapters import BQ_ALREADY_SUCCEEDED, MockPipelineAdapter
from pipeline_reliability.checkpoint import DEMO_CHECKPOINT_PATH, load_checkpoint
from pipeline_reliability.observability import RunTrace
from pipeline_reliability.runner import run_agent
from pipeline_reliability.state import PipelineReliabilityState


def _fresh_demo_state() -> PipelineReliabilityState:
    return PipelineReliabilityState(
        pipeline="daily_orders",
        error="",
        scenario=BQ_ALREADY_SUCCEEDED,
    )


def main() -> None:
    checkpoint_path = DEMO_CHECKPOINT_PATH
    resume_trace: RunTrace | None
    if checkpoint_path.is_file():
        resumed = load_checkpoint(checkpoint_path)
        if resumed.awaiting_human or resumed.outcome is None:
            state = resumed
            resume_trace = None
        else:
            state = _fresh_demo_state()
            resume_trace = RunTrace()
    else:
        state = _fresh_demo_state()
        resume_trace = RunTrace()
    result = run_agent(
        state,
        adapter=MockPipelineAdapter(),
        checkpoint_path=checkpoint_path,
        trace=resume_trace,
        verbose=True,
    )
    print("FINAL OUTCOME:", result.state.outcome)
    print("AGENT RUN ID:", result.agent_run_id)
    print("CHECKPOINT:", checkpoint_path)
    print("STEPS:", [step.action for step in result.trace.steps])
    print("GUARD BLOCKS:", result.metrics.guard_block_count)
    print(
        "Honesty: this demo is SYNTHETIC (MockPipelineAdapter). "
        "It is production-style, not production-ready."
    )


if __name__ == "__main__":
    main()
