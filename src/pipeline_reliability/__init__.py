"""Pipeline Reliability Agent — public package exports.

The LLM may propose facts. Deterministic validation and Guard authorize
every side effect. The model never has action authority.
"""

from pipeline_reliability.adapters import MockPipelineAdapter
from pipeline_reliability.runner import AgentRunResult, run_agent
from pipeline_reliability.state import PipelineReliabilityState

__all__ = [
    "AgentRunResult",
    "MockPipelineAdapter",
    "PipelineReliabilityState",
    "run_agent",
]
