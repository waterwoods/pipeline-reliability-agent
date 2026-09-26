from __future__ import annotations

from pipeline_reliability.agent import Agent, SyntheticAdapter, trace_as_jsonl
from pipeline_reliability.coordination import LeaseStore
from pipeline_reliability.model import IncidentState


def main() -> None:
    state = IncidentState("incident-42", "daily_orders", "scheduled-2026-09-25")
    leases = LeaseStore()
    claim = leases.claim(state.incident_id, "demo-worker", now=0, ttl=30)
    assert claim is not None
    adapter = SyntheticAdapter()
    trace = Agent(adapter, leases).run(state, claim, now=0)
    print(trace_as_jsonl(trace))
    print(f"\noutcome={state.outcome} retry_calls={adapter.retry_calls}")


if __name__ == "__main__":
    main()
