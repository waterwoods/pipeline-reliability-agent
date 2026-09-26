from concurrent.futures import ThreadPoolExecutor

from pipeline_reliability.agent import Agent, SyntheticAdapter, decide, guard
from pipeline_reliability.coordination import LeaseStore
from pipeline_reliability.model import (
    Action,
    CommitStatus,
    EffectStatus,
    IncidentState,
)


def test_stale_evidence_after_retry_forces_reconcile_and_never_retries_twice() -> None:
    state = IncidentState("i-1", "orders", "run-1")
    leases = LeaseStore()
    claim = leases.claim("i-1", "worker-a", now=0, ttl=30)
    assert claim is not None
    adapter = SyntheticAdapter()

    Agent(adapter, leases).run(state, claim, now=0)

    assert adapter.retry_calls == 1
    assert state.commit_status == CommitStatus.COMMITTED
    assert state.outcome == "STOP_SAFE"
    assert state.retry_count == 1


def test_multi_worker_claim_is_atomic() -> None:
    leases = LeaseStore()

    def attempt(worker: int):
        return leases.claim("same-incident", f"worker-{worker}", now=0, ttl=30)

    with ThreadPoolExecutor(max_workers=16) as pool:
        claims = list(pool.map(attempt, range(16)))

    winners = [claim for claim in claims if claim is not None]
    assert len(winners) == 1
    assert winners[0].generation == 1


def test_lease_takeover_fences_old_worker_and_reconciles_before_action() -> None:
    leases = LeaseStore()
    old_claim = leases.claim("i-2", "worker-a", now=0, ttl=10)
    assert old_claim is not None
    assert leases.claim("i-2", "worker-b", now=5, ttl=10) is None

    new_claim = leases.claim("i-2", "worker-b", now=11, ttl=10)
    assert new_claim is not None and new_claim.takeover
    assert not leases.is_current(
        "i-2", "worker-a", old_claim.generation, now=11
    )

    state = IncidentState(
        "i-2",
        "orders",
        "run-2",
        commit_status=CommitStatus.EMPTY,
        commit_evidence_epoch=0,
    )
    adapter = SyntheticAdapter()
    adapter.committed = True
    trace = Agent(adapter, leases).run(state, new_claim, now=11)

    decisions = [event.action for event in trace if event.phase == "Decide"]
    assert decisions[0] == Action.RECONCILE
    assert adapter.retry_calls == 0
    assert state.outcome == "STOP_SAFE"


def test_guard_blocks_retry_when_side_effect_is_unknown() -> None:
    state = IncidentState(
        "i-3",
        "orders",
        "run-3",
        commit_status=CommitStatus.EMPTY,
        commit_evidence_epoch=0,
        retry_effect=EffectStatus.UNKNOWN,
        reconcile_required=True,
    )
    leases = LeaseStore()
    claim = leases.claim("i-3", "worker-a", now=0, ttl=30)
    assert claim is not None

    proposed = decide(state)
    assert proposed.action == Action.RECONCILE
    forced_retry = type(proposed)(Action.RETRY, "unsafe override attempt")
    verdict = guard(
        state, forced_retry, claim=claim, leases=leases, now=0
    )
    assert not verdict.allowed
    assert "unreconciled" in verdict.reason
