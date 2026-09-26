# Engineering Casebook 3: lease takeover + reconcile after crash

## Failure

Worker A persists an `EMPTY` read, dispatches a retry, and crashes before it
can record the result. Its lease expires. Worker B takes over with a checkpoint
that still appears safe to retry.

A lease transfer alone prevents concurrent ownership; it does not make the
checkpoint true.

## Invariant

> A new lease generation fences the old worker and starts in
> `UNKNOWN / reconcile_required`, never from the prior worker's cached
> authorization evidence.

On takeover, the agent clears commit evidence and proposes `RECONCILE` as its
first action. The old generation fails Guard's current-lease check.

## Evidence in this repo

[`test_lease_takeover_fences_old_worker_and_reconciles_before_action`](../../tests/test_safety_contracts.py)
shows:

1. Worker B cannot claim an active lease.
2. Worker B can claim after expiry and receives a higher generation.
3. Worker A's generation is fenced.
4. Worker B's first decision is `RECONCILE`.
5. A discovered commit leads to `STOP_SAFE` with zero new retry calls.

## Why this matters

Crash recovery is not replay. A resumed or replacement worker must first learn
what the external system actually did during the ambiguity window.

## Production gap

This demo does not implement durable checkpoints, clock-skew handling, lease
heartbeats, or datastore isolation-level analysis. Those choices belong to the
real execution environment and must be failure-injected there.

