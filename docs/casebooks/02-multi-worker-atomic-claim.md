# Engineering Casebook 2: multi-worker atomic claim

## Failure

An alert fan-out, duplicate queue delivery, or slow worker can cause multiple
workers to handle the same incident. A “check then set” claim is racy: both
workers may observe no owner and both may dispatch a retry.

## Invariant

> At most one unexpired lease generation owns an incident, and every side
> effect is fenced by that generation.

[`LeaseStore.claim`](../../src/pipeline_reliability/coordination.py) places the
read and write under one lock to model compare-and-set semantics. Guard calls
`is_current` immediately before execution; ownership checked earlier in the
loop is not sufficient.

## Evidence in this repo

[`test_multi_worker_claim_is_atomic`](../../tests/test_safety_contracts.py)
starts 16 concurrent claim attempts for one incident. Exactly one succeeds.

The code is intentionally small enough to expose the contract rather than hide
it behind infrastructure.

## Why this matters

Idempotent business logic does not remove the need for coordination. Without a
single active owner and fencing, a delayed worker can still act after a newer
worker has taken over.

## Production gap

A Python lock coordinates one process only. Production requires a transactional
shared store such as a database row with compare-and-set/update predicates,
server time, lease renewal, and a fencing token checked by the side-effecting
boundary.

