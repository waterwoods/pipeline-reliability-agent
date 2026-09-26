# Engineering Casebook 1: stale evidence after retry

## Failure

At epoch 0, a read reports `EMPTY`. The worker dispatches `RETRY`, the
warehouse commits, and the transport response times out. Reusing the epoch-0
read would incorrectly authorize a second retry.

The dangerous mistake is interpreting “I did not receive success” as “the side
effect did not happen.”

## Invariant

> Any mutation attempt advances the evidence epoch. Pre-mutation evidence can
> never authorize a later mutation.

Before dispatch, the worker records:

- `retry_effect = UNKNOWN`
- `reconcile_required = True`
- `commit_status = UNKNOWN`
- `commit_evidence_epoch = -1`

The next decision is therefore `RECONCILE`, regardless of the timeout.

## Evidence in this repo

[`test_stale_evidence_after_retry_forces_reconcile_and_never_retries_twice`](../../tests/test_safety_contracts.py)
runs the whole loop. The synthetic adapter commits and then raises
`TimeoutError`. The assertions prove:

- exactly one retry call;
- reconciliation observes `COMMITTED`;
- the terminal outcome is `STOP_SAFE`.

The readable execution record is
[`examples/stale-retry.trace.jsonl`](../../examples/stale-retry.trace.jsonl).

## Why this matters

Exactly-once delivery cannot be created by hopeful client-side retries.
Uncertainty must be represented explicitly and resolved against the system of
record.

## Production gap

The public example records intent in memory. A deployed design needs durable,
atomic intent persistence, downstream idempotency keys, and recovery tests
against the actual orchestrator and warehouse APIs.

