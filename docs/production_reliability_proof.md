# Production reliability proof

Sanitized summary of five lab incidents. The agent was called over HTTP. It
used local Airflow and disposable BigQuery tables. The crash incident also
used durable Postgres. This is a lab proof of the control plane. It is not a
fully production-deployed enterprise system.

Raw run artifacts are not published here.

## Safety model

```text
State → Decide → Guard → Execute → Observation → Apply
      → Persist → Reconcile → Continue / Pause / Finish
```

- An LLM may advise. It does not authorize a mutation.
- Guard authorizes dangerous actions.
- `UNKNOWN` is not the same as not executed.
- Resume continues the same `agent_run_id`. It does not replay the mutation.
- After an uncertain side effect, the next step reads fresh external truth.
- Existing correct data is not blindly rewritten.

## Results

| Incident | Scenario | Result |
|---|---|---|
| 1 | Airflow timeout, one guarded retry, fresh warehouse read | **PASS** |
| 2 | Process kill after retry dispatch, resume the same run | **PASS** |
| 3 | Orchestrator and warehouse job succeed, required row missing | **GAP** |
| 4 | Late partition beside two already-correct dates | **PARTIAL** |
| 5 | Renamed column in an incoming file, before any load | **PASS** (safety property) |

## Incident 1 — Airflow timeout, one retry

**Question.** When an Airflow task fails with a timeout, can the agent clear
it once and finish only after a fresh BigQuery read?

**What happened.** The probe task failed on the first try. Guard allowed one
retry. The run paused while the new attempt executed, then resumed on the same
`agent_run_id`. Completion followed a fresh warehouse read that reported
success.

**Result.** PASS.

**Limit.** Completion accepted a successful job. A row count was not required
for that `CREATE TABLE AS SELECT` job.

## Incident 2 — Crash after retry dispatch

**Question.** If the worker dies after a retry has been sent and before the
post-mutation checkpoint is written, will resume issue a second clear?

**What happened.** The agent persisted `UNKNOWN` for the retry side effect.
The process was then SIGKILLed. A new process resumed the same
`agent_run_id`. The first action was a fresh orchestrator read. Airflow showed
the attempt had advanced. The agent did not send a second clear. It read the
new BigQuery job and finished.

**Result.** PASS.

**Limit.** The trace has no `RETRY` step, because Apply had not run before the
kill. The external proof of one retry is the Airflow attempt number, plus the
fresh warehouse read.

## Incident 3 — Execution succeeded, data did not

**Question.** If Airflow is green and the BigQuery job is done with no error,
but a required row is missing, will the agent refuse to finish?

**What happened.** The task wrote one marker. The contract required two. The
agent checked job execution, saw success, and finished. It did not compare
row contents.

**Result.** GAP.

**Lesson.** Execution success is not data correctness. The warehouse check can
tell a failed job from a successful job. It cannot tell a successful job that
wrote the wrong rows.

## Incident 4 — Late partition

**Question.** When two daily partitions are already correct and a middle day
arrives late, will the agent rewrite the correct days or fill only the empty
one?

**What happened.** Before the late day arrived, the agent waited and then
asked a human. It did not backfill. After the source row appeared, a new run
copied only the empty target partition. The two correct dates kept their
existing rows.

**Result.** PARTIAL.

**Limit.** The already-correct dates stayed marked arrived. They were not
marked validated. The mixed range therefore stopped safe and did not finish
automatically.

## Incident 5 — Schema drift

**Question.** If an incoming file renames a required column, will the agent
load it?

**What happened.** A schema check failed before any warehouse write. The
expected header and the observed header differed by one renamed column. The
agent classified the drift and requested human review. The drifted file was
not loaded, cleared, or finished. A corrected file was checked on a new
orchestrator run.

**Result.** PASS on the safety property.

**Limit.** This incident did not perform a real BigQuery load. Same-run resume
after human repair is not supported, so the repaired file was not continued
on the original `agent_run_id`.

## What is proven

- One guarded Airflow clear, then completion only after a fresh warehouse read.
- Durable state survives a real process kill. Resume does not treat `UNKNOWN` as permission to clear again.
- A late empty partition can be copied without rewriting partitions that already have rows.
- A renamed header is classified and held for a human instead of being loaded.
- The semantic boundary fails closed in the evidence: a done job with the wrong rows was accepted as success.

## Known gaps

1. Semantic data-quality checks. Job success is not compared with expected row contents.
2. Mixed-range completion. A late partition can be validated while already-correct partitions stay merely arrived, so the range does not finish.
3. Same-run resume after human repair.
4. Operational production layer: public or cloud hosting, a managed database, secrets management, metrics, alerts, and event-driven resume.

The local Docker follow-up is a separate run. It does not change these
verdicts. See [deployment demo](deployment_demo.md).
