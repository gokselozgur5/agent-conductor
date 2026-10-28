# Response encoding and supervision deadline acceptance

Unix control responses now transfer ownership to a blocking encoding task before
JSON serialization. A process-wide semaphore admits at most four encoding jobs,
including jobs waiting in Tokio's blocking pool. The closure owns its permit: an
aborted socket task cannot release capacity while its encoder is still executing.
Cancellation while waiting for admission never launches the queued encoder.
Encoding does not mutate task or database state.

The socket write deadline starts after encoding. It remains five seconds in
production and 40 ms in the unread-response regression. Oversized frames still
become the same bounded structured error; even the error must fit, including its
newline. The response is moved, and oversized fallback replaces the payload on
the blocking thread instead of cloning the oversized response on the runtime.

This is not a total request deadline or a hard CPU/memory limit. Service execution,
encoding admission and encoding time are separate from write time. Running
blocking jobs are not forcibly aborted, and runtime shutdown may wait for them.
Connection admission and encoding admission remain separate bounds; no new
unbounded stream of blocking jobs is created when clients disconnect.

## Evidence for issue #20

The original socket assertion was reproduced on unchanged main `9172b14`: 15 of
32 concurrent daemon-suite processes exceeded its one-second observation. A
separate instrumented source/target measured synchronous serialization before
the write timer. In failing samples one serialization took 737–1324 ms wall time;
all ten instrumented failures subsequently released both permits and answered
health. This identifies a reproducible mechanism, not retrospective proof of the
original machine scheduling or a universal performance measurement.

Offloading alone did not make the original combined-phase timing assertion pass:
21 of 32 socket samples still failed under the same deliberate load. The test now
waits for one actual response byte from each socket, within a separate three-second
readiness bound, before applying the unchanged one-second write-recovery bound.
The remaining large response is deliberately unread. All 32 complete daemon suites
passed after separating these phases. This does not promise one-second total
response latency under arbitrary system load.

A current-thread regression blocks an encoder and proves timer progress,
retention of capacity after caller cancellation, refusal to start another job
until capacity returns, cancellation of queued work and successful later reuse.
Separate negative controls restore inline execution and disable the effective
write timeout; the corresponding regressions must fail. This prevents phase
separation from silently removing either protection.

The earlier worker-start failures have a different established cause. The
`readiness-v1` controlled probe delayed startup by 300 ms with a 150 ms heartbeat:
no renewal correctly stopped the worker before readiness; renewal until actual
progress allowed startup and subsequent controlled cancellation. Current runner
tests maintain the lease during bounded readiness, then deliberately test missed
renewal, parent loss or cancellation. Their no-later-write assertions remain.
See the historical evidence in [REPOSITORY_HOOKS.md](REPOSITORY_HOOKS.md).

Private evidence: `reviews/agent-conductor-runtime-20261028/`, including
`issue20-load-diagnostic-manifest.json`, `encoding-v2-load32-receipt.json`,
`encoding-negative-*` and `encoding-validation-manifest.json`. No live user task
was used for these stress fixtures. Full product, host isolation and operational
soak acceptance remain separate.
