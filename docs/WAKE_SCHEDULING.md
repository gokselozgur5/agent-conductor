# Event wake scheduling

A committed event with matching deliveries sends a coalesced in-process scheduling
hint. Committed nonempty repository-observation batches, subscription changes,
workflow admission and successful continuation/recovery requeue also signal the
same channel. Replayed event publication and failed publication do not signal.
Hints never contain event payloads or credentials and cannot grant execution.

The dispatcher subscribes before its first scan and marks notifications observed
before each scan, so a commit during the scan-to-wait interval remains visible.
It rechecks the existing atomic SQLite claim, including subscription matching,
source echo, dependency acceptance, expiry, generation, retries and exclusive
running ownership. Only that transaction can admit a wake. Notifications coalesce
rather than accumulating an unbounded queue. Completion immediately permits the
next scan; workflows still execute sequentially.

The 500 ms periodic fallback remains for time-based eligibility, external or
missed hints and cancellation after commit but before notification. A process
restart always scans durable state; notification state is not persisted. Separate
Store instances do not share a channel. Signals are a latency optimization, not
a recovery or durability mechanism. Filesystem debounce and vendor process startup
remain separate latency stages. Shutdown takes priority at dispatch/wait boundaries.

## Validation

Tests check successful admission and continuation, rollback, replay, coalescing,
the publication-before-wait race, and atomic repository observation behavior.
The existing eligibility and fencing tests remain unchanged.

The initial release probe used 256 queued event-gated synthetic stored plans,
80 distinct publications and randomized inter-publication delay. Those plans
intentionally fail decoding after atomic admission, so no task or vendor process
runs. Initial p95 publication-to-observed-ACK was 538.70 ms (511.85 ms after the
publication response), above the <500 ms target. An independent fixture containing
100 projects, 10,000 tasks and 100,000 historical events measured 513.71 ms.
These timings include CLI startup/roundtrip and up to 2 ms observation polling.
No filesystem watcher or concurrent board traffic was active; this is not a
model workflow, saturated dispatcher, host suspension or seven-day soak result.

Exact before/after receipts and binary hashes are retained under the operator's
`reviews/agent-conductor-runtime-20261028/` evidence directory.

With notification scheduling, the same probes measured p95 39.17 ms and 42.89 ms,
respectively (maximum 52.89 ms and 47.12 ms). Each run acknowledged all 80 distinct
events exactly once, with no model execution. All 10,000 background task aggregates
remained byte-identical to the copied source fixture. These are local observations
on the tested release, not universal latency bounds.

## Maintenance lock contention

Instruction invalidation and lease expiry discovery must not retain the shared
mutation mutex while loading and decoding inactive tasks. Candidate discovery is
advisory. Each active candidate is reloaded under mutation ownership before its
current state, lease, execution fence or instruction binding can authorize a
lifecycle action. A task activated after discovery is handled by a later sweep;
normal launch-time instruction and ownership checks remain in force.

Regression coverage checks that inactive discovery can finish while another
mutation holds the mutex, and that a discovered expired lease cannot override a
renewal committed before candidate reconciliation. Existing recovery and
instruction invalidation tests remain required.

A local 10,000-task diagnostic found multi-second maintenance lock holds and
publication waits. With narrower lock scope, an 80-event combined-load comparison
observed median wake time decrease from 805.6 to 34.6 ms and p95 from 3013.8 to
1518.7 ms. Both measurements include client overhead on the same busy host and
are observations, not isolated cross-host performance guarantees. The 500 ms wake
and 100 ms board p95 targets still failed; this change does not close performance
acceptance. No durability or timing threshold was relaxed.
