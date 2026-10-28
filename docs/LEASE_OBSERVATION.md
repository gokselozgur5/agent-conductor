# Task lease observations and verification provenance

Fresh task reads (`get-task`, `list-tasks`, `list-task-page`, and the board's task
API) include an optional `lease_observation` when the aggregate has an attempt.
The daemon samples its current boot generation and suspend-aware monotonic clock
after reading the aggregate. It uses the domain's `Lease::accepts_worker_mutation`
predicate; the browser does not reconstruct authority from wall-clock time.

The observation contains:

- `observed_at_ms`: wall-clock timestamp for display only.
- `daemon_boot_generation` and `boot_matches`: whether the persisted deadlines
  belong to the current daemon boot.
- `accepts_worker_mutation`: the lease predicate at that sample, not the result of
  validating a complete worker command.
- `worker_remaining_ms` and `owner_remaining_ms`: saturating differences from the
  sampled monotonic clock. They are null for prior-boot or ended leases. Worker
  remaining time can reach zero while the owner's recovery margin remains.

An active current-boot lease accepts the lease predicate only before both
worker and owner deadlines. Recovery-pending, ended, mismatched-boot and expired
leases do not. A failed/regressed host clock already fails closed through the
existing clock implementation. Wall-clock jumps do not affect the predicate.

## What the board means

The board displays lease epoch, recorded/current boot, separate remaining times,
observation timestamp, and an explicit valid-at-observation, expired,
recovery-pending, prior-boot or ended label. Task refresh obtains another sample;
the UI does not run an authoritative countdown. Connection loss labels displayed
data stale, and disconnect removes it. A missing observation on an attempted task
is shown as unavailable rather than inferred from persisted deadlines.

This is not OS process liveness, verified instruction freshness, proof of a fenced
execution, or permission to mutate. A subsequent command must still pass all
normal lifecycle, actor, version, source and ownership checks. No lease renewal,
state transition or audit write occurs merely from reading the observation.

Each required check now also shows its recorded actor and source identity. The
independent verdict's source is displayed separately from the task's bound source.
These are persisted verification records, not newly rerun checks or a summary of
all execution artifacts. Reported usage and deeper execution receipts remain
separate operator work.

## Historical responses and compatibility

Command outcomes and their idempotent replays omit `lease_observation`. They must
not acquire fresh-looking authority while returning an older task aggregate.
New readers accept older responses/outcomes that lack the field. No schema change
is required; creation responses naturally omit it because a new task has no
attempt. Existing response-size bounds remain in force.

## Validation

250 workspace tests, strict Clippy, formatting and release/credential checks pass.
Controlled observations cover the exact worker deadline, remaining owner margin,
wall-clock extremes, fail-closed monotonic exhaustion, boot mismatch, recovery,
ended/no-attempt and historical serialization. The aggregate remains unchanged.

The release daemon/CLI/HTTP/Chrome probe creates a claimed task through the public
API, observes its ten-second worker deadline expire while owner time remains,
and compares the unchanged aggregate. It also replays the original claim without
an observation, verifies recorded check/verdict provenance from a copied real
completed native task, and checks connection-loss and disconnect behavior.
No model is invoked. `lease-browser-v1/receipt.json` binds the tested binary hashes;
`lease-validation-manifest.json` binds source and supporting evidence under
`reviews/agent-conductor-runtime-20261028` in the operator workspace.
