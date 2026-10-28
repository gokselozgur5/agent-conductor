# Status

## Current result

The opt-in native workflow now runs assignment → restricted Claude writer →
source capture → isolated required checks → distinct read-only native verifier →
domain-controlled completion. The first actual Claude/Codex synthetic run reached
`completed` in 71.36 seconds; its source checkout stayed unchanged and SQLite
confirmed distinct worker/verifier identities with matching source evidence. A
second run reused the registered project and database and completed in 65.08
seconds with the same source identity and unchanged original checkout.

This is a working vertical workflow, not full v1 acceptance. The daemon supports both one-shot `--run-plan` and an opt-in persistent
`--dispatch` queue. CLI/board submission persists approved plans before execution. See
[NATIVE_WORKFLOW.md](NATIVE_WORKFLOW.md) for the operator command and limitations.
The native workflow shipped through PR #2 (issue #1 closed). The native
terminal/renewal race fix shipped through PR #5 (issue #4 closed).

An opt-in authenticated loopback operator board now displays authoritative task
state, checks and independent acceptance. Two Chrome probes verified rendering,
wrong-token refusal, injection-as-text, live refresh, stale-state indication and
disconnect. The read-only board shipped through PR #6 after exact-head macOS/Linux CI.
Version-bound ready/pause/resume/cancel controls are implemented under issue #7
and shipped through PR #8 after macOS/Linux CI, real SQLite/HTTP tests and an
isolated Chrome run. Atomic verification/source publication shipped through PR #10; its fault
regression preserves task state after a late bind failure. Durable dispatch shipped through PR #12 after exact-head CI. Fenced-workspace
continuation shipped through PR #14 after exact-head CI. Observed restricted
writer stop evidence under issue #15 passed real writer-pause/restart/continuation
acceptance and shipped through PR #16 after exact-head CI.

## Implemented

- Transactional SQLite lifecycle, audit/idempotency, ownership epochs, leases,
  restart recovery state and versioned local socket/CLI.
- Frozen attempt contracts, explicit capabilities and pinned native CLI adapters.
- Durable runner preparation/acknowledgement/release, independent watchdog,
  bounded output, timeout/cancellation and uncertain-exit reconciliation.
- Private dirty Git snapshots and attempt source capture, including new files and
  baseline deletions. The original checkout is never reset, cleaned or merged.
- Restricted writer source finalization and source-bound verification state.
- Isolated exact-argv checks with persisted receipts and concurrent cancellation
  rejection; standalone `conductorctl run-check` is also available.
- Distinct read-only native reviewer, strict task/source-bound decision parsing,
  source revalidation and existing completion invariants.
- Semantic event publication/subscriptions, bounded delivery and claim/ACK state.
- Authenticated loopback board backed by the same service projections and commands;
  credentials stay out of URLs, HTML, logs and browser storage.

## Evidence

The latest local workspace run passed 161 tests, formatting, strict Clippy and
credential/media scans. Evidence is kept privately under
`reviews/agent-conductor-runtime-20261028/durable-recovery-workspace.log` and
`native-workflow-v1/`. The model-call cap is not an actual spend measurement.

Phase 1 has separate accepted evidence in [PHASE1_ACCEPTANCE.md](PHASE1_ACCEPTANCE.md).
Native workflow head `5520583` and race-fix head `277d653` passed macOS/Linux CI
before their respective merges. Board head `2937f43` also passed both hosts and merged at `3fc64c7`.
Control head `02e6954` passed both hosts and merged at `a761d94`.
Atomic-source head `f461560` passed both hosts and merged at `2eb1f61`.
Durable-dispatch head `eede6c9` passed both hosts and merged at `82496d3`.
Continuation head `61de159` passed both hosts and merged at `cdd32b9`.
Writer-stop head `a7baea7` passed both hosts and merged at `853899e`.

## Trust boundaries

The local socket assumes a trusted local operator; role registration is not
per-agent authentication. Native CLI harnesses are trusted for authentication and
model transport; their model-facing tools are restricted by accepted profiles.
The normal file-only writer exit fence depends on that pinned capability boundary,
not a generic proof that arbitrary command-capable descendants are dead.
Command-capable writers remain refused. Checks currently require macOS Seatbelt.
See [ADAPTER_CONFORMANCE.md](ADAPTER_CONFORMANCE.md) and [SANDBOX.md](SANDBOX.md).

## Remaining full-product work

- Native interruption/recovery acceptance and automatic continuation from retained
  checkpoints; recovery of retained fenced captures and durable review recovery.
- Complete recovery for interrupted writers without verified fences; explicit
  continuation from fenced retained attempts is available on the current branch.
- Broader operator API and coalesced filesystem/Git hooks.
- Host protocol, Thor acceptance and supported host isolation beyond macOS checks.
- Remaining adapter negative tests, load/restore/redaction validation and soak.
  Seven-day soak cannot be claimed from a same-day run.

Prioritize a usable integrated workflow over more standalone infrastructure.
[RESPONSIBILITIES.md](RESPONSIBILITIES.md) defines the SOLID ownership boundaries;
[PHASE2_DESIGN.md](PHASE2_DESIGN.md) preserves the complete delivery scope.

## Durable dispatch acceptance

A real synthetic plan was submitted twice identically while dispatch was disabled,
then the daemon was stopped and restarted with dispatch enabled. The queued plan
survived and completed through Claude, checks and pinned Codex in 77.46 seconds;
SQLite confirmed the task completed. No answer file appeared in the original
source checkout. Evidence: `queued-workflow-v2/receipt.json` in the private review
root. The initial preflight refused a changed Codex symlink before any model call;
the successful run used the retained 0.163.0 binary with its verified hash.
An isolated Chrome run also submitted a local plan file and verified queued state
and absence of the stored plan body from API output (`operator-queue-v1`).

See [WORKFLOW_DISPATCH.md](WORKFLOW_DISPATCH.md). Dispatch `finished` is not task
acceptance; interrupted claims become `needs_recovery`, retaining artifacts.
Unfenced mid-writer crash recovery remains refused. Opt-in receipt-based automatic continuation is locally accepted below.

## Retained-work continuation acceptance

A synthetic native writer created answer.txt; the daemon was interrupted during
verification after writer fencing, then restarted. An explicit version-bound
continuation created a new attempt and higher epoch from a fresh copy of the
retained workspace, reran checks and independent review, and completed in 113.73
seconds from the continuation request. Old files remained unchanged; stale-version
and terminal-task continuation requests were refused. Evidence:
`workflow-continuation-v1/receipt.json`. The real probe preceded one additional
retained-packet attempt-ID consistency guard; the final native lifecycle regression
and strict Clippy passed with that guard included. Full workspace tests passed157.

This proves fenced-workspace continuation through the CLI. The board continuation button also passed an isolated Chrome acceptance on a
copy of a genuinely rejected, fenced workflow; it queued a version-bound
continuation while model dispatch remained disabled (`operator-continuation-v1`). Mid-writer termination without a confirmed
fence remains refused, not silently treated as recoverable.

## Observed writer-stop validation

Cancellation output is now retained and checked only after the wrapper settles.
A valid restricted file-only envelope can establish the existing capability-based
write fence; malformed/truncated/command-capable/fake cases remain blocked. The
expanded native lifecycle regression verifies that stop admission does not create
a successful candidate, and controlled process tests verify bounded captured
stdout survives cancellation.159 workspace tests and strict Clippy pass.

The first real writer-pause probe established fencing with no successful exit code,
restarted and continued retained files, then passed checks but was correctly
rejected by the independent verifier: its synthetic objective demanded historical
Write/read ordering that source-only review could not prove. The task remained
verifying, not completed. Evidence: `writer-pause-v1/rejected-receipt.json`. A new
fixture uses explicitly source-verifiable final-state requirements; no acceptance
gate or prior verdict was overridden.

The replacement writer-pause fixture passed: a running writer was paused after
creating answer.txt, its observed stop was fenced without a successful exit code,
and a daemon restart followed by explicit retained-work continuation completed
through independent verification in65.91seconds. New attempt/epoch, unchanged old
files, copied retained output and stale/terminal refusals were verified. Evidence:
`writer-pause-v2/receipt.json`. The first rejected fixture remains preserved.


## Durable crash receipt acceptance

The trusted wrapper now publishes private sanitized terminal evidence outside the
writer tree even when its controller pipe is gone. With `--dispatch --recover-fenced`,
a matching receipt admits one automatic retained-work continuation. Missing or
invalid evidence remains blocked; explicit operator pause/cancel remains respected.

A real synthetic writer created answer.txt, then the test killed only its daemon
with SIGKILL. Restart reconciled the independent wrapper receipt and automatically
created a fresh attempt, completed required checks and independent review in62.17s.
Old bytes and the fresh retained copy were verified; terminal continuation was
refused and the durable automatic-recovery count was exactly1. Evidence:
`writer-crash-v2/receipt.json`. The first harness run mistook a stale socket path
for server readiness; its artifacts remain in `writer-crash-v1`. The replacement
checks an actual socket request before proceeding. No task verdict was overridden.

161 workspace tests, strict Clippy, formatting and credential/media checks passed.
Receipt negatives cover mismatch, hardlinks, symlinks, permissions, oversize and
no-overwrite publication. Storage regression proves stale-boot refusal and the
one-automatic-retry limit across another restart; explicit continuation remains
available. This slice still requires exact-head CI. Generic hostile-process
fencing, hooks, other-host isolation and full-product acceptance remain open.


## Repository observation integration

The current hooks branch provides opt-in daemon `--watch-config`, bounded
read-only content observation, quiet/max-latency debounce and atomic publication
of at most256 events plus the advanced baseline. A real daemon/CLI/Git probe passed
burst coalescing, path routing, source-echo suppression, no raw-content payload,
updates/deletion, ignore exclusion, changes across restart and HEAD events. See
[REPOSITORY_HOOKS.md](REPOSITORY_HOOKS.md) for exact limits and current evidence.

166 full workspace tests passed after a stop-fixture readiness correction; all
prior failures remain recorded. The controlled delayed-start probe explains the
fixture's missed-renewal assumption, not every historical timeout. The earlier
socket-permit deadline remains unresolved under issue20. Issue19 remains open:
owned-attempt attribution and eligible bounded wake are not yet implemented.
No model calls were made for watcher acceptance.

Explicit instruction integration now carries ordered approved original-file pins
through native plans, including exact outer-workspace files without parent tool
access. Active tasks are blocked on changed pins; pause/cancel and retained files
are preserved, with block persistence across restart. Continuation revalidates
original pins. All 174 workspace tests pass, including JSON prompt-expansion
refusal and durable invalidation. This does not complete automatic discovery,
attribution or wake. The synthetic native instruction workflow completed in
301.89 seconds: queued submission survived restart, duplicate submission was
idempotent, Claude produced the required file, checks passed and independent
Codex review accepted. Original project and instruction content stayed unchanged;
no policy copy appeared in work/source outputs. The persisted writer packet
contains only workspace plus exact approved-file read roots. Reviewer pin
construction was source-inspected; its packet is not separately persisted.
Evidence: private `instruction-workflow-v1/receipt.json` and `packet-audit.json`.
Exact integration commit CI 40693075343 passed on both supported CI hosts.


## Watcher operational health

The service health response and operator board now report observation state.
A requested watcher starts degraded until its first successful scan round, and
failure, task abort or unexpected drop cannot leave it reporting healthy.
Graceful shutdown reports stopped. Disabled observation remains explicit.
The daemon remains available for diagnosis and task control after watcher failure.

The shared-service failure/abort/shutdown regression passed. The isolated real
daemon and Chrome probe displayed `Degraded · Watcher: failed`, retained task
rendering and live refresh, refused unauthenticated access and preserved injection,
credential-storage and disconnect boundaries. Evidence: private
`watcher-health-v1/receipt.json`. This reports watcher process state, not bounded
scan freshness. Owned-attempt attribution and bounded wake remain outstanding.
