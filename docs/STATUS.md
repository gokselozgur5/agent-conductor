# Status

## Current result

The local operator launcher (`tools/conductor`) runs the authenticated board,
repository observation and opt-in durable workflow dispatcher. Accepted pinned
Claude and Codex adapters have completed real writer → immutable capture → checks
→ independent review workflows, including retained-work and fenced crash recovery.
The original checkout remains unchanged; task completion belongs to the domain.

Repository hooks now observe configured source repositories and owned attempt
trees, debounce changes, flush normal accepted terminal captures and admit one
approved subscribed workflow through atomic delivery acknowledgement. Advisory
snapshot references and explicit original-file instruction pins reach native
packets with separate trust boundaries. See [LOCAL_OPERATOR.md](LOCAL_OPERATOR.md),
[REPOSITORY_HOOKS.md](REPOSITORY_HOOKS.md) and
[NATIVE_WORKFLOW.md](NATIVE_WORKFLOW.md).

Task history exposes paginated audit metadata in the board and CLI. See
[TASK_HISTORY.md](TASK_HISTORY.md) for its read-only contract and validation.
Task lists use bounded pages after the required synthetic scale exposed an
all-list response failure; see [TASK_PAGINATION.md](TASK_PAGINATION.md).
Controlled cancellation now confirms group absence before reporting a stop; see
[PROCESS_STOP.md](PROCESS_STOP.md) for the observed race and refusal boundary.
Explicit dependencies now gate dispatch on independent completed acceptance and
can seed one successor from a selected accepted predecessor tree. Frozen source
binding and any wake acknowledgement share one transaction. Read-only reviewer
bundles include the starting manifest to verify preservation requirements.
A real two-task Claude/checks/Codex chain passed in 123.56 seconds, including
restart and a changed-input refusal before worker creation. Local validation
passed 195 tests and strict lint/build. See
[WORKFLOW_DEPENDENCIES.md](WORKFLOW_DEPENDENCIES.md) for limits and evidence.
Dependent-writer pause/restart/continuation now preserves initial task evidence
separately from retained unfinished work. The real continuation passed independent
review in 78.96 seconds; corrupt initial evidence was refused before another
attempt. The correction passes 198 local tests and strict lint/build.
External repository observation now uses native dirty hints with explicit 5-second
reconciliation, preserving full content validation and debounce/drain polling.
A controlled release comparison measured 31.05% → 2.88% of one CPU at idle;
linked-worktree event/restart/root-loss and 300-file drain checks passed.
See [REPOSITORY_HOOKS.md](REPOSITORY_HOOKS.md) for accounting and timing limits.
Task decisions now persist through the same versioned command/audit path and
block completion until resolved. The board exposes operator actions; subsequent
attempts receive labelled decision context. See [TASK_DECISIONS.md](TASK_DECISIONS.md)
for authority and compatibility. Initial operator questions and optional resolved
rationales can now be declared in plans before the first worker claim; open
questions retain the completion gate.
Native worker checkpoint capture, continuation and read-only board/CLI visibility
are validated locally: 215 tests, strict lint, real Chrome and a real Claude
pause/restart/continuation followed by independent Codex acceptance.
See [CHECKPOINTS.md](CHECKPOINTS.md) for claim and backpressure boundaries.
Explicit registered-worker reassignment now retains the immutable plan and prior
evidence through an atomic successor/queue/audit record. A real interrupted local
workflow completed under a distinct registered successor with independent review
in 80.47 seconds; 222 tests and strict lint pass. Cancellation during assignment
also rolls back. See [WORKFLOW_REASSIGNMENT.md](WORKFLOW_REASSIGNMENT.md).
All twelve remaining manual SQLite transaction owners now use rollback-on-drop.
Deterministic before/after controls reproduce and correct partial writer state;
execution-owner and lost-COMMIT-response controls cover the important boundaries.
See [STORE_TRANSACTIONS.md](STORE_TRANSACTIONS.md) for the audit and evidence.
Opt-in outer-workspace discovery now freezes bounded untrusted advisory sources
into native plans without adding agent filesystem capabilities. See
[SCOPED_ADVISORY.md](SCOPED_ADVISORY.md) for selection and trust boundaries.
Retained nonterminal workspaces now publish explicitly external observations after
ownership loss, without granting the former worker a lease or changing recovery
acceptance. See [RETAINED_OBSERVATION.md](RETAINED_OBSERVATION.md).

Event routing uses literal, case-sensitive prefixes and rechecks current filters
before delivery leasing or workflow wake. See [EVENT_ROUTING.md](EVENT_ROUTING.md)
for subscription updates and the in-flight acknowledgement boundary.

The board and CLI now expose bounded project, agent, event, subscription and
delivery metadata pages. See [OPERATOR_INVENTORY.md](OPERATOR_INVENTORY.md) for
snapshot semantics, omitted payloads/tokens and pagination boundaries.
Fresh task reads now include daemon-clock lease observations; the board shows
worker expiry separately from owner margin and displays check/verdict provenance.
See [LEASE_OBSERVATION.md](LEASE_OBSERVATION.md) for snapshot/authority limits.
Adapter-reported terminal usage is now retained per invocation and visible through
the inventory; missing reports remain unknown. See [REPORTED_USAGE.md](REPORTED_USAGE.md).
The working local workflow is not full v1 acceptance. Remaining work includes
deeper execution evidence, usage-loss recovery, abnormal ownership recovery,
host protocol/Thor/Linux isolation,
load/restore/redaction validation and seven-day soak. Issue #20 retains an
unresolved historical supervision deadline; do not infer a cause from green tests.

The sections below preserve earlier acceptance milestones. Their counts and
then-pending items are historical; current hook coverage is maintained in
REPOSITORY_HOOKS.md, and current product limits are listed above.

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
