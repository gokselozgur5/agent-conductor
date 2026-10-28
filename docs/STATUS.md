# Status

## Current result

The opt-in native workflow now runs assignment → restricted Claude writer →
source capture → isolated required checks → distinct read-only native verifier →
domain-controlled completion. The first actual Claude/Codex synthetic run reached
`completed` in 71.36 seconds; its source checkout stayed unchanged and SQLite
confirmed distinct worker/verifier identities with matching source evidence. A
second run reused the registered project and database and completed in 65.08
seconds with the same source identity and unchanged original checkout.

This is a working vertical workflow, not full v1 acceptance. The daemon currently
serves status while one plan runs and exits when that invocation ends. See
[NATIVE_WORKFLOW.md](NATIVE_WORKFLOW.md) for the operator command and limitations.
The native workflow shipped through PR #2 (issue #1 closed). The native
terminal/renewal race fix shipped through PR #5 (issue #4 closed).

An opt-in authenticated loopback operator board now displays authoritative task
state, checks and independent acceptance. Two Chrome probes verified rendering,
wrong-token refusal, injection-as-text, live refresh, stale-state indication and
disconnect. This read-only board is tracked in issue #3 on
`feat/3-operator-board`; its exact-branch CI is still pending.

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
- Authenticated loopback read-only board backed by the same service projections;
  credentials stay out of URLs, HTML, logs and browser storage.

## Evidence

The latest local workspace run passed 155 tests, formatting, strict Clippy and
credential/media scans. Evidence is kept privately under
`reviews/agent-conductor-runtime-20261028/operator-board-with-terminal-fix-workspace.log` and
`native-workflow-v1/`. The model-call cap is not an actual spend measurement.

Phase 1 has separate accepted evidence in [PHASE1_ACCEPTANCE.md](PHASE1_ACCEPTANCE.md).
Native workflow head `5520583` and race-fix head `277d653` passed macOS/Linux CI
before their respective merges. The board branch still requires its own exact-SHA
CI. Local results do not substitute for that remote check.

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
  checkpoints; source begin/bind crash reconciliation and durable review recovery.
- Continuous scheduling and complete operator start/pause/resume/cancel workflow.
- Operator board task controls and broader operator API; coalesced filesystem/Git hooks.
- Host protocol, Thor acceptance and supported host isolation beyond macOS checks.
- Remaining adapter negative tests, load/restore/redaction validation and soak.
  Seven-day soak cannot be claimed from a same-day run.

Prioritize a usable integrated workflow over more standalone infrastructure.
[RESPONSIBILITIES.md](RESPONSIBILITIES.md) defines the SOLID ownership boundaries;
[PHASE2_DESIGN.md](PHASE2_DESIGN.md) preserves the complete delivery scope.
