# Status

## Current gate

Phase 1 is accepted for the documented control-core scope. Independent review
accepted the lifecycle fixes at `5d26e14` and the transport correction at
`751ba5b`; no reproduced findings remain open. Published `bebddb9` passed both
macOS and Linux jobs in [CI run 40646746932](https://github.com/gokselozgur5/agent-conductor/actions/runs/40646746932).
Local formatting, strict Clippy, and three full runs of 97 tests also passed.
See [PHASE1_ACCEPTANCE.md](PHASE1_ACCEPTANCE.md) for the exact evidence and deferred
v1 requirements. Subsequent commits must pass their own exact-SHA CI.

## Implemented

- Seven-crate Rust workspace, versioned SQLite migrations and read-only projections.
- Local Unix socket, exclusive runtime lock, bounded requests/responses, connection
  timeouts, completed-handler draining, orderly shutdown, human and JSON CLI.
- Transactional lifecycle/audit/idempotency; frozen attempt contracts and epochs.
- Restart boot generation and recovery heartbeat; automatic periodic
  expiry reconciliation preserving durable task state.
- Semantic event deduplication, source attribution, bounded delivery, expiring
  claim tokens and atomic consumer-state/ack transactions.
- Independently attributed checks/verdicts and completion invariants. The privileged
  source-binding request is reachable through the CLI and real daemon socket.
- Fake effect intent/claim/observation/completion crash harness and an internal
  audited lifecycle port for inventory contradictions.

## Authority and evidence limits

The socket is mode `0600` and assumes a trusted local operator. Registered role
checks are not per-agent authentication. Phase 1 sources/checks are labelled
fake/operator inputs; there is no real source capture, check executor, external
write fence, worker launcher or live inventory adapter. The daemon automatically
reconciles expired leases; fake external inventory is exercised by the testkit,
not discovered or executed by a production adapter.

The Python spike remains separate behavior evidence. It is not the production
runtime and there is no migration of real user task state to perform.

## Active work

The operator authorized continuing to a usable complete Conductor on 2026-10-28.
Phase 2 requirements/design are recorded in [PHASE2_DESIGN.md](PHASE2_DESIGN.md).
Runtime contracts, bounded process supervision, and durable execution records
are implemented. The independent `conductor-runner` wrapper acknowledges the
packet before accepting a matching release, rejects late/mismatched renewals,
and stops controlled process groups on parent loss or watchdog expiry. Its
protocol tests cover pre-release refusal, parent loss, renewal and spawn failure.
This is not a hostile-process sandbox: wrapper SIGKILL, escaping descendants and
normal-exit orphan fencing still need stronger isolation and recovery evidence.
The service now connects fresh durable preparation and recorded acknowledgement
to wrapper release under the lifecycle mutation lock. Launch and renewal check
the authoritative monotonic lease; replay and mismatched connection hashes are
refused. This internal port accepts controlled fake adapters only. Service tests
use a controlled protocol peer; real wrapper watchdog tests remain in runtime.
Release and renewal now carry an absolute suspend-aware host deadline derived
conservatively from the current service lease. Expired grants cannot launch or
revive a worker; heartbeat activity alone cannot extend that deadline. Expiry
reconciliation blocks tasks with unfenced execution records instead of requeuing
them, and new claims refuse those records even after an operator resumes a task.
An ordinary process exit is not a write fence.
Controlled stop now persists normal exit separately from an uncertain/group-only
stop and returns bounded diagnostic output without completing the task. Restart
marks unresolved runtime observations blocked and preserves their packet and
checkpoint; repeated restart does not spawn a replacement. A blocked execution
may later accept explicit verified fencing evidence, but cannot restart itself.
The real-wrapper exit/cancel regression checks that surviving descendants never
produce a successful stop observation across five controlled race timings.
Private Git snapshot construction now captures tracked and unignored untracked
working-tree bytes, executable bits and explicit deletions into a new owner-only
directory with ordered content hashes. Read-only Git inventory checks preserve
HEAD/index; source reads reject ancestor/final symlinks, hardlinks, special files,
path escapes and size overflow. Sparse checkouts, submodules and non-UTF8 paths
are currently refused explicitly. Ignored files are excluded. The second source
read/inventory detects observed changes, but is not an atomic filesystem freeze;
this is a baseline work copy, not the final immutable verification source.
Published runtime baseline `fe0aa69` passed both macOS and Linux CI in run
[40663638336](https://github.com/gokselozgur5/agent-conductor/actions/runs/40663638336).
Snapshot changes require their own exact-SHA CI after publication.
Next: verify the actual workspace/write-isolation boundary, then reconcile fence
evidence and expose the operator launcher. OS scheduling is not
a hard real-time guarantee; reassignment still awaits confirmed fencing. Isolated
source capture, vendor adapters and the operator workflow remain incomplete.
Real agents have not been launched yet.
The full product workflow remains the target; later phase and soak requirements
are not waived by the same-day usability objective.

## Deferred

HTTP/dashboard and hooks, Thor/multi-host, performance targets, seven-day soak,
full backup/restore rehearsal, release architecture matrix and end-to-end v1
acceptance remain outside this phase. Database reopen tests do not establish the
full disaster-recovery procedure.
