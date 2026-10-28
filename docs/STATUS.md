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
Next: connect durable preparation/acknowledgement to wrapper release in the
controller, then isolated source capture and real adapters. Real agents have
not been launched yet.
The full product workflow remains the target; later phase and soak requirements
are not waived by the same-day usability objective.

## Deferred

HTTP/dashboard and hooks, Thor/multi-host, performance targets, seven-day soak,
full backup/restore rehearsal, release architecture matrix and end-to-end v1
acceptance remain outside this phase. Database reopen tests do not establish the
full disaster-recovery procedure.
