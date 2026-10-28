# Status

## Current gate

Phase 1 implementation and final independent reproducer review are accepted at
`5d26e14`, with a separately reviewed transport correction at `751ba5b`.
The initial Linux CI failure and its regression are recorded in the acceptance
matrix. No reproduced review findings remain open locally. The exact published
commit's macOS/Linux CI is the remaining acceptance gate.
The implementation has passed local formatting, strict Clippy and repeated full
workspace tests. See [PHASE1_ACCEPTANCE.md](PHASE1_ACCEPTANCE.md) for exact evidence
and the distinction between core tests and deferred end-to-end v1 behavior.

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

## Next phase

After final Phase 1 review and CI acceptance, design the Phase 2 adapter contract
and fake-agent harness before integrating local authenticated Codex/Claude CLIs.
Resume-packet acknowledgement, instruction trust, dirty-state snapshots, worktree
ownership, real check execution and capability enforcement must be proven before
write-capable agents launch. No Phase 2 implementation is authorized by Phase 1
closeout alone.

## Deferred

HTTP/dashboard and hooks, Thor/multi-host, performance targets, seven-day soak,
full backup/restore rehearsal, release architecture matrix and end-to-end v1
acceptance remain outside this phase. Database reopen tests do not establish the
full disaster-recovery procedure.
