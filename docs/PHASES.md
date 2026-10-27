# Delivery Phases

Every phase uses the same gated sequence:

1. Requirements
2. Design
3. Development
4. Feedback and measured acceptance

A later phase cannot silently weaken an earlier invariant.

## Phase 0 — Executable spike

Status: complete, retained as behavioral evidence rather than production architecture.

- SQLite task lifecycle
- Independent verification gate
- Lease expiry and checkpoint retention
- Idempotent event delivery
- One-shot content-hash filesystem watcher
- Canonical project-path intake
- Instruction/doc/Git resume discovery
- Kanban projection

## Phase 1 — Production control core

- Rust workspace and versioned migrations
- Project/task/agent/event/audit domain model
- Transactional state machine
- CLI and structured output
- Property and crash-boundary tests
- Importer for the Python spike database is not required unless real user state exists

Exit gate: domain invariants and required failure tests for core transitions pass on macOS and Linux.

## Phase 2 — Adapter runtime

- Versioned adapter protocol
- Fake-agent harness
- Codex adapter
- Claude Code adapter
- Process inventory, heartbeat, interrupt, exit, and relaunch
- Resume-packet acknowledgement
- Isolated worktree ownership and lease-epoch fencing
- Dirty-state recovery snapshots and unexpected-HEAD blocking
- Capability-based authority enforcement

Exit gate: the complete real-adapter failure suite in `REQUIREMENTS.md` passes;
killed, suspended, and stale workers cannot retain ownership, mutate verification
state, perform unauthorized effects, or cause false completion.

## Phase 3 — Repository isolation and hooks

- Cross-worktree path/subsystem overlap leases
- Debounced/coalesced filesystem and Git events
- Instruction-change invalidation
- Bounded wake scheduler

Exit gate: event storms and causal loops cannot create duplicate workers or unbounded queues.

## Phase 4 — Local operator interface

- Axum API
- React/TypeScript dashboard
- Projects, Kanban, agents, checkpoints, events, evidence, blockers, and decisions
- Pause/resume/reassign/cancel controls
- SSE or WebSocket live updates selected by measured need

Exit gate: CLI and UI produce identical lifecycle effects through the same API.

## Phase 5 — Multi-host operation

- Authenticated encrypted host connection
- Thor runtime registration
- Host capability and capacity inventory
- Remote process recovery and artifact references

Exit gate: network loss and host restart preserve task truth without expanding filesystem or credential authority.

## Phase 6 — v1 hardening

- Seven-day soak
- Load and queue-growth measurements
- Backup/restore and migration rehearsal
- Secret-redaction tests
- macOS arm64 and Linux arm64/amd64 release artifacts
- Operator runbook and disaster recovery exercise

Exit gate: the end-to-end v1 acceptance scenario in `REQUIREMENTS.md` passes with both Codex and Claude Code.
