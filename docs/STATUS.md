# Status

## Current gate

Requirements v1 is accepted for design. Phase 1 design was revised on
2026-10-27 after a second independent Claude Fable 5.1 architecture review.
The local consistency pass accepted the design gate; production development has
not started.

## Executable spike

The Python/SQLite spike is clean at commit `15f6639` with 14 passing tests. It proves selected lifecycle and path-intake behaviors but is not the production architecture.

## Decisions

- Process: requirements → design → development → feedback for every bounded phase.
- Local-first and single-operator by default.
- Durable protocol enforcement, not prompt-only compliance.
- Kanban is derived from authoritative task state.
- Workers cannot self-verify.
- No real adapter launches before fake-process failure tests.
- Lease epochs fence stale workers; expiry alone never proves a process is dead.
- Repository content is untrusted unless the operator pins an instruction by hash.
- Verification is bound to an immutable tree hash and a read-only independent verifier.
- Sensitive-action policy must be enforced by adapter capabilities, not only prompts.
- A long-running daemon owns the SQLite writer and serves a local Unix socket.
- Suspend-aware monotonic deadlines are recovered through a boot generation and
  bounded `recovery_pending` grace window.
- Verification fences writes before recording the immutable source identity.

## Selected stack

- Rust, Tokio, SQLx with SQLite, Serde, Tracing, and Clap for Phase 1
- Axum and Notify in later service/hook phases
- React and TypeScript for the later dashboard
- Versioned JSON adapter/event/resume protocols

## Deferred design work

- Adapter transport and lifecycle contract
- Worktree ownership model
- Wake scheduling and loop prevention
- API and UI boundary

## Explicitly absent

- Production Rust core
- Real Codex or Claude Code process launch
- Running production daemon
- Worktree locks
- Remote/Thor runtime
- HTTP/WebSocket/SSE service
- Web dashboard
