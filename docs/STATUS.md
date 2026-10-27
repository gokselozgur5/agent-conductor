# Status

## Current gate

Requirements v1 revised on 2026-10-27 after an independent Claude Fable 5.1
adversarial review. A local acceptance pass is still required before the design gate;
production development has not started.

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

## Proposed stack awaiting design lock

- Rust, Tokio, Axum, SQLx with SQLite, Serde, Tracing, Notify, Clap
- React and TypeScript for the later dashboard
- Versioned JSON adapter/event/resume protocols

## Open design work

- Domain and transition model
- Audit/event consistency model
- Adapter transport and lifecycle contract
- Worktree ownership model
- Wake scheduling and loop prevention
- Authority envelope
- Threat and failure model
- API and UI boundary

## Explicitly absent

- Production Rust core
- Real Codex or Claude Code process launch
- Background daemon
- Worktree locks
- Remote/Thor runtime
- HTTP/WebSocket/SSE service
- Web dashboard
- GitHub repository or publication
