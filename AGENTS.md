# Agent Conductor

This repository is an English-only, local-first agent coordination control plane.

## Non-negotiable invariants

- SQLite state is authoritative; chat transcripts are not.
- A worker cannot verify its own task.
- `completed` requires a recorded acceptance decision and all required checks.
- Expired work is re-queued without deleting its checkpoint or evidence.
- Events are idempotent and are not delivered back to their source by default.
- Filesystem hooks publish events; they never directly launch every agent.
- Destructive actions, credential use, publication, and merge authority remain explicit.

## Development

- Treat the current Python package as an executable spike, not the production architecture.
- Follow `docs/REQUIREMENTS.md`, `docs/DESIGN.md`, and `docs/THREAT_MODEL.md`.
- Build the production core in Rust with the crate boundaries defined in the design.
- Add a regression test for every lifecycle or recovery bug.
- Do not weaken a refusal path to make a test pass.
- Keep adapters outside the state machine. Vendor behavior must not define task truth.
- Run formatting, lint, tests, and secret checks before each atomic commit.
