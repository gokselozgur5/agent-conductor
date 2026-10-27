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

- Keep the core dependency-free until a real integration requires otherwise.
- Add a regression test for every lifecycle or recovery bug.
- Do not weaken a refusal path to make a test pass.
- Keep adapters outside the state machine. Vendor behavior must not define task truth.
