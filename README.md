# Agent Conductor

Agent Conductor is a local-first control plane for durable, evidence-backed work across coding agents.

It does not trust a conversation to remain alive and it does not accept a worker's claim of completion as proof. Tasks, leases, checkpoints, events, subscriptions, deliveries, and verification decisions live in SQLite.

## Executable spike

- Explicit task lifecycle: `queued -> claimed -> running -> verifying -> completed`
- Exclusive task leases with heartbeat renewal
- Crash recovery that preserves checkpoints and re-queues expired work
- Independent verification: a worker cannot approve its own task
- Required acceptance checks before completion
- Idempotent event publication
- Topic/path subscriptions with bounded event generations
- No source-agent echo by default, preventing simple hook loops
- Deterministic polling hooks that route only matching topic/path changes
- Project path intake with rule/doc discovery, Git state, and a live Kanban projection

The spike is retained as behavioral evidence. Production Phase 1 is a Rust
control core with a long-running local daemon, SQLite single-writer ownership,
epoch-fenced leases, transactional effect intents, and immutable-source
verification. See `docs/REQUIREMENTS.md`, `docs/DESIGN.md`, and
`docs/THREAT_MODEL.md` before implementation.

## Quick start

```bash
python3 -m venv .venv
. .venv/bin/activate
python -m pip install -e .
conductor --db ./state/conductor.db init
conductor --db ./state/conductor.db register-agent codex-main codex
conductor --db ./state/conductor.db add-project /absolute/path/to/project
conductor --db ./state/conductor.db create-task "Build the API" --project-id PROJECT_ID --check tests --check docs
conductor --db ./state/conductor.db list-tasks
conductor --db ./state/conductor.db board
conductor --db ./state/conductor.db subscribe test-agent 'file.*' --path-pattern 'src/*'
conductor --db ./state/conductor.db watch-once . --snapshot ./state/project-snapshot.json
```

For development:

```bash
PYTHONPATH=src python3 -m unittest discover -s tests -v
```

Real vendor adapters, write-capable worktree control, HTTP service, and the
dashboard are later phases. The production state machine comes first so those
surfaces cannot silently invent lifecycle semantics.

## Path intake

The operator supplies an absolute project path. Conductor records its canonical path and produces a bounded resume packet containing applicable `AGENTS.md` files, known status/phase documents, current Git status, and the project's Kanban projection. Agents receive the packet and read the referenced files in order; Conductor does not dump an entire repository into model context.
