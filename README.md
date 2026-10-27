# Agent Conductor

Agent Conductor is a local-first control plane for durable, evidence-backed work across coding agents.

It does not trust a conversation to remain alive and it does not accept a worker's claim of completion as proof. Tasks, leases, checkpoints, events, subscriptions, deliveries, and verification decisions live in SQLite.

## Phase 1 guarantees

- Explicit task lifecycle: `queued -> claimed -> running -> verifying -> completed`
- Exclusive task leases with heartbeat renewal
- Crash recovery that preserves checkpoints and re-queues expired work
- Independent verification: a worker cannot approve its own task
- Required acceptance checks before completion
- Idempotent event publication
- Topic/path subscriptions with bounded event generations
- No source-agent echo by default, preventing simple hook loops

## Quick start

```bash
python3 -m venv .venv
. .venv/bin/activate
python -m pip install -e .
conductor --db ./state/conductor.db init
conductor --db ./state/conductor.db register-agent codex-main codex
conductor --db ./state/conductor.db create-task "Build the API" --check tests --check docs
conductor --db ./state/conductor.db list-tasks
```

For development:

```bash
PYTHONPATH=src python3 -m unittest discover -s tests -v
```

The HTTP service, vendor adapters, filesystem watcher, and dashboard are later phases. The state machine comes first so those surfaces cannot silently invent lifecycle semantics.
