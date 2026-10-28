# Agent Conductor

Agent Conductor is a local-first control plane for durable, evidence-backed work across coding agents.

It does not trust a conversation to remain alive and it does not accept a worker's claim of completion as proof. Tasks, leases, checkpoints, events, subscriptions, deliveries, and verification decisions live in SQLite.

## Current implementation

Phase 1 implements the Rust control core: a local Unix-socket daemon, transactional
SQLite store, task/attempt lifecycle, fenced leases, durable events and delivery,
independent verification records, and fake-effect recovery tests. Acceptance and
remaining boundaries are tracked in [STATUS](docs/STATUS.md) and the
[Phase 1 evidence matrix](docs/PHASE1_ACCEPTANCE.md).

The socket is privileged single-operator access, not authenticated agent transport.
Verification sources and check outcomes are operator/test inputs in this phase;
Conductor does not yet capture Git trees, run checks, or fence filesystem writes.

## Rust quick start

The pinned toolchain is installed through rustup. From the repository root:

```bash
cargo build --workspace
cargo run -p conductor-daemon --bin conductor-daemon
```

In another terminal at the same repository root:

```bash
cargo run -p conductor-cli --bin conductorctl -- --json health
cargo run -p conductor-cli --bin conductorctl -- register-project operator register-demo demo "$PWD"
cargo run -p conductor-cli --bin conductorctl -- create-task operator create-demo demo-task demo "Exercise the control core"
cargo run -p conductor-cli --bin conductorctl -- --json list-tasks
cargo run -p conductor-cli --bin conductorctl -- check-invariants
```

Defaults live under ignored `state/`: `conductor.sqlite3`, `conductor.sock`, and
`conductor.lock`. Stop the foreground daemon with Ctrl-C. Reusing the same command
identity and payload returns its stored outcome. A changed payload requires a new
idempotency key. Use the same lock path for every owner of a database.

`conductorctl apply TASK_ID COMMAND_JSON` submits a versioned domain command.
`bind-verification-source TASK_ID EXPECTED_VERSION IDEMPOTENCY_KEY SOURCE`
binds a labelled fake/operator source through the daemon's internal System actor.
It is not proof of a Git snapshot or a sandbox. External System-role commands are
rejected. Use `--help` for registration, event subscription/publication, delivery
claim/acknowledgement, and structured query commands.

Development gates:

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features
```

## Retained Python spike

The Python implementation is separate behavioral evidence; its database is not
the Rust database. Do not point the two implementations at the same file.

```bash
python3 -m venv .venv
. .venv/bin/activate
python -m pip install -e .
conductor --db ./state/spike.db init
PYTHONPATH=src python3 -m unittest discover -s tests -v
```

Real Codex/Claude adapters, worktree control, instruction discovery in the Rust
runtime, filesystem hooks, HTTP/dashboard, and multi-host operation remain later
phases. See [phases](docs/PHASES.md), [requirements](docs/REQUIREMENTS.md),
[design](docs/DESIGN.md), and [threat model](docs/THREAT_MODEL.md).

## Python spike path intake

In the retained spike, the operator supplies an absolute project path. Conductor records its canonical path and produces a bounded resume packet containing applicable `AGENTS.md` files, known status/phase documents, current Git status, and the project's Kanban projection. Agents receive the packet and read the referenced files in order; Conductor does not dump an entire repository into model context.

### Native task execution

The opt-in plan-driven daemon mode now runs a native worker, isolated checks and
a distinct read-only reviewer through the durable lifecycle. See
[Native workflow](docs/NATIVE_WORKFLOW.md) for setup and the first real synthetic
acceptance result. Original checkouts remain unchanged; broader product and
recovery gates are still tracked in [Status](docs/STATUS.md).
