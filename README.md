# Agent Conductor

Agent Conductor is a local-first control plane for durable, evidence-backed work across coding agents.

It does not trust a conversation to remain alive and it does not accept a worker's claim of completion as proof. Tasks, leases, checkpoints, events, subscriptions, deliveries, and verification decisions live in SQLite.

Start with [PRODUCT.md](docs/PRODUCT.md): it is the operator-approved direction
(two leaders and their soldiers in one terminal chat). Other current docs:
[architecture](docs/ARCHITECTURE.md), [chat delivery](docs/CONVERSATION_DELIVERY.md),
[native workflow](docs/NATIVE_WORKFLOW.md), [requirements](docs/REQUIREMENTS.md),
[threat model](docs/THREAT_MODEL.md), [store transactions](docs/STORE_TRANSACTIONS.md).
Paused goals and past phases are in [docs/archive](docs/archive/README.md).

## Run the local operator workflow

For CI-built native binaries, see [runtime kits](docs/RUNTIME_KITS.md).

Build once, initialize a private runtime for a committed Git project, then run it:

```sh
cargo build --workspace --release
./tools/conductor init --bin-dir "$PWD/target/release" --project /absolute/path/to/project
./tools/conductor run --dispatch
```

Open http://127.0.0.1:8787 and log in with the credential from
`./tools/conductor token` (use `| pbcopy` on macOS). The launcher configures the
socket, database, project registration and repository watcher. In another terminal,
`./tools/conductor status` shows authoritative health, tasks and dispatch state.
See [local operator setup](docs/LOCAL_OPERATOR.md) for model authentication,
preparing plans with accepted adapter pins, event-triggered tasks and restart.

## Current implementation

The Rust runtime supports durable task/lease state, private dirty Git snapshots,
pinned native Claude writers, independent Codex/Claude review, isolated
checks, retained-work continuation, attributed repository events and subscribed
workflow wake admission, and [verified predecessor handoff](docs/WORKFLOW_DEPENDENCIES.md). The authenticated local board submits plans and projects
the same state as the CLI. Workers cannot complete or verify their own tasks.

The local socket is privileged operator access. Native file-only writer fencing
relies on the accepted trusted CLI capability profile; generic hostile-process
fencing, other-host isolation and full product hardening remain open. See
[status](docs/STATUS.md), [requirements](docs/REQUIREMENTS.md), and
[adapter boundaries](docs/ADAPTER_CONFORMANCE.md). This is not a completed v1 release.

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
cargo build --workspace
CONDUCTOR_TEST_BIN_DIR="${CARGO_TARGET_DIR:-$PWD/target}/debug" cargo test --workspace --all-features
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

The Python spike is not the production runtime. Automatic advisory instruction
discovery, broader host support and remaining v1 acceptance are tracked separately. See [requirements](docs/REQUIREMENTS.md),
[design](docs/DESIGN.md), and [threat model](docs/THREAT_MODEL.md).

## Python spike path intake

In the retained spike, the operator supplies an absolute project path. Conductor records its canonical path and produces a bounded resume packet containing applicable `AGENTS.md` files, known status/phase documents, current Git status, and the project's Kanban projection. Agents receive the packet and read the referenced files in order; Conductor does not dump an entire repository into model context.

### Native task execution

The opt-in plan-driven daemon mode now runs a native worker, isolated checks and
a distinct read-only reviewer through the durable lifecycle. See
[Native workflow](docs/NATIVE_WORKFLOW.md) for setup and the first real synthetic
acceptance result. Original checkouts remain unchanged; broader product and
recovery gates are still tracked in [Status](docs/STATUS.md).

### Local operator board

An opt-in authenticated loopback board displays task state, checks and independent
review results. See [Operator board](docs/OPERATOR_BOARD.md) for `--http`, credential
handling and operator controls. The existing Unix-socket CLI remains available.

Native plan submission and persistent execution: [Workflow dispatch](docs/WORKFLOW_DISPATCH.md).

Opt-in repository change observation: [Repository hooks](docs/REPOSITORY_HOOKS.md).
