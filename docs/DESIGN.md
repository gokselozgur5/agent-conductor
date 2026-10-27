# Phase 1 Design

## 1. Scope

Phase 1 replaces the Python executable spike with a production Rust control
core. It owns project, task, lease, event, audit, and verification truth. It
does not launch Codex or Claude, mutate project worktrees, or expose a web UI.

The phase exists to make later process and repository side effects safe to add.

## 2. Technology decision

- Rust stable, with the minimum supported version pinned in `rust-toolchain.toml`
- Tokio for the daemon runtime and cancellation
- SQLx with SQLite for typed migrations and transactional persistence
- Clap for the CLI
- Serde for versioned JSON envelopes
- Tracing for structured diagnostics
- UUID v7 identifiers and UTC wall timestamps for audit display
- Suspend-aware monotonic clocks behind a platform port (`mach_continuous_time`
  on macOS and `CLOCK_BOOTTIME` on Linux)

Axum, Notify, and React/TypeScript remain selected for later phases; they are
not Phase 1 dependencies.

SQLite runs in WAL mode with foreign keys enabled, a busy timeout, and an
explicit synchronous policy selected by durability tests. A long-running local
daemon owns one dedicated writer connection. Every command transaction uses
`BEGIN IMMEDIATE` and is serialized by the service; a separate pool serves
read-only projections.

The CLI communicates with the daemon over a versioned JSON protocol on a local
Unix-domain socket. All mutations go through the daemon; offline direct database
access is limited to explicit read-only diagnosis and integrity recovery tools.

## 3. Workspace shape

```text
crates/
  domain/       pure types, commands, transitions, invariants
  store/        SQLite migrations, transactions, projections, audit
  protocol/     versioned command, event, resume, and adapter envelopes
  service/      application commands and reconciliation decisions
  daemon/       Tokio runtime, local socket, writer ownership, shutdown
  cli/          human and machine-readable local interface
  testkit/      deterministic clock, fake effects, crash/fault harness
```

Dependency direction is inward: `daemon/cli -> service -> domain`. `protocol`
maps versioned DTOs to domain commands; `store` and future adapters implement
ports declared by `service` or `domain`. Domain code does not depend on SQLx,
Tokio, protocol serialization, vendor SDKs, Git, or HTTP.

## 4. Authoritative records

### Project and workspace

A project is the Git common-directory identity or a non-Git directory identity.
A workspace is one canonical filesystem location within that project, including
a main checkout or managed worktree. Filesystem identity records device/inode
where supported plus a canonical path for display and recovery.

### Task and attempt

A task stores objective, scope, non-goals, dependencies, priority, authority
envelope, required checks, and terminal result. An attempt is one execution era
of the task and freezes:

- worker identity and adapter capability profile;
- monotonically increasing lease epoch;
- workspace identity and starting tree/HEAD;
- authoritative instruction hashes;
- required checks and open acceptance decisions;
- resume-packet version.

Changing one of those inputs ends or invalidates the attempt; it is never
silently edited underneath a running worker.

### State model

```text
backlog -> ready -> claimed -> running -> verifying -> completed
            |         |         |          |
            +---------+---------+----------+-> paused
            +---------+---------+----------+-> blocked
                                   verifying -> running (rejected/invalidated)

non-terminal -> cancelled
blocked/paused -> ready
```

`completed` and `cancelled` are terminal. Reopening creates a new task linked to
the terminal task. Dependency resolution is the only path from `backlog` to
`ready`. Blocking or pausing ends the current lease while preserving the
attempt, workspace, checkpoint, and evidence; resumption creates a claim with a
new epoch. A blocker is typed and includes its resumption condition.

Every transition is a command with expected state/version, actor, reason,
correlation ID, and idempotency key. The transaction atomically updates the
aggregate version and appends transition and audit records. Optimistic version
failure returns a conflict and performs no partial write.

The domain applies `(aggregate, command, clock reading)` and returns a complete
outcome: state delta, transition, audit entries, events, and effect intents. The
store persists that outcome atomically with a version compare-and-swap; it does
not decide lifecycle policy. Partial unique indexes enforce structural guards
such as one active attempt per task and one write owner per workspace.

## 5. Lease and fencing model

Claiming a task increments its lease epoch. The lease record stores display wall
time, daemon boot generation, remaining duration at the last accepted heartbeat,
grace duration, and the daemon's live monotonic deadline. Every
worker-originated command includes task ID, attempt ID, lease epoch, and command
idempotency key.

The domain rejects a command unless all identities match the current active
attempt and the suspend-aware monotonic deadline remains valid. A worker
receives a shorter local mutation deadline. Later write-capable adapters must also provide an
external fence—isolated sandbox/worktree permissions and process termination—
because rejecting stale API messages cannot stop direct filesystem writes.

Each daemon start increments a durable boot generation. Existing leases enter
`recovery_pending` and are re-armed from the fresh monotonic clock for no more
than `min(remaining_at_last_heartbeat, recovery_grace)`. Only a heartbeat for
the current attempt and epoch restores active status. Expiry plus grace is
required before reassignment.

Suspend, clock adjustment, heartbeat races, daemon restart, and network
partition are explicit test cases. Wall time is never used alone to decide a
live-process lease. Unexpected wall/monotonic drift forces reconciliation.

## 6. Transaction and side-effect protocol

Phase 1 records effects but executes only fake effects. Later effect runners use
a transactional outbox:

1. A command transaction writes state, audit, and `effect_intent` with a stable
   token.
2. The runner claims the intent and performs the idempotent external action.
3. The produced process/worktree/resource is tagged with the token.
4. A result transaction records the observed identity and marks the intent done.
5. On restart, reconciliation compares committed intent, tagged live inventory,
   and filesystem observations. Contradiction blocks the task.

There is no distributed transaction claim. The system obtains convergence by
durable intent, idempotent effects, fencing, and reconciliation.

An effect intent progresses through `pending`, `claimed`, `observed`, `done`, or
`abandoned`. A claim has a runner and deadline. On reconciliation: matching
intent and tagged inventory records the observation; a missing idempotent-safe
effect may be retried with the same token; an ambiguous or unsafe observation
blocks the task. The token is a versioned hash of task, attempt, lease epoch,
correlation ID, and effect kind.

## 7. Events and delivery

Domain events and audit records are appended inside the same transaction as the
state mutation. Delivery uses an outbox row per matching subscription.

- Source IDs are used when stable.
- Otherwise the idempotency key is a versioned hash of normalized payload,
  source identity, and source session.
- Consumers receive at-least-once delivery.
- Applying a consumer result and acknowledging the delivery is one transaction.
- Wake effects key on task, lease epoch, and correlation ID.
- Generation, fan-out, retries, queue age, and payload size have configured caps.

Expired deliveries become audited stale records; they do not become fresh work.

Command idempotency is stored as `(actor, key, request_hash, outcome)`. A replay
with the same request returns the stored outcome before version comparison; the
same key with a different request is rejected. Retention is at least the maximum
accepted delivery age.

## 8. Verification model

Entering `verifying` first fences all mutation-class commands and revokes the
worker's external write capability. Only then does Conductor record an immutable
Git tree hash or a content manifest for non-Git work. Heartbeats and bounded
status reports remain allowed. Each required check has a stable definition,
executor, timeout, and expected result.

Check execution records command identity, source hash, environment fingerprint,
exit status, and bounded artifact references. Worker transcripts and pasted test
output are claims only. A distinct verifier with read-only workspace access may
interpret results and issue a verdict bound to the same source hash. Any source
mutation invalidates pending checks and verdicts.

The task completes only when the service re-reads an equal source identity and
validates every required check and an accepted independent verdict in one
transition transaction. Domain rules enforce that the verdict actor differs
from the worker and that only operator/verifier roles may change acceptance
checks or decisions.

## 9. Trust and authority

Resume packets contain separately labelled sections:

- operator authority and task contract;
- pinned authoritative instructions and hashes;
- Conductor state and verified facts;
- untrusted repository context;
- unverified worker claims and recovered dirty state.

No free-form text can expand authority. The authority envelope is typed data.
Phase 1 represents only adapter read/write mode, protocol fingerprint, and an
opaque hashed capability document. Phase 2 defines the capability vocabulary
for filesystem roots, network, credentials, and tool interception. Scheduling
fails closed when the required boundary is absent or unknown.

## 10. Reconciliation and invariant checking

The reconciler emits commands; it does not patch tables directly. Its inputs are
database state, a monotonic clock, tagged process inventory, workspace inventory,
and adapter conformance status.

It checks at least:

- one active attempt and lease per task;
- one owner per managed write workspace;
- current epoch on every accepted worker mutation;
- task state equal to Kanban projection;
- no live tagged effect without matching intent;
- no completed task with missing checks/verdict/tree identity;
- no active adapter with an unknown protocol fingerprint.

Contradictions become explicit blocked reasons. The checker is reused by tests,
startup recovery, diagnostics, and the soak test.

## 11. Protocol versioning

Every envelope contains `protocol`, `version`, `message_id`, `correlation_id`,
and a typed payload. Unknown major versions or required fields fail closed.
Unknown optional fields survive round trips where applicable.

The Phase 1 CLI supports human text and versioned JSON output. Exit codes
distinguish validation, conflict, unavailable dependency, invariant violation,
and internal failure.

## 12. Phase 1 test strategy

- Pure transition tables cover every legal and illegal state edge.
- Property tests generate command sequences and assert invariants.
- SQL integration tests execute migrations and transaction races.
- Failpoints terminate execution before and after each transaction/effect edge.
- A deterministic clock covers expiry and suspend/resume cases.
- Model tests compare the Rust state machine to a simple reference model.
- The Python spike remains behavior evidence, not an oracle for new guarantees.

Phase 1 exits only when its supported subset of the required failure tests passes
on macOS and Linux and database restore/reopen reproduces the same projections.

## 13. Deferred decisions

- Axum API authentication and SSE versus WebSocket belong to Phase 4.
- Remote-host identity and transport belong to Phase 5.
- Exact sandbox implementations are adapter-specific Phase 2 design work.
- PostgreSQL and cloud control planes are outside v1.
