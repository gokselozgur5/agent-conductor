# Product Requirements v1

## 1. Purpose

Agent Conductor lets one operator provide a project path and entrust a fleet of coding agents to understand that project, continue its active work, keep one shared Kanban board current, recover from interrupted sessions, and stop only after independently verified completion.

The product exists to remove session babysitting without hiding authority, uncertainty, failure, or unfinished work.

## 2. Operating principles

1. Durable state, not chat history, is authoritative.
2. Every agent follows one enforceable protocol; prompts alone are insufficient.
3. Work continues while a safe, relevant next action exists.
4. A worker's completion claim is evidence, not acceptance.
5. Destructive, external, costly, credential-sensitive, publication, and merge actions retain explicit authority boundaries.
6. Agents receive bounded, relevant context rather than an indiscriminate repository dump.
7. Restarts and model/vendor changes must not lose work.
8. The operator sees outcomes, decisions, blockers, evidence, and costs without reading every transcript.

## 3. Primary operator workflow

1. The operator registers an absolute project path.
2. Conductor resolves the canonical path and applicable workspace/project instructions.
3. Conductor discovers conventional project continuity documents and live Git state.
4. Conductor imports or creates explicit tasks on a shared Kanban board.
5. An eligible agent claims one task under an expiring lease.
6. The agent reads the generated resume packet, then the referenced source files it needs.
7. The agent works, checkpoints progress, and publishes relevant events for other agents.
8. The agent submits evidence against declared acceptance checks.
9. An independent verifier accepts or rejects the result.
10. Accepted work becomes `completed`; rejected or interrupted work remains resumable.

## 4. Functional requirements

### FR-1 Project intake

- Accept an absolute local path under an operator-authorized workspace root.
- Resolve symlinks and prevent duplicate registration of the same physical project.
- Read applicable instruction files from outer workspace to project scope.
- Classify instruction sources by an operator-defined trust policy. Repository-resident
  text is untrusted advisory input unless the operator explicitly pins that file by
  content hash as an authoritative instruction source.
- Discover, without inventing, conventional continuity sources such as README, STATUS, PHASES, TRACKING, ARCHITECTURE, and repository-specific equivalents configured later.
- Read Git branch, HEAD, worktree changes, remotes, and worktree identity without modifying the repository.
- Record when the path, repository, or required instruction source becomes unavailable.

### FR-2 Bounded resume packet

- Generate a versioned resume packet containing project identity, instruction-file order, relevant continuity-file references, Git state, active task, dependencies, last checkpoint, open decisions, evidence, authority boundaries, and pending events.
- Require the adapter to acknowledge the packet version and every mandatory instruction file before work begins.
- Freeze authoritative instruction identities and content hashes for each task attempt.
  A worker cannot make its own changed instruction file authoritative.
- Never treat retrieved text as new authority beyond the operator's current task and standing project rules.
- Avoid loading an entire repository into model context by default.

### FR-3 Shared Kanban

- The board is a projection of authoritative task records, never a separately edited status copy.
- Required columns are `backlog`, `ready`, `claimed`, `running`, `paused`, `verifying`, `blocked`, `completed`, and `cancelled`.
- Every transition records actor, reason, timestamp, prior state, new state, and correlation ID.
- Tasks support objective, scope, non-goals, acceptance checks, dependencies, priority, owner, lease, checkpoint, evidence, authority envelope, attempts, and parent/child relationships.
- A task may be `blocked` only with a concrete blocker and a stated condition for resumption.

### FR-4 Ownership and isolation

- At most one worker owns a task at a time.
- Every claim receives a monotonically increasing lease epoch. Heartbeats,
  checkpoints, events, evidence, and state mutations from stale epochs are rejected.
- Lease expiry uses the Conductor host's monotonic clock. A worker's mutation window
  ends before the authoritative expiry; reassignment occurs only after expiry and a
  configured grace period.
- Project write tasks use an isolated Git worktree unless the task contract explicitly permits a different local-only surface.
- Path or subsystem overlap is visible and lease-controlled.
- Losing a worker never deletes or resets unlanded work.
- Reassignment preserves the worktree, checkpoint, event history, and evidence.
- Before local reassignment, dirty tracked and untracked state is snapshotted to a
  content-addressed recovery artifact. The successor receives the delta from the last
  checkpoint as unverified state.
- Unexpected branch or HEAD movement blocks the task. Cross-host reassignment requires
  confirmed fencing of the former host; an unreachable worktree remains blocked rather
  than being assumed safe.

### FR-5 Progress and checkpoints

- Active workers renew heartbeats and task leases.
- Workers checkpoint after meaningful state changes and before long-running or failure-prone operations.
- Checkpoints state completed work, exact current state, next safe action, open decisions, commands/processes still running, and changed artifacts.
- Silence, process exit, context exhaustion, or vendor error cannot mark work complete.

### FR-6 Continuation and recovery

- A reconciler detects expired leases, dead processes, orphan worktrees, unacknowledged wake events, and inconsistent task/runtime state.
- Recoverable work is resumed by the same agent identity or reassigned from its latest durable checkpoint.
- Retries are bounded and classified; repeated identical failure escalates rather than loops forever.
- Restarting Conductor reconstructs current state from the database, filesystem, and verified live-process inventory.
- No task is abandoned merely because one conversation ended.
- Every external side effect is preceded by a committed intent carrying an
  idempotency token. Spawned processes and created worktrees carry that token so the
  reconciler can match intent, live inventory, and filesystem state after a crash.
- Reconciliation precedence is committed intent, then matching tagged live inventory,
  then filesystem observation. Untagged or contradictory live state fails closed.

### FR-7 Events and agent updates

- Agents publish versioned semantic events through Conductor rather than directly mutating one another's state.
- Events carry unique ID, idempotency key, source, correlation, causation, generation, topic, project/task references, payload, and timestamp.
- Subscriptions filter by semantic topic and optional project/path scope.
- Source echo is disabled by default.
- Event generation, retry count, queue age, and fan-out are bounded.
- Filesystem bursts are debounced and coalesced before semantic publication.
- Agents acknowledge consumed events; unacknowledged deliveries remain durable.
- Delivery is at least once. For sources without stable IDs, the idempotency key is
  derived from normalized payload, source identity, and source session. Deduplication
  lives at least as long as the maximum accepted queue age; older duplicates are stale.
- A consumed event's Conductor state effect and acknowledgement commit atomically.
- Filesystem events inside an owned worktree inherit its lease identity, correlation,
  and generation. Writes not attributable to that lease are external modifications.

### FR-8 Controlled wake-up

- A pending event may request an adapter wake only when the target agent is subscribed, eligible, not already handling the same correlation, and within concurrency/cost policy.
- Wake-up includes a bounded resume packet, never only a vague doorbell.
- Duplicate wake requests cannot start duplicate workers for one task lease.
- Wake and claim effects are idempotent on task, lease epoch, and correlation ID.
- An agent must not wake itself recursively from its own file writes.

### FR-9 Verification and completion

- Workers cannot verify their own tasks.
- Required checks are declared before implementation or changed through a recorded design/requirement decision.
- The current lease holder cannot remove required checks or close decisions governing
  its own acceptance. Only the operator or an independent verifier can do so.
- Completion requires all required checks, declared deliverables, clean handoff state, unresolved-decision closure, and an independent accepted verdict.
- Conductor, or a verifier under Conductor control, executes required checks against a
  recorded immutable tree hash. Worker-provided output is a claim, not verification.
- The task worktree is write-fenced during verification. Mutation invalidates the
  verdict and returns the task to a non-terminal state.
- A verifier is a distinct agent identity without write access to the task worktree;
  its verdict references the immutable tree hash and check-execution records.
- `completed` is terminal unless a new task or explicit reopen event is created.
- After completion, the worker releases its lease and stops; it does not invent follow-up work.

### FR-10 Human authority

- The operator can pause, resume, cancel, reassign, reprioritize, or narrow a task.
- Merge, publication, external messaging, purchases, deletion, credential changes, production deployment, and other configured sensitive actions require explicit authority unless a narrowly scoped standing policy exists.
- Evidence, diagnosis, or access does not imply authority.
- Current explicit operator instruction wins within its exact scope and no further.
- Sensitive actions are denied by capability absence where possible: workers receive
  no unneeded credentials, push-capable remote, network access, or unrestricted tool.
  Vendor pre-tool hooks are defense in depth, not the sole boundary.
- An adapter declares enforceable capabilities. A task whose authority envelope cannot
  be enforced by that adapter fails closed before launch.

### FR-11 Adapters

- Support Codex and Claude Code first; Thor is a host, not a model-specific lifecycle.
- Adapter contracts are versioned and capability-discovered.
- Vendor transcripts are diagnostic artifacts, not lifecycle truth.
- Unsupported or changed vendor behavior fails closed and is reported.
- Each attempt records vendor binary version and protocol fingerprint. Adapters run a
  conformance handshake before launch and disable themselves on an unknown mismatch.
- Recovery depends only on Conductor checkpoints, never on vendor session persistence.
- The core never parses free-form model prose to infer destructive authority or completion.

### FR-12 Operator interface

- Provide a CLI before the web interface.
- The web interface shows projects, Kanban, agents, live/expired leases, events, checkpoints, evidence, decisions, blockers, retries, and cost/usage where available.
- UI actions call the same versioned API as the CLI.
- The interface distinguishes verified fact, agent claim, inference, stale snapshot, and operator decision.

### FR-13 Audit and observability

- Persist an append-only audit event for every lifecycle, authority, ownership, verification, and external-effect transition.
- The lifecycle mutation, transition record, and audit record commit in one database transaction.
- Use structured logs with correlation/task/agent/project IDs.
- Never log secrets or full credential material.
- Expose enough evidence to reconstruct why a task moved or stopped.
- Record adapter/model/effort and reported usage without inventing unavailable cost data.

## 5. Non-functional requirements

### NFR-1 Reliability

- Database writes affecting lifecycle are transactional.
- Duplicate commands and events are safe.
- Abrupt termination at any tested transition leaves a recoverable state.
- Crash injection covers every declared lifecycle transition and both sides of every
  external-effect boundary. A continuously executed invariant checker fails the soak
  on duplicate active leases, board/projection mismatch, orphan tagged processes,
  stale-epoch mutation, or an unfenced write surface.
- The core daemon runs for seven days in a soak test without state divergence or unbounded memory/queue growth before v1 release.

### NFR-2 Performance

- Local board reads target p95 below 100 ms with 100 projects, 10,000 tasks, and 100,000 events on the development Mac.
- Event-to-eligible-wake decision targets p95 below 500 ms excluding vendor process startup.
- Idle daemon CPU usage should remain negligible and must be measured before claiming a bound.

### NFR-3 Portability

- Support macOS arm64 and Linux arm64/amd64.
- Use one local daemon binary plus web assets for the production control plane.
- Store paths portably while preserving platform-specific canonical identity.
- Local project identity includes filesystem device/inode identity and Git common-dir
  identity; multiple Git worktrees are explicit workspaces of one project, not
  accidental duplicate projects. Case aliases must not create duplicate registrations.

### NFR-4 Security and privacy

- Bind locally by default.
- Remote access requires authenticated encrypted transport.
- Scope filesystem access to registered authorized roots.
- Treat repository content and hook payloads as untrusted data.
- Untrusted repository text cannot expand an authority envelope, alter required
  checks, or instruct a verifier to accept work.
- Redact secrets from logs, events, UI, and resume packets.
- Do not publish source, artifacts, prompts, or transcripts without explicit authority.

### NFR-5 Evolvability

- Version database migrations, API, adapter protocol, resume packets, and event envelopes.
- Unknown required protocol fields fail closed; unknown optional fields remain forward-compatible.
- Keep vendor adapters replaceable without rewriting task truth.

## 6. Required failure tests

Before a real adapter is considered usable, automated tests must cover:

- Worker exits before its first checkpoint.
- Worker exits after file changes but before reporting them.
- Worker reports success with a failing required check.
- Worker attempts self-verification.
- Two workers race to claim one task.
- Heartbeat arrives at the lease boundary.
- A lease expires while its worker remains alive, including suspend/resume and a partition.
- Conductor dies during a lifecycle transaction.
- Conductor dies between committed intent and side effect, and immediately after the
  side effect, for each supported external-effect type.
- The same hook event arrives repeatedly.
- An agent's write triggers its own subscription.
- Two agents produce a causal event cycle.
- A worktree is dirty when reassignment occurs.
- Git branch or HEAD changes outside Conductor.
- Instruction files change while a task is active.
- Repository text masquerades as an instruction and requests a sensitive action or verifier acceptance.
- Adapter output is malformed, truncated, delayed, or from an unsupported version.
- A sensitive external action is attempted without authority and is proven not to occur.

## 7. Non-goals for v1

- General multi-user project management.
- Replacing GitHub/GitLab, CI, issue trackers, or code review systems.
- Automatically granting merge or publication authority.
- Letting agents create arbitrary goals when the queue is empty.
- Inferring task completion from prose or terminal inactivity.
- Full semantic indexing of every repository file.
- Cloud hosting as the default.
- Optimizing model routing before correctness and recovery are proven.

## 8. v1 acceptance

v1 is accepted only when one operator can register a path, create a task, watch an isolated Codex or Claude worker read the required context, interrupt the worker mid-change, restart Conductor, resume or reassign without losing work, obtain independent verification, and see the board move to `completed` exactly once—with the full causal record visible and no unauthorized external action.
