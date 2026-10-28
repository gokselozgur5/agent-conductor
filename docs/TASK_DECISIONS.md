# Task decisions

A task can retain up to 32 acceptance questions, each with a stable identifier,
question, opener and timestamp. A resolution records its rationale, actor and time.
Resolved records are immutable; a changed question requires a new decision. The
aggregate and audit event commit together through the existing versioned command
API. Duplicate command replay is safe; a reused key with different content fails.

Open decisions prevent completion even when all checks pass and an independent
verifier accepts the immutable source. The invariant checker also reports a
completed task with an unresolved decision. Decision text is context, not an
instruction pin, capability grant or permission to change required checks.

The operator or a registered independent verifier can open and resolve decisions
in backlog, ready, paused or blocked. The current worker cannot do either, including
by presenting another role. Pause running or verifying work first. Continue retained
work after the decision: the new attempt receives the recorded decision snapshot,
including resolved rationales, in labelled untrusted context. It must obtain fresh
source-bound checks and review. Editing does not resume work or resolve a blocker.
Existing worker packets are immutable and are not silently rewritten.

On the board, use **Open decision** and **Resolve decision**. A rationale is required.
Questions are bounded to 1024 UTF-8 bytes, identifiers to 128, and rationales to 2048.
All text renders as text nodes. Stale versions are refused; inspect refreshed state
before retrying. This is recorded operator/verifier context, separate from the
review verdict displayed on the card.

The CLI uses the same `ApplyCommand` path:

```sh
conductorctl --socket /absolute/control.sock apply TASK_ID '{"actor":{"id":"operator","role":"operator"},"expected_version":0,"correlation_id":"format-question","idempotency_key":"format-question","reason":"Confirm required output format","kind":{"type":"open_decision","id":"format","question":"Which deliverable format is required?"}}'
```

Resolve with a fresh expected version/idempotency key and
`"kind":{"type":"resolve_decision","id":"format","rationale":"Deliver UTF-8 text."}`.
The socket is privileged; worker access remains absent. Text must not contain
credentials. This feature does not claim general secret redaction.

Migration 0014 is a semantic compatibility fence: decisions use existing aggregate
storage, but older binaries must refuse a database carrying the new migration.
Legacy tasks without a decision field decode to an empty list. Keep a database
backup for an intentional downgrade; never remove the migration record to force
an older binary to run.

## Initial plan declarations

Native plans accept an optional `decisions` array (maximum 32 unique IDs):

```json
{"decisions":[{"id":"format","question":"Which format?","rationale":"Use UTF-8 text."},{"id":"scope","question":"Confirm final delivery scope"}]}
```

Use `tools/conductor plan ... --decisions-file /absolute/decisions.json` to supply
that array. Each declaration opens a question through the existing operator
command; an optional nonempty rationale then resolves it. Supplied author or time
fields are rejected: the service records the operator and observation time.
Validation occurs before queue admission or artifact creation. Legacy plans omit
this field when empty, preserving their canonical encoding and replay identity.

Declarations are applied after task creation and before readiness or first claim.
They are separate audited transactions: if a later write fails, earlier declared
questions remain visible in the unfinished backlog task, and no worker is claimed.
This does not promise an all-or-nothing task initialization transaction. Such a
partial intake requires operator reconciliation; it is never silently replayed.
Continuation uses the task's current records and does not redeclare initial
questions or replace later resolutions with the original plan.

Open questions allow work to begin but prevent completion. To resolve one during
an active attempt, pause first, resolve it, then continue with fresh context and
independent review. A resolved initial decision is an operator declaration, not
worker authority or proof that the deliverable satisfies it.

Workers do not autonomously create/close decisions. Checkpoint claims remain
separate from formal decisions; see [CHECKPOINTS.md](CHECKPOINTS.md). Reassignment
controls and full project/agent/event visibility remain separate work.

Validation covers completion refusal despite passing checks/verdict, resolution
followed by a new attempt/epoch and review, current-worker role spoofing, active
state refusal, immutable resolution, text/capacity limits, and legacy decoding.
Real SQLite/HTTP checks cover injected audit rollback, exact replay, conflicting
replay, stale versions and restart. Context checks verify decision snapshots and
unchanged instruction pins; the verifier inherits the writer packet context.
A Chrome probe exercises open/resolve, CLI read-back, markup-as-text, token storage
boundaries and disconnect with an in-flight response. A pre-decision binary was
actually refused on a migrated fixture database. No model calls were needed for
these controlled decision tests; this is not new native model acceptance.

Initial intake validation passes 219 workspace tests and strict Clippy, including
bounds/duplicates, pre-claim ordering, restart, invalid-input side-effect refusal
and an injected audit failure preventing claim. A real launcher/CLI process probe
verified generation, identical resubmission, forged-author refusal and queued
restart persistence without model calls. Evidence is retained privately in
`reviews/agent-conductor-runtime-20261028/intake-cli-v1/receipt.json` and
`intake-workspace-v2.log`. This does not replace the earlier native workflow
acceptance or claim automatic recovery of partially initialized tasks.
