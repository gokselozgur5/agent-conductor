# Durable native workflow dispatch

Start the daemon with `--dispatch` to execute explicitly submitted native plans
sequentially while keeping its socket and optional board available:

```sh
conductor-daemon --database /private/state.sqlite3 \
  --socket /private/conductor.sock --lock /private/conductor.lock \
  --dispatch --http 127.0.0.1:8787
conductorctl --socket /private/conductor.sock submit-workflow /private/plan.json
conductorctl --socket /private/conductor.sock list-workflows
```

The paths above are placeholders. Configure the board token and native vendor
authentication in the daemon environment, never in a plan or command argument.
See [NATIVE_WORKFLOW.md](NATIVE_WORKFLOW.md) for the approved plan schema and
adapter requirements. The board can submit a selected local plan JSON file after
explicit confirmation and displays dispatch receipts alongside authoritative
tasks. Model work can begin immediately when dispatch is enabled.

`--dispatch` conflicts with one-shot `--run-plan`. Without it, submission persists
plans but does not launch models. A later daemon invocation with dispatch enabled
can process queued plans. Plan input is bounded to 128 KiB and pending dispatches
to 256. Identical normalized plans for the same task ID replay; different plans
under that ID are refused. Native execution still refuses an already-existing
task instead of overwriting it. Treat plan submission as explicit authority to
execute the pinned workflow, including its model calls and declared checks.

Dispatch status is not task acceptance:

- `queued`: approved plan stored, not claimed.
- `running`: claimed by the current daemon boot; only one claim may run.
- `finished`: the invocation returned. Inspect `completed` in its receipt and the
  authoritative task state; rejected checks/review and execution errors also
  finish an invocation without completing a task.
- `needs_recovery`: an earlier boot held the claim when it stopped. Automatic
  relaunch requires the explicit recovery option below. Task, execution records and private artifacts remain available.

Restart changes the boot identity, rejects stale claim/finish operations, preserves
queued plans and marks prior running dispatches for reconciliation. It does not
prove old process fencing. Pause/resume/cancel still use existing domain commands;
resuming a task alone does not create a fresh dispatch or erase its workspace.

## Continue retained work

After inspecting the task, use `conductorctl continue-workflow TASK_ID VERSION`
or the board queue entry’s **Continue retained work** button. This explicitly
authorizes another model attempt. The service checks the displayed/current task
version, refuses terminal tasks, and requires every prior execution to have a
verified write fence. The retained packet must match the task/attempt/worker/epoch
and stored packet digest. A running or already queued dispatch cannot be requeued.

An enabled dispatcher rechecks these conditions, ends the prior attempt lease
through domain commands and copies the retained workspace into a new private
`continuation-UUID/work/tree`. A new attempt and epoch run the original objective,
checks and independent review. Old attempt files and evidence remain untouched.
Required checks and objective must match the stored plan; no task or source reset
is used. A stale request is refused instead of silently adopting newer state.

This supports interrupted verification after a normally exited, fenced writer
and an observed stop of the accepted restricted file-only writer. The latter
requires both settled wrapper observation and a valid restricted tool envelope.
A mid-writer crash with no confirmed fence still blocks continuation. No elapsed
time or missing PID is accepted as fence proof, and no blanket automatic retry
or generic command-capable writer recovery is claimed.

The queue response omits stored plan bodies. Authentication stays in host process
memory and the dispatcher supplies only approved environment keys to native
execution. Native pin/conformance checks remain mandatory; a changed executable
symlink is not silently admitted. Queue errors do not echo raw plan contents or
credentials. The latest 256 receipts are listed; older rows remain in SQLite.

## Verification

The storage regression exercises simultaneous claim attempts, duplicate/colliding
admission, restart recovery, stale boot refusal and completed-claim refusal. Full
workspace checks cover protocol and existing ownership behavior. Real daemon and
browser evidence is retained privately under the workspace review directory;
consult STATUS for the exact accepted run and remaining scope.

## Opt-in crash recovery

Add `--recover-fenced` alongside `--dispatch` to allow at most one automatic
continuation per interrupted workflow. It never retries a normally finished
rejection or failure. Paused, cancelled, completed and operator-blocked tasks
remain unchanged. A missing task or unconfirmed fence remains unfinished.

After stopping/reaping an admitted restricted writer, the trusted wrapper writes
an exclusive private terminal receipt outside the writer's allowed tree before
publishing its terminal protocol frame. The receipt contains packet/attempt and
output digests, never raw model output. On restart and maintenance, the service
reconciles blocked executions against matching regular, single-link, mode-0600,
bounded receipts. Missing, malformed, mismatched or linked files are refused.
A receipt proves the existing trusted CLI capability boundary, not protection
against a hostile process with the same host user identity.

A verified receipt permits the existing retained-work continuation pipeline:
new directory, attempt and epoch; original artifacts retained; checks and an
independent review still required. The durable one-retry limit survives restart.
Further recovery requires explicit operator continuation. A crash during receipt
publication can leave a pending file or a two-link receipt; these conservatively
require operator investigation and are never automatically treated as fenced.

## One subscribed event can admit an approved workflow

An optional `wake_subscription` field names an existing active subscription for
`TASK_ID:worker`. Register that worker with the plan's exact adapter kind,
conformance fingerprint, executable digest and `read_write` access, then register
a non-echo subscription with the desired topic/project/path filters. Submit the
otherwise unchanged native plan with, for example:

```json
"wake_subscription": "consumer-input-ready"
```

Submission authorizes one invocation of that exact plan when an eligible event
arrives. The dispatcher must be enabled. Until then, CLI queue output exposes
`waiting_for_event: true`, and the board labels the entry as waiting for a
subscribed event. Waiting plans count toward the existing 256-plan capacity but
do not prevent an unrelated eligible plan from being selected.

Only pending, available, unexpired deliveries below their retry limit and causal
generation8 are eligible. Source-self deliveries cannot admit a wake. Trigger
context is limited to64KiB. Concurrency remains one running dispatch; plan/task
identity and the existing lease/attempt rules prevent duplicate workers. No
additional plan, subscription, model budget or authority is inferred from an event.

Delivery acknowledgment, retained trigger and dispatch claim commit together.
Acknowledgment means durable admission, not task acceptance. A crash after claim
still requires the existing recovery/fencing rules. The trigger is retained across
explicit continuation and included as labelled untrusted context in the writer's
resume packet. Private-workspace events inherit its correlation, parent event ID
and next causal generation. Direct one-shot launch refuses a gated plan without
its durable wake admission. Required checks and independent verification are
unchanged; a consumed event never marks a task completed.

Storage regressions cover admission rollback for wrong subscriptions, no-event
and source-self refusal, concurrent claims, once-only delivery consumption,
restart/continuation context, expired/leased delivery refusal and queue progress.
Real native acceptance and exact published CI are tracked separately.

The real `wake-workflow-v1` probe observed zero executions before its event,
then one native writer after duplicate publication of the same trigger. Its
answer.txt event inherited correlation and generation1 with the original event
as cause. Required checks passed, but the first independent verifier stopped
without a verdict; the former generic diagnostic does not establish its cause.
The task remained verifying. Retained-work continuation preserved the trigger,
created a new epoch and completed through independent verification in83.27seconds.
Evidence: private `wake-workflow-v1/initial-failure.json` and
`continuation-receipt.json`. No initial failure was rewritten as success.

Subsequent source retains trigger context for the reviewer alongside check
receipts; that final context-preservation adjustment is source-inspected and
covered by workspace checks, not a separate native run. Dispatch now exposes
only allowlisted fixed verifier failure reasons; arbitrary error/vendor text
remains hidden. The board wait label has not yet received a new browser acceptance.

Event routing uses literal, case-sensitive prefixes and rechecks current filters
before delivery leasing or workflow wake. See [EVENT_ROUTING.md](EVENT_ROUTING.md)
for subscription updates and the in-flight acknowledgement boundary.
