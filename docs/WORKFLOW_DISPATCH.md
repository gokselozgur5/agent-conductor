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
- `needs_recovery`: an earlier boot held the claim when it stopped. No automatic
  relaunch occurs. Task, execution records and private artifacts remain available.

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

This supports interrupted verification after a normally exited, fenced writer.
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
