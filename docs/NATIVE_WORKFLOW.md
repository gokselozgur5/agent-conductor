# Native task workflow

The opt-in `conductor-daemon --run-plan` mode composes the existing control ports
into one task invocation. It serves the normal local status socket while the task
runs and exits after the outcome. It does not edit or merge the original project.

```sh
cargo build -p conductor-daemon -p conductor-runtime --bins
./target/debug/conductor-daemon \
  --database /absolute/private/state.sqlite3 \
  --socket /absolute/private/conductor.sock \
  --lock /absolute/private/conductor.lock \
  --run-plan /absolute/private/plan.json
```

Use a short socket path (Unix socket path limits apply). The runtime parent
directory must be private. A second daemon cannot take the same runtime lock.
A running task can be inspected or cancelled through the existing `conductorctl`
commands against that socket. Ctrl-C stops this invocation and preserves its
unfinished task/artifacts; it does not claim completion or automatically rerun it.

## Plan and authorization

A plan is an explicit operator authorization to send the named project snapshot
and objective to the chosen model services and execute the listed checks. Do not
include private or unauthorized inputs. The initial Git snapshot includes tracked
and unignored untracked files, including existing dirty changes. Ignored inputs
are excluded. No automatic merge or publication is performed.

The JSON object has these fields (unknown fields are rejected):

- `task_id`: a new task ID. Existing IDs are refused, preserving recovery state.
- `project`: absolute source Git checkout path.
- `objective`: bounded engineering assignment and acceptance requirements.
- `artifacts`: absolute new directory; existing directories are refused.
- `wrapper`: absolute path to the built `conductor-runner` executable.
- `worker`, `verifier`: accepted `VendorPin` objects, each with `kind`, `version`,
  `executable`, `executable_sha256`, and `conformance_sha256`.
- `checks`: nonempty `CheckDefinition` array, each with `id`, exact `argv` and
  `timeout_ms`. Total check deadlines must fit within 50 minutes.
- `toolchain_roots`: optional canonical directories granted read-only to checks.
- `model_timeout_ms`: positive per-model deadline, at most five minutes.

Pins must refer to locally accepted conformance evidence, not a newly computed
hash of an untested executable. Current writer admission supports the restricted
Claude file-tool profile. The distinct read-only verifier can use an accepted
Codex or Claude profile. Model-specific permissions stay in the adapter.

Authentication is inherited from the trusted launching process into the selected
child's explicit environment only: `ANTHROPIC_API_KEY` for Claude, existing Codex
authentication with optional `CODEX_HOME` for Codex, and HOME/LANG/TMPDIR.
Do not put credentials in the plan, shell arguments, receipts or Git. The native
Claude adapter retains its per-call $0.50 cap; that is not a total account budget.

## Execution and outcome

1. Capture a private work copy and register/reuse the actual project identity.
2. Register distinct worker/verifier identities, freeze the attempt and assign it.
3. Launch the writer through persisted preparation/acknowledgement/release.
4. Finalize an accepted normal restricted-writer exit and bind the source copy.
5. Execute each frozen required check in its own isolated copy; persist receipts.
6. If all checks pass, run the read-only verifier against that exact source and
   require a strict task/source-bound JSON decision.
7. Capture the source again, recheck current task version/lease, record the verdict
   and use the existing domain completion gate.

The process prints a JSON outcome containing task ID, source identity, check
receipts, completion flag and artifact path. Zero exit means completed. A failed
check or rejected review leaves the task verifying and returns nonzero. Setup,
protocol, timeout, cancellation or binding errors retain unfinished durable state.
The model's raw result is not a command and cannot directly complete the task.

## Verified evidence and remaining scope

The first real synthetic run used pinned Claude 2.1.311 to create `answer.txt`,
a real isolated content check, and a distinct pinned Codex 0.163.0 read-only
reviewer. It reached durable `completed` in 71.36 seconds. Database inspection
confirmed different worker/verifier IDs, a passing check and matching source
identities. The original checkout remained unchanged. Private evidence is under
`reviews/agent-conductor-runtime-20261028/native-workflow-v1` in the workspace;
no project input or model output was committed.

A second actual run against the same registered project and database completed
in 65.08 seconds with the same source identity and an unchanged original checkout.
Its private receipt is under `native-workflow-v2/`.

These are two successful native workflows, not complete-product acceptance.
Automatic continuation after interruption, all failure-path native acceptance,
continuous scheduling, UI, hooks, Thor and long-running hardening remain open.
The restricted-writer fence relies on the trusted pinned CLI capability boundary,
as documented in ADAPTER_CONFORMANCE; it is not a generic escaped-process fence.
