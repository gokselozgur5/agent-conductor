# Local operator setup

## Connect another account to the coordinator

The board exposes the existing control protocol at `POST /api/rpc`. An account
with an operator-provisioned credential can use `conductor rpc` without access to
the daemon's private state directory or Unix socket:

```sh
printf '%s\n' '{"type":"health"}' | ./conductor rpc \
  --address 127.0.0.1:8790 --token-file /absolute/private/operator-token
```

Use `tools/conductor` instead of `./conductor` from a source checkout. The token
file must be a private regular file owned by the calling account. The operator
provisions it explicitly; the client never copies credentials between accounts.
The credential grants full operator control, like the local socket, and is not
an agent-scoped capability. Do not place it in a prompt, command argument or URL.

Supply a protocol request payload on stdin or with `--request-file`. Registration,
task commands, checkpoint queries and event subscription/publication/claim/ack
use the same request fields as the Unix protocol. The client adds and verifies
the protocol envelope and prints the full correlated response. Structured errors
retain the Rust CLI's exit codes. There are no automatic retries: preserve the
operation's idempotency key and reconcile state after a timeout before retrying.

This transport accepts numeric loopback addresses only and ignores proxy
environment variables. Accounts on this machine can coordinate without changing
their HOME or vendor authentication. Native dispatcher execution still runs as
the daemon account; RPC registration does not launch a worker under another UID,
transfer its workspace, or prove remote-host execution. Those integrations remain
required for automatic cross-account native workflows.

## Initialize a local runtime

The `tools/conductor` launcher uses the Rust daemon and CLI for every state change.
It supports Python3 and the current Unix host. Native isolated checks currently
require macOS; this launcher does not add Linux isolation or remote-host support.

From the repository root, build and initialize once:

```sh
cargo build --workspace --release
./tools/conductor init --bin-dir "$PWD/target/release" --project /absolute/path/to/your/git-project
```

The project must have an existing Git commit. Initialization creates private
`state/operator` configuration, board credential, plan and artifact directories.
It does not call a model or modify project files. Existing state is never replaced.
For a long checkout path, use a short absolute `--state` path on every command.

Start the foreground runtime and open the board:

```sh
./tools/conductor run --dispatch
```

Open `http://127.0.0.1:8787`. In another terminal, copy the private token into its
login field. On macOS:

```sh
./tools/conductor token | pbcopy
./tools/conductor status
```

Ctrl-C stops the daemon and preserves its database and artifacts. Run the same
command again to restart. A second owner is refused. Omit `--dispatch` to inspect
or queue plans without starting models. `--recover-fenced` additionally enables
the existing bounded, verified-fence recovery policy.

The launcher first runs a short registration-only daemon, then starts the normal
board/watcher daemon against the same database. It inherits existing model
credentials. If Claude needs an API key, pass `--anthropic-key-file /private/key`;
the owned mode0600 regular file must contain exactly one key. The value stays in
the process environment, is never placed in argv/configuration, and is not printed.
The board token is separately generated and stored mode0600; worker environments
do not inherit it. `token` prints it only when explicitly invoked by the operator.

## Prepare and submit a task

Use a previously accepted native plan as the adapter template. The launcher
rechecks its executable hashes and resolves their immutable paths. It does not
certify an unknown CLI by hashing it. Write your objective in a UTF-8 text file and
your exact required checks in a JSON array, for example:

```json
[{"id":"tests","argv":["/absolute/path/to/test-command"],"timeout_ms":60000}]
```

```sh
./tools/conductor plan --from-plan /private/accepted-plan.json \
  --task first-task --objective-file /private/objective.txt \
  --checks-file /private/checks.json --instruction /approved/AGENTS.md
./tools/conductor submit state/operator/plans/first-task.json
```

Repeat `--instruction` in outer-to-inner order for each explicitly approved file.
Omitting it approves no instruction sources; repository text remains advisory.
Plan preparation writes a new private plan but starts no model. Submission is the
explicit authorization to execute the selected model workflow and declared checks.
The original checkout stays unchanged; accepted output lives in the plan's private
artifact directory. Publication and merge remain separate operator actions.

To build on a predecessor's independently accepted output, submit the predecessor
first, then prepare a successor with explicit dependencies:

```sh
./tools/conductor plan --from-plan /private/accepted-plan.json \
  --task second-task --objective-file /private/second-objective.txt \
  --checks-file /private/second-checks.json \
  --depends-on first-task --input-task first-task
./tools/conductor submit state/operator/plans/second-task.json
```

Repeat `--depends-on` for additional prerequisites (up to 16). The input task must
be one of them, in the same project. The dispatcher waits for independent accepted
completion and copies the selected accepted tree into the successor's private work
area. Without `--input-task`, dependencies only gate execution; the starting tree
comes from the original project. There is no automatic merge of multiple outputs.
The board and `list-workflows` show pending admission and frozen source identities.
See [verified handoff](WORKFLOW_DEPENDENCIES.md) for refusal and recovery rules.

To wait for another agent's semantic event, submit with a topic filter:

```sh
./tools/conductor submit state/operator/plans/first-task.json \
  --on-topic 'file.*' --path-prefix src
```

This registers the planned worker with its exact adapter identity, installs a
non-echo project subscription, and submits a derived immutable gated plan. The
source plan is unchanged. Repeated identical submission is idempotent. Do not
submit the same task both immediately and gated; use distinct task identities.
See [workflow dispatch](WORKFLOW_DISPATCH.md) for eligibility, acknowledgment,
causal bounds and retained-work recovery. CLI and board show waiting status.

Forward any existing CLI command through the configured socket:

```sh
./tools/conductor ctl --json list-tasks
./tools/conductor ctl --json list-workflows
./tools/conductor ctl check-invariants
```

## Evidence and limits

Real daemon/HTTP acceptance verified private modes, ready/running watcher health,
unauthenticated API refusal, duplicate-owner refusal, restart and absence of the
board token from logs, without model calls (`operator-launcher-v3/receipt.json`
in the private review directory). An initial fixture had no Git commit and produced
a correctly degraded watcher; initialization now rejects that case up front.
Plan creation, CLI forwarding, idempotent gated submission and preservation of
the source plan also passed with dispatch disabled. This is local launcher
acceptance, not complete v1 or multi-host acceptance.

An isolated real Chrome run also displayed the gated plan's waiting label and
running watcher, refused unauthenticated queue access, and kept the token out of
browser storage, rendered text and daemon logs. Evidence:
`operator-launcher-browser-v1/receipt.json`. Its dispatcher stayed disabled and
its browser/daemon were stopped after the probe.

## Inspect project context

```sh
./tools/conductor inspect
```

This read-only command reports conventional continuity/instruction document paths,
digests, starting Git HEAD/status digest and a baseline manifest identity. It uses
the same bounded Git inventory as runtime capture, excludes ignored files and
refuses unsupported layouts/links. It does not alter HEAD, index or files, load
whole documents into the prompt, or grant instruction authority from filenames.
Direct CLI equivalent: `conductorctl inspect-project /absolute/project`.

Native workflow packets now include the same advisory references from their
already captured work copy, not a fresh read of the original checkout. The reviewer
retains them as labelled starting-baseline data alongside check receipts. Up to64
references and16KiB of encoded reference entries are included, with an explicit
omitted count. Supported conventional names include README, STATUS, PHASES,
TRACKING, ARCHITECTURE, REQUIREMENTS, DECISIONS, HANDOFF and TODO (Markdown, text or
RST), plus scoped AGENTS.md and CLAUDE.md. Paths sort outer-to-inner then lexically.

Outer-workspace files outside the captured project still require explicit
`--instruction` selection and pins. This does not implement arbitrary filename
configuration or full outer-workspace advisory discovery. A model must choose
relevant references to read; listing a document is not proof it was read.

Runtime tests cover count/encoded-byte bounds, escaped paths, deletion exclusion
and unchanged authority. The real CLI probe found six actual documents in this
repository with unchanged HEAD/index/worktree status and no ignored-state paths.
Evidence: private `advisory-intake-v1/receipt.json`. Native packet integration is
source-inspected/service-tested; no new model call was made for this change.

## Inspect task history

Select **View history** on a task card to read audit metadata. **Older entries**
pages backward; **Newest entries** explicitly refreshes. Disconnect clears the
selected history. The same read is available locally:

```sh
./tools/conductor ctl --json task-history TASK_ID
./tools/conductor ctl --json task-history TASK_ID --before SEQUENCE
```

Use the returned `next_before` cursor unchanged. Each page contains at most 100
entries; command/checkpoint payloads are omitted. This view does not acknowledge
semantic events or change the task. See [TASK_HISTORY.md](TASK_HISTORY.md).

## Initial evidence during continuation

Keep the task's original artifact directory, including `work/manifest.json`.
Continuation copies retained unfinished files into a new attempt, but preservation
review now uses the initial task manifest bound to the first durable execution.
A missing, linked, changed or unbound initial manifest refuses continuation before
another attempt starts. The task and prior work remain intact. Older packets that
lack a baseline digest cannot establish this evidence; do not manufacture a new
baseline from their dirty work to bypass the refusal.

Initial decisions can be supplied with `plan --decisions-file /absolute/decisions.json`.
The file is an array of objects with `id`, `question` and optional `rationale`.
See [TASK_DECISIONS.md](TASK_DECISIONS.md) for limits, attribution and pause/resolve
behavior; an open question blocks completion, not initial execution.

For explicit successor selection after a verified stop, see
[WORKFLOW_REASSIGNMENT.md](WORKFLOW_REASSIGNMENT.md).

## Outer-workspace advisory sources

Use `tools/conductor inspect --workspace-root /absolute/workspace` to review
bounded source candidates. The same option on `plan` freezes untrusted data pins;
repeated `--document relative/path` selects repository-specific names. These are
separate from authoritative `--instruction` pins and add no native read roots.
See [SCOPED_ADVISORY.md](SCOPED_ADVISORY.md) for bounds and change handling.

## Cold state snapshots

After stopping and reconciling the runtime, `tools/conductor backup DESTINATION`
exports private state and `tools/conductor verify-backup DESTINATION` checks it
without starting workers. See [OPERATOR_BACKUP.md](OPERATOR_BACKUP.md) for the
fencing requirements, external dependencies and pending restore limitations.

## Host-local prebuilt preview

`tools/package_local.py --bin-dir /absolute/built/bin --accepted-plan /private/accepted-plan.json --destination /new/private/bundle`
creates a separate bundle with exact binaries, operator tools, documentation and
a content manifest. It never copies model credentials, project files or runtime
state, and refuses an existing destination or changed adapter binary. Packaging
is not adapter conformance or full release acceptance. Use an already accepted
Claude-writer/Codex-reviewer plan; the profile remains specific to this host.

Inside the bundle, `./conductor init --project /absolute/git-project --port 8788`
selects the bundled binaries. `./conductor plan` selects its accepted profile by
default, so no private test-plan path or build toolchain is needed to operate it.
The remaining arguments and authorization rules are identical to `tools/conductor`.
The manifest records whether packaging source was dirty and hashes all shipped
files. Use a clean committed source and separately validate the actual workflow
before handing a bundle to an operator.

The host-local release bundle was exercised through its own launcher with two
linked native tasks: Claude wrote each task, isolated checks passed, and distinct
Codex reviewers accepted both outputs. The second task consumed the first task's
exact accepted source identity. Independent SQLite/file verification confirmed
one completion per task, four settled auxiliary invocations, both fenced writers,
unchanged original files and 42 healthy invariant samples. A stopped/start/read/stop
rehearsal preserved task aggregates, executions, publications and dispatch outcomes.
The bundle's 26 shipped files retained their manifest hashes. Evidence is retained
privately as `operator-bundle-native-v2/receipt.json` and
`operator-bundle-native-v2/independent-verification.json`.

The earlier debug bundle is not accepted: its second verifier failed during
startup, leaving unresolved auxiliary ownership. A process sample observed
executable SHA-256 validation; that sample does not establish the sole cause of
the failure. Its extended observation, failure and owned-process cleanup records
are preserved separately; no pending state was cleared or upgraded. The release
exercise used a fresh state and project, not a repaired or relabelled debug run.
Use release binaries for the operator workflow. This two-task local exercise does
not establish native Codex writing, Linux isolation, full endurance or complete v1.
