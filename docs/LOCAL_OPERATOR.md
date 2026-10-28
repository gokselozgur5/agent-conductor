# Local operator setup

The `tools/conductor` launcher uses the Rust daemon and CLI for every state change.
It supports Python3 and the current Unix host. Native isolated checks currently
require macOS; this launcher does not add Linux isolation or remote-host support.

From the repository root, build and initialize once:

```sh
cargo build --workspace
./tools/conductor init --project /absolute/path/to/your/git-project
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
