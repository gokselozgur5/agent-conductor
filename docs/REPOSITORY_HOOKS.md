# Repository observation

The opt-in daemon watcher turns repository observations into the existing durable
semantic-event/delivery system. It never directly launches a model. Automatic wake,
owned-attempt attribution and automatic wake are still pending.

Register the project and a dedicated read-only observer agent through the CLI.
Create subscriptions with `file.*` and optional project/path filters; use
`git.head_changed` for HEAD observations. Source echo remains disabled by default.
Then provide `--watch-config /private/watches.json` to the persistent daemon:

```json
[
  {
    "id": "project-files",
    "project_id": "registered-project",
    "source_agent_id": "registered-observer",
    "root": "/absolute/canonical/project"
  }
]
```

Configuration accepts at most 16 distinct watch IDs and 64 KiB. It conflicts with
one-shot `--run-plan`; it can run alongside explicitly enabled `--dispatch`.
Root must be canonical and match the registered project filesystem/Git identity.
Changing a persisted watch configuration under the same ID is refused. No Git
hooks are installed and no repository content/index/HEAD is changed by observation.

Initial observation establishes a baseline without publishing existing files as
new changes. Subsequent scans compare bounded file-content identities rather than
Git porcelain text alone. Ignored files are excluded, tracked deletions are
reported, and unsupported links/layouts fail closed. Limits inherit the snapshot
reader: 10,000 paths, 32 MiB per file, 512 MiB aggregate. No file content is stored in event
payloads: events include paths, before/after digests, sizes and executable flags.

Polling waits 250 ms between sequential scan rounds. A 750 ms quiet period coalesces
bursts; continuous observed changes become eligible after 5 s. These intervals do
not include or override bounded Git/read execution time. Scans interrupted by
concurrent source changes retry without publishing a false deletion. An unsupported
observation or publication failure stops the watcher with a generic diagnostic;
the daemon remains available. Check watcher stderr as well as daemon health.

Each publication handles at most 256 net changes. Its semantic events, matching
deliveries and new baseline commit in one transaction. Remaining paths retain the
prior baseline and drain in later rounds. A restart either sees the old baseline
and republishes its differences, or sees the already committed new baseline;
there is no separate event/checkpoint crash gap. Config/version checks reject
concurrent or stale publication. Graceful shutdown lets in-flight publication
finish; process loss relies on SQLite rollback.

The watcher reports unowned repository observations as external changes. It does
not infer which agent wrote a path merely from a current task lease. Events are
`file.created`, `file.updated`, `file.deleted` or `git.head_changed`; project/path
subscriptions and existing generation/fanout/delivery limits apply. Transient file
states between scans may coalesce away. This is change observation, not a complete
filesystem audit or an atomic source-verification snapshot.

## Current evidence

A real synthetic Git repository, daemon and CLI run verified a ten-write burst
coalesces into one creation, path-filtered delivery, source-echo suppression,
content-free payloads, updates/deletion, ignored exclusion, a change made while
the daemon was stopped delivered once after restart, and a Git HEAD event.
Private evidence: `reviews/agent-conductor-runtime-20261028/hooks-v1/receipt.json`.
No model call occurred. A separate transaction failure regression proves an event
batch and baseline roll back together; deterministic tests cover a 600-path storm
draining in three batches and continuous-change debounce limits. This does not
prove owned-worktree attribution, instruction invalidation or automatic wake.

The first full local suite hit an existing socket-permit recovery test's one-second
observation deadline. The exact unchanged test passed alone in 0.40s; the initial
failure remains recorded in `hooks-workspace.log`. This does not establish its
cause. A full rerun also failed two worker-start observation deadlines, and a serial run failed the live positive control in `stopped_observation_allows_prior_writes_but_no_later_writes`. Serial execution therefore does not establish that this is only parallel scheduling pressure. Issue #20 tracks diagnosis; the original failing runs remain preserved.


A controlled delayed-start probe (`readiness-v1/receipt.json`) established a
specific fixture defect: with a 300ms worker delay and150ms heartbeat, no renewal
correctly produces `lease_or_runtime_expired` before readiness; renewing until
readiness allows the same worker to start and then report `cancelled`. The stop
tests now renew during bounded readiness/progress observation, then intentionally
stop renewal or cancel. Parent-loss probing releases a descendant write gate only
after the stop observation, avoiding historical-write confusion. All11 runner
tests passed with these changes. Production timeouts and fencing are unchanged.
This does not establish the cause of the earlier socket-permit timeout.

The final default-parallel full workspace run passed all 166 tests after the
readiness correction (`hooks-workspace-readiness.log`); strict Clippy, formatting
and credential/media scans passed. Exact-head remote CI is still required.

## Instruction boundary in progress

The runtime now reads operator-pinned instruction files before wrapper ACK and
release, rather than echoing unverified hashes. Every renewal rechecks the pins;
a mismatch or unavailable source stops the worker with
`instruction_changed_or_unavailable`. Native command construction includes only
verified UTF-8 texts in frozen order. Files are limited to 64 KiB each and 256 KiB in
aggregate; missing files, symlinks, hardlinks, special files and hash mismatch
are refused. Explicit approved pins can name exact canonical files outside the
workspace; their parent directories are not granted to native model tools.

Regressions demonstrate refusal before ACK/release, a live worker stopped after
instruction replacement, rejected bounded/file-type/content cases, ordered prompt
text and debug redaction. These are synthetic local process/command tests, not
new native model conformance runs. Native workflow plans now carry explicit
ordered pins. Maintenance blocks active tasks when original instruction bytes
change, preserves operator pause/cancel, and persists the block across restart.
Continuation revalidates the original pins; approving new hashes requires a new
plan. See [native workflow instructions](NATIVE_WORKFLOW.md#explicit-approved-instructions).
Automatic advisory discovery, owned attribution and eligible wake remain open;
these changes do not complete the full repository-hook requirement.
