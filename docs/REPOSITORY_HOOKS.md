# Repository observation

## Current issue #19 acceptance (2026-10-28)

The scoped repository-hook integration is accepted. The historical milestones
below retain their original limitations; this section supersedes their pending
attribution, instruction and wake statements. This is not full-product acceptance.

| Requirement | Implementation and evidence |
| --- | --- |
| Bounded observation and content-free events | Native dirty hints, bounded reconciliation, debounce and transactional batch drain described below. `hooks-current-v1` repeated real burst/create/update/delete/ignore/HEAD checks on release `fb6545e`. `native-watch-validation-manifest.json` retains linked-worktree, root-loss and 300-path drain evidence. |
| Durable restart publication | Baseline and event/delivery writes share a transaction. Current real daemon SIGKILL/restart probe observed the stopped-period change once; transaction cancellation and lost-response controls are documented in STORE_TRANSACTIONS.md. This is not a physical power-loss durability claim. |
| Filtering, echo and attribution | EVENT_ROUTING.md covers literal project/path/topic matching and retry admission. Current probe passed path separation and source echo suppression. Valid owned packets retain attempt/epoch; configured retained nonterminal workspaces without current ownership publish external identity. `unowned-validation-manifest.json` includes forged-packet and restart controls. |
| Active instruction invalidation | `instruction-change-native-v2` changed an approved file after a real pinned Claude writer created output. The task blocked with `instruction_changed`; execution became fenced in 0.15 seconds. Continuation was queued but dispatch refused the changed pin before task mutation or another execution. Restoring bytes did not resume the task. |
| Bounded wake and causal refusal | `causal-validation-manifest.json` binds two-consumer generations 0–8, generation-9 refusal, crash/lease/replay controls, and a real Claude/checks/Codex workflow whose generation-8 owned output could not wake a successor. Delivery acknowledgement and admission are atomic; repeated claims do not grant duplicate leases. |
| Opt-in operation | Explicit watch configuration is required. Fixtures use isolated repositories and databases. No automatic user repository hook installation or source edits are performed. |

The new instruction probe also restarted observation of the fenced retained tree:
create/update/delete yielded three external deliveries, zero ordinary file
subscriber deliveries, no lease/causation claim or raw content, and no duplicates
after another restart. The task stayed blocked and the original checkout stayed
clean. Elapsed time was 19.61 seconds, not a latency guarantee.

Private evidence lives under `reviews/agent-conductor-runtime-20261028/`:
`hooks-acceptance-manifest.json`, `hooks-current-v1/receipt.json`, and
`instruction-change-native-v2/receipt.json`. The first instruction probe stopped
at an incorrect synchronous-refusal assertion: the API acknowledges queueing,
while the dispatcher validates pins before lifecycle mutation. The corrected
probe tests that actual contract; no production refusal was weakened.

Limits remain: bounded snapshots can miss transient intermediate filesystem
states; watcher process health is not a freshness guarantee. Retained observation
requires configured scope and excludes arbitrary orphan, superseded and terminal
trees. The causal proof is not eight model-driven hops or detection of arbitrary
external causal relationships. Host/Thor/Linux check isolation, issue #20,
load/restore/redaction, broader recovery and seven-day soak remain separate work.


The opt-in daemon watcher turns repository observations into the existing durable
semantic-event/delivery system. It never directly launches a model. Native private
workspaces carry authoritative attempt identity; explicitly approved subscribed
plans can be admitted by the separate dispatcher (see WORKFLOW_DISPATCH.md).

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
the daemon remains available. Health reports `repository_watcher` as `disabled`,
`starting`, `running`, `stopped` or `failed`. Requested observation is degraded
until its first scan round completes, and stopped/failed observation keeps overall
health degraded. CLI health and the authenticated board expose the same state.
Task cancellation or panic also marks the watcher failed; a graceful shutdown
marks it stopped. This is process-liveness reporting, not a scan freshness bound
or proof that every transient filesystem change was observed.

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

## Owned workspace integration under validation

Native launch persists a private-tree baseline before wrapper release. Configured
project watchers sample started native execution trees and revalidate the durable
execution and monotonic task lease under lifecycle serialization before atomic
publication. Events carry worker, task, attempt, epoch and spawn-effect correlation
at generation zero for these operator-launched workflows. Attribution describes
workspace ownership, not proof of the OS process responsible for each write.
Fake executions are excluded; missing baselines fail closed.

The full workspace suite and strict lint pass. The strengthened lifecycle test
separately verifies creation/deletion, repeat-scan deduplication, actual delivery
identity and raw-content exclusion. Real daemon delivery acceptance is recorded
below; additional stale-owner and interruption cases remain part of full acceptance.
Expired ownership is tested to publish no event. Started executions are selected
in ordered pages of at most 16 per configured project per round, with a wrapping
cursor so a busy early execution does not permanently hide later records. FR-7 and FR-8 are not complete.

A real native workflow passed owned observation acceptance in75.91seconds:
queued submission survived restart, pinned Claude created answer.txt, the watcher
published its actual task/attempt/epoch/spawn correlation, and the separate
subscribed consumer received one durable delivery. Checks and independent pinned
Codex verification completed the task. Original project and approved outer policy
remained unchanged. Evidence: private `owned-workspace-v2/receipt.json`.
The first v1 probe refused a changed Claude symlink before model launch; v2 used
the previously accepted retained2.1.311 binary. This proves delivery to the
consumer queue, not automatic adapter wake or consumption acknowledgment.


## Coalescing and final captured changes

Private-workspace scans now use the same750ms quiet /5s maximum observed-burst
eligibility as external observation, including their first changed observation.
The authoritative baseline still precedes worker release. A configured project's
ordered execution pages retain at most256active debounce entries per scan cycle;
finished entries are removed, cache replacement remains bounded, and exceeding
active capacity fails observation explicitly. Timing excludes scan/database costs.

An accepted normally exited file-only writer already supplies a fenced source
capture for verification. For configured projects, finalization now drains that
capture's remaining differences before entering verification, using existing
atomic baseline/event transactions of at most256events each. The lifecycle lock
and per-batch ownership revalidation prevent stale-lease publication. The bounded
capture is reused; there is no extra read of a possibly changing live tree. A
short-lived writer's final changes therefore do not depend on a later poll.
Observation-disabled projects do not acquire this terminal event publication.

The controlled lifecycle regression produces300files after its last periodic
scan, verifies300identity-bound deliveries through final capture, and checks the
same operation publishes none when observation is disabled. It also verifies
repeat polling produces no duplicates. This is a service/wrapper fixture, not a
new native model run. Abnormal termination, revoked ownership and writes that
appear/disappear between sampled captures remain outside the normal-exit coverage;
this does not claim a complete filesystem audit or verified task acceptance from
a file event. Later verification failure can coexist with valid change events.

The updated native path also completed a real Claude/checks/Codex workflow in
237.89seconds (`owned-terminal-native-v1/receipt.json`). Its answer.txt event was
delivered once with the actual task/attempt/epoch, original project and approved
policy stayed unchanged, and the persisted writer packet contained the advisory
README reference. This integrated run does not distinguish periodic publication
from final-capture publication; the300file controlled regression above specifically
covers the latter. No additional model run was made for the subsequent board text
clarification showing blocked resume conditions and the existing workflow queue.

## Idle observation design

Issue #33 measured non-negligible idle CPU. On the same project/database seed,
30-second process-lifetime observations reported approximately 0.75% of one CPU
with the debug watcher disabled, 45.19% with debug polling, and 28.99% with release
polling. `wait4` includes waited child CPU here, confirmed by a separate nested-child
control. Earlier `ps -S` samples did not expose that child cost on this host; retain
them as raw process samples, not complete-tree accounting. Sampling also found
unoptimized SHA-256 work. No universal CPU bound is inferred.

Use native filesystem notifications only as a hint to perform the existing bounded
Git/content observation. Keep 250 ms scheduling, 750 ms quiet debounce, 5 s observed
burst eligibility and bounded draining. A pending debounce/drain continues polling;
quiet watches reconcile fully every 5 s even without notification. Missed native
notifications can therefore delay discovery up to that reconciliation interval
plus observation I/O and debounce; this is an explicit fallback, not a claim that
notifications are lossless. Active owned-attempt observation remains unchanged.

Register the project, resolved Git directory/common directory, and nonrecursive
parents before the first scan. Recheck roots after scans. Parent changes relevant
to a moved/deleted root trigger validation; unrelated sibling paths do not. Use
constant-size dirty/error flags instead of an unbounded event queue. Access-only
notifications do not trigger self-scans. Backend creation/watch/error failures
stop observation with degraded health; overflow/rescan hints force a full scan.
Events never replace content hashes or project/lease validation and never grant
worker launch authority.

The implementation uses the native backend selected by
[notify](https://docs.rs/notify/8.2.1/notify/) and its
[watch modes](https://docs.rs/notify/8.2.1/notify/trait.Watcher.html). Network or other
filesystems may omit native events; the periodic reconciliation remains necessary.

Controlled before/after acceptance (`idle-controlled-v3/receipt.json`) used the
same detached f83fbf7 project, SQLite seed and release configuration, with no
HTTP, dispatcher or models. Over 30-second samples, waited lifetime CPU fell
from 9.390903 seconds / 30.245466 seconds (31.05% of one CPU) to 0.872194 /
30.286177 (2.88%), approximately 90.7% lower. Lifetime accounting includes startup
and waited children. This is one host/workload, not a universal bound or soak.
Earlier fixture attempts retained a conflicting old observation configuration and
were refused; their incomplete measurements are not included.

A native linked-worktree process probe passed burst coalescing before fallback,
path routing, source echo suppression, content omission, update/delete, ignored
paths, restart reconciliation exactly once, external Git HEAD notification and
root-loss degraded health. All 300 distinct burst paths drained in 1.63 seconds.
These are filesystem-to-event checks, not new model execution or full wake-latency
acceptance. Scheduler tests cover missed hints, late hints, settling and backend
failure; native notifications are exercised on CI hosts.

## Retained nonterminal workspaces

Configured observers now monitor current retained attempts after ownership loss.
Changes use a separate `workspace.external.*` topic with observer identity and no
lease or causal claim; ordinary `file.*` subscriptions remain separate. See
[RETAINED_OBSERVATION.md](archive/RETAINED_OBSERVATION.md) for current boundaries, source
validation and replay behavior. Scoped outer document discovery is documented in
[SCOPED_ADVISORY.md](SCOPED_ADVISORY.md); earlier sections preserve historical gates.

Event routing uses literal, case-sensitive prefixes and rechecks current filters
before delivery leasing or workflow wake. See [EVENT_ROUTING.md](EVENT_ROUTING.md)
for subscription updates and the in-flight acknowledgement boundary.
