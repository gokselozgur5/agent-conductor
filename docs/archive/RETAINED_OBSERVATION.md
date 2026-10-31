# Retained workspace observation

A private workspace can outlive its worker's lease. The watcher previously selected
only started executions and skipped invalid ownership, so those changes vanished
from the observation path. A deterministic regression on `d160d9d` wrote a file
after lease expiry and received zero events instead of an external observation.

## Attribution and routing

The bounded execution page now selects the current acknowledged attempt of each
nonterminal task, including started, exited, blocked and fenced records. Superseded
attempts, unacknowledged preparations and terminal tasks are not monitored by this
path. The existing sixteen-record pages and 256-entry active debounce bound remain.

A valid live lease on a started/exited execution retains ordinary `file.*` events
with its worker, attempt, epoch and inherited trigger correlation/generation.
Otherwise, an explicitly configured registered read-only observer can publish:

- `workspace.external.created`
- `workspace.external.updated`
- `workspace.external.deleted`

These events identify the observer as source. The task is an association, and
`payload.observed_attempt_id` identifies the retained workspace being inspected.
`attempt_id`, `lease_epoch` and `causation_id` are absent; generation is zero and
the observation starts its own correlation. The payload says
`attribution: external_workspace`, containing file identities/digests, not text.
It makes no claim that the former worker wrote the bytes or still owns the lease.

Existing `file.*` subscriptions do not match this separate topic family. A consumer
must explicitly subscribe to `workspace.external.*` or a more general filter. This
does not silently introduce new wake triggers for ordinary file subscribers.
Unknown causation is not reconstructed from a former task's trigger. These events
cannot establish a process fence, make blocked work resumable or complete a task.

## Scope and durability

The daemon's existing `--watch-config` enables retained observation for its
registered projects. Its source agent must be registered with `read_only` access
for external workspace observation. Multiple configured observers for one project
select the lexicographically smallest source ID deterministically. No configured
scope means no external observation. The operator launcher already registers a
read-only observer.

Packet hash, task/attempt/worker/epoch binding, current task identity and durable
execution record are checked before and after filesystem observation. A changed
classification or replaced execution discards the sampled scan; corrupt packet
identity refuses observation. Existing safe tree reads and debounce remain in use.
This is workspace attribution, not OS-level authorship proof or a lossless journal
of files that appear and disappear between samples.

The publication baseline is shared with prior owned observations, so an ownership
change does not relabel already published events. Snapshot advancement, events and
deliveries commit atomically. Restart reuses the durable baseline and re-applies the
explicit observation configuration. Task state and recovery eligibility stay under
the existing lifecycle and fencing rules. There is no schema migration.

## Evidence and remaining scope

The old-source regression fails at missing delivery after expiry; the corrected
implementation delivers the event with observer identity and no lease claim.
Service/wrapper controls cover live ownership preservation, disabled scope,
paused/fenced and blocked outcomes, service restart, deduplication, changed packet
refusal and terminal exclusion. They are not vendor conformance or containment
proof. An early test helper incorrectly attempted fixture mutation through the
read-only pool; it was corrected to a dedicated fixture connection, and its failure
is not counted as a product defect.

Full local validation passes 242 tests, strict Clippy and a release build. A real pinned
Claude task was paused and fenced, then observed after daemon restart: creation,
update and deletion produced three external-consumer deliveries, zero ordinary
file-subscriber deliveries and no duplicate publication on a second restart. The
task remained paused and the original project stayed unchanged. This acceptance
took 16.25 seconds; it is not task completion or a new verifier run. Evidence is
`unowned-native-v1/receipt.json` in the private runtime review directory. The source
inventory and baseline remain bounded; no new CPU or latency bound is claimed.

This does not watch all historical/orphan/terminal worktrees, recover an unfenced
writer, infer causal links for external changes, or establish full issue #19 / v1
acceptance. Broader causal-chain and operational acceptance remain separate.
