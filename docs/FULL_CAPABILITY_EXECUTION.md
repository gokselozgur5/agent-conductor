# Full-capability agent execution

## Accepted direction

The operator requires agent freedom, including shell, builds/tests, tool selection
and network use. Conductor is the coordinator, not a replacement coding harness.
The historical two-file-tool broker profile remains useful evidence about bounded
file effects but is insufficient for the requested product.

## Selected execution boundary

The operator selected distinct operating-system user accounts with machine-wide
access under each account's OS permissions. Workers are not confined to a project
folder or required to run inside a VM/container. The selected harness invocations
are `codex --yolo` and `claude --dangerously-skip-permissions`, with working roots
following the operator's `$HOME/codex` or `$HOME/claude` convention per account.
These are working-directory conventions, not additional filesystem restrictions.

Preserve each account's own home, vendor login and environment. Conductor must
launch as the selected account, record that identity with the attempt and keep
ownership stable across restart/reassignment. Do not substitute the daemon user's
credentials or a restricted file-only profile. Account separation does not itself
prove that a stopped process left no descendants; recovery must still report the
observed process state accurately.

## Implemented workflow and remaining gates

- Native plans now accept `full_capability` with a frozen OS account and PATH.
  The worker process must already run as that account. The operator plan command
  preserves the profile from its accepted template. Authenticated loopback RPC
  permits control-plane clients in another account without sharing the private
  daemon directory. Normal account-channel launch and artifact transport are
  implemented. Controlled distinct-UID fixtures pass on macOS and Linux; personal
  vendor login deployment under distinct accounts and started unrestricted worker
  recovery remain open.
- `native_writer_launch` routes explicit full profiles to Codex `--yolo` or Claude
  `--dangerously-skip-permissions`. Both use the selected account's own HOME and
  configuration. Restricted profiles retain their previous behavior.
- Full native dispatch sends only the frozen packet and selected vendor pin to
  the runner's `--account-native` entry. The receiver verifies its OS identity and
  executable bytes, then builds CLI arguments and HOME locally before Ready.
  It still waits for a durably recorded acknowledgement and a current lease
  before launching. Local dispatch uses this stdio entry; plans with
  `account_worker` select the persistent account-channel receiver instead.
- The runtime's same-host account channel authenticates both kernel peer UIDs
  and binds one connection to an exact packet hash. It requires an existing
  receiver-owned directory that the coordinator can traverse; it never changes
  account credentials or directory permissions. Socket admission alone grants
  neither execution nor a process identity or fence. Snapshot bytes can cross
  this channel with the existing independently pinned manifest verification.
  Same-UID tests do not establish a distinct-account deployment.
- The runtime entry `conductor-runner --account-listen SOCKET COORDINATOR_UID
  OWNER_TAG` receives one authenticated channel and uses the same native packet,
  Ready/Release, lease watchdog and terminal-receipt path as the stdio runner.
  `AccountRunnerConnection` verifies the frozen account/digest and calls the
  controller's acknowledgement-persistence hook before Release; a refused or
  expired hook closes the channel without starting a worker. It deliberately
  supplies no child-process identity and cannot attest receiver exit or fencing.
  The socket request also names the exact authority/packet/wrapper identity; a
  receiver with a different pre-recorded tag refuses before Ready.
- `PersistentControlService` can prepare fresh account execution intent, release
  it once after a committed account ACK, and renew only the current binding/lease.
  Schema 31 records an explicit account peer independently of local process data.
  Transport selection is immutable and recorded before connection; recovery never
  treats an account workspace's receipt files as a local write fence, including
  when no peer ACK was recorded.
  The existing authoritative-runtime refusal of a local ACK without process
  evidence remains. Account peers cannot be promoted to a local `exited` state,
  even through a direct SQL update. A terminal channel event therefore does not
  grant source-capture eligibility by itself. Normal daemon dispatch composes
  account report binding, exact candidate import, checks and independent review.
- A successful unrestricted exit can publish a source snapshot for independent
  checks and review. Its execution remains `exited`, not `fenced`. The transactional
  capture-eligibility view admits auxiliary work only for this exact normally
  exited attempt, with no other unfenced attempts. This does not permit continuation
  or claim that arbitrary detached descendants have stopped.
- Dependent tasks copy only independently accepted source bytes, rechecking the
  snapshot hash and original execution/profile identity. They do not reuse the
  predecessor's live working directory.
- Service-created execution intent, current leases, durable acknowledgement and
  process ownership remain required. They coordinate execution; they do not
  constitute OS containment or prove that all descendants have stopped.
- Make stop/recovery semantics truthful for the selected boundary. A process-group
  exit is not proof that escaped descendants stopped. Preserve uncertainty and
  prevent conflicting reassignment when the previous writer may still be active.
- Feed captured source into isolated checks and independent review. A worker's
  prose, exit status or self-reported tests cannot complete its own task.

## Acceptance scenario

Use the normal operator launcher and CLI to register a project and create a task.
The actual agent must inspect context, execute a useful shell command, run the
project's check and make the requested change with its normal tools. Verify network
use through a controlled allowed endpoint under the selected boundary. Interrupt
mid-change, restart Conductor, then resume or reassign without losing work or
allowing concurrent owners. An independent agent reviews the resulting source;
the board reaches completed exactly once with the full causal record.

Keep the installed preview and existing user processes unchanged until an explicit
upgrade. Report this scenario's actual outcome and the next concrete blocker;
commit/test counts and unsupported percentage estimates are not completion proof.

### Persistent receiving-account worker

A receiving account can keep `conductor-runner --account-worker SOCKET COORDINATOR_UID`
open for sequential account-channel requests. The socket parent must already be
canonical, owned by that receiving account, non-writable by others, and traversable
by the selected coordinator account. The command does not create accounts, change
parent permissions, copy credentials, or switch identities.

Unlike `--account-listen`, this entry does not require a packet digest or receiver
tag at startup. Kernel peer credentials identify the selected coordinator; its
bounded handshake supplies the packet digest. The subsequent request must match
that digest and carry a valid prepared receiver tag. Native account/pin checks,
durable coordinator ACK and current-lease Release still precede child launch.
Malformed requests and failed acknowledgements close that connection; the endpoint
remains available for another task. There is no automatic replay or reconnect.
SIGINT/SIGTERM stop admission after the current bounded task finishes and remove
the owned socket entry. Forced termination can leave a stale socket; startup never
overwrites an existing entry.

Normal account workflows use this receiving entry for baseline transfer, native
execution and candidate return. A peer terminal event does not establish local
child exit or machine-wide write fencing.

The persistent worker also accepts exact baseline staging before native launch.
`stage_account_snapshot` connects using the independently selected manifest digest,
streams the archive, and accepts success only after the receiver has validated and
published the same manifest in a new private destination. The coordinator can then
put `DESTINATION/tree` in the prepared native packet. No shared writable directory
or copied login material is required. Existing destinations are refused, and failed
transfers leave no successful acknowledgement; unpublished partial directories
must not be reused. Buffered archive bytes are preserved across request dispatch,
and socket I/O uses an absolute deadline, including slow or silent peers.

Staging authorizes byte delivery to the explicitly selected destination under the
receiver's OS permissions. It does not acknowledge execution, grant a lease,
certify a result, or establish fencing. Normal daemon dispatch separately binds
the prepared execution and the returned candidate to independent verification.

Candidate return uses two explicit operations. `capture_account_candidate` asks the
worker to create a new snapshot from a selected source and baseline inventory,
binding the response to the selected packet digest. Its receipt is a peer claim
about captured bytes, not evidence of process exit, fencing or acceptance.
`fetch_account_snapshot` then imports that separately selected snapshot digest into
a new coordinator-owned directory. The worker revalidates manifest and file bytes
while exporting; changed content cannot publish a successful local manifest.

The coordinator must establish execution/lease eligibility separately, then run
required checks and independent review against the imported copy. These transport
operations do not themselves advance task state or wire normal daemon dispatch.

The persistent service records account outcomes separately from local process
observations. `await_account_execution` renews current ownership, retains bounded
checkpoints, parses the native output and records its digest. Malformed or unknown
terminal outcomes cannot become successful reports. They block the execution and
its matching active task with `account_terminal_failed`; operator pause/cancel and
newer attempts remain unchanged. Retained dirty bytes and unresolved ownership do
not establish a write fence or authorize another attempt. Execution uncertainty
is persisted before the task transition; restart reconciliation covers a crash
between those writes. `finalize_account_source`
consumes that report, captures and fetches the exact candidate, rechecks ownership,
and records the imported digest before binding the task's verification source.

Account executions remain `started` and unfenced with no fabricated process or
exit code. An imported candidate is eligible for independent verification only
while its recorded digest agrees with the task's selected source; blocked records
lose that eligibility. Reporting alone does not permit capture-based checks.
Schema 32 persists these separate report/import bindings; normal account workflow
dispatch uses this service composition.

### Normal workflow selection of an account worker

A native workflow plan may select an existing receiving-account endpoint:

```json
"account_worker": {
  "socket": "/shared/worker/control.sock",
  "artifacts": "/Users/worker/codex/conductor-artifacts"
}
```

This requires `full_capability` with the receiving account's exact identity.
Start `conductor-runner --account-worker SOCKET COORDINATOR_UID` as that account.
The endpoint parent and artifact directory must already be owned and accessible
as required by that account; Conductor does not create OS accounts, change HOME
permissions, or copy credentials. Socket paths must fit the native socket limit.
The worker uses its own HOME and existing vendor login. Its pinned executable
path is resolved and hashed by the receiver before ACK; the coordinator still
validates the independent verifier's local executable.

Before starting the receiver, run the read-only setup check as its selected OS
account (use an accepted plan or the package's `accepted-adapters.json`):

```sh
tools/conductor account-check --from-plan /absolute/accepted-plan.json --role worker
```

After starting the receiver, run the same command as the coordinator with
`--role coordinator`. In an installed package use `./conductor account-check
--role worker` or `--role coordinator`; the package supplies its accepted template.
Neither role needs initialized operator state. The worker check validates account
database metadata, real/effective UID and GID, its executable hash, and directory
ownership/access. The coordinator check validates its verifier binary and socket
access without opening the worker's private binary or artifact directory.

These are local observations, not an execution authorization or login test. No
vendor is launched, credentials are not read, and no connection is made. An
existing socket is reported as `present_unverified`, including a stale socket;
do not remove or replace it based on preflight output. Runtime peer authentication,
packet admission and current executable validation remain authoritative.

Normal `conductor plan`, package generation, submission and daemon dispatch
preserve this selection. Dispatch stages the exact baseline, records ACK before
Release, waits with lease renewal, imports the candidate, runs required checks,
and requests independent verification. Live workspace polling skips account
workspaces; imported snapshots supply final file events. Dependent tasks use
the accepted coordinator-side snapshot, never a mutable receiving-account tree.
Operator-pinned instruction paths must be readable by both selected accounts.

A successful peer report is not a local process-exit or write-fence receipt.
The account execution remains Started/unfenced after candidate acceptance.
The privileged CI fixture runs an actual controlled worker under a second OS UID
on macOS and Linux, checking kernel peer identity, HOME, file ownership, ACK before
release, and snapshot transfer. It uses a controlled vendor fixture, not personal
Codex or Claude logins. Started-worker interruption/restart recovery and personal
vendor deployment under distinct accounts remain separate delivery gates. See
[the delivery audit](DELIVERY_ACCEPTANCE.md) for pinned acceptance evidence.

After a controller restart, an independently accepted account snapshot can still
serve as a dependent task's input. Its completed task, accepted source identity,
account report and imported manifest must agree; the copy is rehashed before
publication. This read-only reuse does not reopen capture, renew the old lease,
or clear the old execution's Blocked/unfenced status. Unaccepted candidates still
require normal live capture/verification eligibility.
