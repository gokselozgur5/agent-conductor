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

## Required integration

- Native plans now accept `full_capability` with a frozen OS account and PATH.
  The worker process must already run as that account. The operator plan command
  preserves the profile from its accepted template. Authenticated loopback RPC
  permits control-plane clients in another account without sharing the private
  daemon directory. Native cross-account launch/artifact transport and started
  unrestricted worker recovery still require implementation and acceptance.
- `native_writer_launch` routes explicit full profiles to Codex `--yolo` or Claude
  `--dangerously-skip-permissions`. Both use the selected account's own HOME and
  configuration. Restricted profiles retain their previous behavior.
- Full native dispatch sends only the frozen packet and selected vendor pin to
  the runner's `--account-native` entry. The receiver verifies its OS identity and
  executable bytes, then builds CLI arguments and HOME locally before Ready.
  It still waits for a durably recorded acknowledgement and a current lease
  before launching. This is receiving-account command preparation; the current
  dispatcher still launches locally and does not yet select a cross-account channel.
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
  grant source-capture eligibility. Daemon workflow selection, account terminal
  evidence and workspace transfer/capture still need integration; this service
  port is not yet a complete operator workflow endpoint.
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

This is the receiving entry for daemon integration. Normal workflow selection,
baseline transfer, account terminal/source import and independent acceptance are
not yet wired to this entry. A peer terminal event still does not establish local
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
certify a result, or establish fencing. Normal daemon plan selection and the return
path for candidate source verification remain separate integration work.

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
