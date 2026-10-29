# Codex writer capability design

Status: broker, persistence barriers and MCP translation primitives implemented; production integration remains proposed.
Native Codex writer admission remains refused. This design does not certify a
file-only adapter, write fencing, model behavior or complete v1 delivery.

## Evidence

The installed pinned Codex 0.163.0 binary matches the previously accepted digest
`8813367285524ac798dd96d1b071aea30d58708c7226fd236cd275cf1cfdee31`.
Its feature inventory exposes `shell_tool`. The official
[configuration reference](https://learn.chatgpt.com/docs/config-file/config-reference)
describes `features.shell_tool` as enabling command execution tools. That setting
alone does not establish a complete restricted writer capability.

Three isolated local-provider fixtures use synthetic credentials, private empty
HOME/CODEX_HOME, no user configuration/rules, and a loopback HTTP server with no
model backend. The server captures only request tool definitions, then refuses
inference. No production authentication, project input or model service is used.
The requested fixture model is `gpt-5.3-codex`; these observations do not establish
the tool set for every model or for an independently changed CLI binary.

1. Default filesystem policy: shell enabled exposes `exec_command` and
   `write_stdin`; disabling it removes both. Neither request offers file editing.
2. Explicit workspace-write profile, otherwise matching the existing adapter's
   filesystem/network restrictions: the same difference occurs. Changing the
   permission envelope alone does not create file tools.
3. Explicit discovery-only MCP server: both profiles expose its namespace with
   exactly `conductor_read_text` and `conductor_write_text`. With shell disabled,
   `exec_command` and `write_stdin` remain absent. The fixture refuses every tool
   execution; these are visibility observations, not successful file operations.

Other visible built-ins include resource discovery, view_image and goal/input
operations. They require an explicit conformance audit; absence of shell is not
proof that no alternate effect path exists. Tool schemas were inspected as nested
namespace entries, not inferred from a namespace name alone.

Private scripts, captured schemas and receipts live in the workspace review
folder under `codex-file-tools-inventory-v1`, `-v2`, and `-v3`. No fixture directory
is reused. `codex-file-tools-discovery-manifest.json` binds their exact bytes.

## Capability boundary

Keep the Codex harness's own task filesystem authority read-only. Disable command
execution and unrelated integrations explicitly, ignore ambient customization,
and pin the accepted binary, tool schema and execution configuration. A model
that receives an unexpected writer/execution tool must fail conformance before
native admission. Prompts and declared tool descriptions are not enforcement.

Provide only bounded project file operations through a trusted Conductor broker.
The MCP child is a protocol proxy, not an independent file writer: it forwards
validated requests to the wrapper-owned broker over a private invocation-bound
channel. The broker owns the filesystem capability and the stop boundary. A proxy
that survives its parent must not retain write capability after the broker closes.
Do not inherit the restricted-Claude exit proof merely because shell is disabled.

This avoids making an unproved MCP process-group relationship the write fence.
It does not avoid proving that relationship and shutdown behavior where they
matter for other effects. The current generic escaped-session counterexample
continues to apply to command-capable workers.

## Responsibilities

- **Vendor adapter:** selects accepted CLI/tool configuration, preserves local
  authentication, constructs the bounded resume packet and rejects unknown pins.
- **Wrapper/broker:** holds the invocation/attempt/epoch binding and scoped root
  handles; validates bounded requests; performs authorized file operations;
  closes admission and completes or aborts in-flight operations before declaring
  its write capability revoked. Durable intent precedes external effects.
- **MCP proxy:** bounded JSON-RPC transport and fixed tool schemas only. It has no
  arbitrary command, dynamic tool installation, external network, or direct
  model-controlled filesystem operation. It cannot create new capabilities.
- **Store/service:** retains lifecycle truth, checkpoint/effect history and exact
  fence evidence; only accepted broker settlement may enable source capture or
  continuation. Channel loss alone is not evidence of completed writes.
- **Checks/verifier:** continue using independent immutable copies and existing
  source-bound acceptance. A successful broker write cannot complete the task.

## Implementation and acceptance gates

1. Define a versioned bounded broker protocol and deterministic file-operation
   outcomes. Bind every request to the active invocation/epoch; reject replay,
   stale ownership, unknown fields and unsupported operations. No credential,
   arbitrary command or original-checkout write surface enters this protocol.
2. Implement root-scoped reads/listing and replacement/edit/create/delete as
   separately authorized capabilities. Check path components, symlinks,
   hardlinks, special files, size/inventory limits, atomic publication and
   interruption before/after each external effect. Preserve interrupted bytes
   and ownership; do not equate an error return with rollback.
3. Implement bounded MCP proxy framing and explicit startup/configuration pins.
   Test malformed/oversized messages, channel replacement, EOF and stalled
   peers. Prove no filesystem write after broker revocation even when the proxy
   remains alive or requests race with cancellation.
4. Exercise the actual pinned Codex harness against synthetic inside/outside
   canaries: read and edit inside; deny outside reads/writes, control socket,
   credentials, network, shell/process creation and alternate built-in effects.
   Verify observable denied operations, not merely a model's refusal statement.
5. Verify normal exit, pause, timeout, worker crash, daemon crash, restart and
   retained dirty continuation with a live independent verifier and exactly one
   completion. Test fresh local authentication without introducing an API-key
   runtime dependency. Bind all resulting evidence to exact source and binaries.
6. Only then add a distinct accepted Codex writer capability/fence type. Keep
   command-capable and unknown configurations refused. Re-run the complete
   integrated monitoring and supported-host acceptance before release.

The first engineering step is the versioned broker capability and revocation
contract, with a minimal synthetic read/write/proxy race harness. Do not enable
Codex writing by changing the vendor enum check or copying Claude's fence label.

## Implemented protocol and revocation primitive

`crates/runtime/src/file_broker.rs` now defines strict version-1 scope/request
types and a serial in-process capability gate. Scope binds authority, boot
generation, invocation UUID, task, attempt and lease epoch. Requests use exact
monotonic sequence numbers, at most 100,000 operations, a 1 MiB encoded frame
limit, 256 KiB text writes and lexical relative paths. Write replacement requires
an expected digest; absent digest means create-only. The concrete filesystem port
below enforces that precondition within a private tree whose external writers
must be excluded; the gate alone does not.

The gate constructs a content-bound intent digest, invokes the trusted admission
port, rechecks revocation/deadline, then invokes the synchronous operation port.
Only successful completion advances the sequence. Concurrent duplicates cannot
obtain two admissions. An admission error, failed/partial operation, interrupted
admission or panic cannot produce a clean revocation acknowledgement. No payload
type implements Debug; fixed errors omit paths and file contents.

Revocation requests immediately close admission. Acknowledgement waits for an
in-flight synchronous operation to finish; queued requests are refused. A stalled
port can prevent acknowledgement indefinitely. The future wrapper must isolate
that wait from its responsive control loop and report uncertainty on timeout,
never assume that timeout stopped the write. Ports must not retain write access
or spawn deferred work. This requirement is not enforceable by a generic Rust
callback type and must be proved for the concrete broker implementation.

Tests cover strict parsing/bounds, admission-before-effect, exact-once concurrent
duplicates, stale scope/replay refusal, revocation during admission, revocation
during a real temporary-file write, retained partial-write failure, expiry and
panic poisoning. Port callbacks and intent markers are synthetic; these are not
SQLite durability, process-death, proxy confinement or native Codex acceptance.
No production launcher uses the gate yet and Codex writer admission stays closed.

Next: connect durable intent admission and the filesystem port below to the
private broker/proxy transport. Filesystem interruption, orphan proxy and actual
adapter negative controls remain required before granting a write capability.

## Concrete bounded filesystem port

`BrokerFilesystem` owns its gate and pinned directory descriptor. The only public
operation path admits the intent and serializes actual reads/writes through that
gate. UTF-8 reads and writes are bounded to 256 KiB; cumulative write contents are
limited to 64 MiB per instance. It opens intermediate directories and final files
relative to descriptors without following symlinks. It refuses hardlinks, special
files, cross-device descendants, case variants of `.git`, and the reserved
`.conductor-broker-` staging namespace. Root identity is rechecked before work and
publication. This is not a policy for an arbitrary mutable host directory.

Creates and replacements first write an exclusive invocation/sequence-named
staging file, sync its bytes and permissions, then publish it. Create-only uses
an atomic hardlink operation that cannot replace an existing name, followed by
removal of the staging name; replacement uses rename after rechecking the prior
content digest. Parent directory sync precedes a successful return. Replacement
preserves ordinary permission bits but does not propagate special privilege bits.
A failed admitted operation closes the gate as uncertain. No failure cleanup
removes staging evidence; pre-existing staging names are never overwritten.

**Required host invariant:** the broker must be the only writer to the private
attempt tree, including its directories. Descriptor traversal and repeated
checks do not prevent another writer from moving an already opened directory
outside the root or modifying a destination after its digest check. This port
does not supply an atomic content compare-and-swap against a hostile concurrent
writer. The launcher must establish OS-enforced read-only access for Codex/proxy
and exclude other writers before granting this capability. That integration and
its adversarial race acceptance remain unimplemented. Root replacement and
static symlink/hardlink refusals in tests do not prove this host invariant.

Real temporary-file regressions cover create/read/replace, executable permission
preservation, duplicate create and digest mismatch, outside canaries behind file
and directory links, hardlinks, FIFO nonblocking refusal, root replacement during
admission, invalid text/size, protected staging and Git names, retained staging
collision and admission failure. A private unit control seeds an exhausted byte
budget and proves refusal before a filesystem effect; it is not a measured 64 MiB
load run. No crash/power-loss durability, native Codex confinement or durable
SQLite admission is claimed by these tests.

This minimum port handles existing directories and text files. Directory creation,
listing, deletion and binary editing remain explicit capability work; they are not
silently supported. Store admission, completion receipts, restart reconciliation,
proxy transport and native end-to-end acceptance remain the next integration work.

## Durable operation state and result ordering

Migration 26 adds immutable broker bindings and sequenced operation records.
Registration binds the current recovery authority/generation, acknowledged
wrapper invocation, execution, task/attempt/lease and a trusted root identity
hash. New admission requires the matching active execution/lease/task projection;
the runtime's continuous deadline still governs elapsed time. The database stores
request/result digests and audit metadata, not file contents or request paths.
It does not derive or attest OS confinement from a caller-supplied root hash.

`register_file_broker` and `admit_file_operation` return true only for a new
registration or admission. Identical replay returns false and must never execute
the external effect again. Changed bindings/digests, sequence gaps, a preceding
pending operation, stale ownership and closed brokers are refused. An operation
is pending until its exact successful result is committed. Result recording may
finish after the lease/task stops accepting new work, while the owning authority
and generation remain current; this records prior effects, not new permission.
Closure requires the exact completed sequence and no pending operation. Each
transition and its audit append share a cancellation-owned SQLite transaction.

`execute_recorded` holds the runtime gate through both the filesystem operation
and trusted result persistence. A result-persistence failure or panic therefore
cannot produce a clean revocation even if the file was already published. The
legacy `execute` convenience method has no completion port and supplies no such
durability claim; production integration must use the recorded path.

Tests connect actual `BrokerFilesystem` writes to a real SQLite store through
synchronous callbacks on a dedicated blocking task. The successful control
persists intent before the file and result before clean closure. A deliberately
failed completion audit preserves written bytes with a pending operation, refuses
clean closure and grants no replay after reopening the database. Separate controls
cover exact replay, scope/lease/task refusal, all four transaction audit failures,
and deterministic future cancellation at each actual SQLite mutation. A runtime
race control holds result persistence while revocation waits for its completion.

These fixtures use synthetic acknowledged execution setup. Database reopen and
new runtime generation checks are not abrupt process-death or physical power-loss
acceptance. Pending work remains inspectable across generations and is never
inferred complete from current file contents. Automatic reconciliation, a durable
broker shutdown receipt, production transport and native adapter launch remain required before enabling
this route; ownership barriers are implemented below.
No execution fencing or task completion is changed by these records.

## Abrupt process-exit controls

The broker crash matrix launches nine separate owned child processes and exits
without Rust unwinding at these exact boundaries:

- Registration row mutation before commit, and after registration commit.
- Operation admission row mutation before commit, and after admission commit.
- Actual filesystem publication before result persistence.
- Result row mutation before commit, and after result commit.
- Closure row mutation before commit, and after closure commit.

SQLite update hooks select the pre-commit cuts; post-commit cuts follow successful
store returns. The filesystem cut runs the concrete file port and its sync/name
publication path. The parent requires the expected exit code, reopens the database
and checks integrity, exact whole-database rollback at every transaction cut,
unchanged unrelated tables, exact broker audit count and retained/absent file
bytes. Committed admission never grants replay; an unknown result blocks sequence
advance and clean closure. Completed operations remain non-replayable. A second
open preserves state and a new generation refuses old-owner mutation.

These are real process exits with synthetic acknowledged execution state and
no model calls. They are not SIGKILL during a filesystem syscall, machine power
loss, native proxy survival or accepted writer fencing. The ownership barriers below preserve the unclosed broker and its pending
evidence. Verified shutdown/recovery and native transport still remain required
before native launch can be enabled.


## Ownership barriers (schema 27)

`unresolved_file_broker_tasks` projects every open broker and any contradictory
closed broker with an incomplete operation onto its execution's task. SQLite
triggers refuse execution fencing, a new attempt/execution, task progression into
claim/verification/completion, auxiliary admission, and new/advanced dispatch
admission while that ownership remains unresolved. The existing-attempt upsert
path remains available for heartbeat, expiry/release and cancellation bookkeeping;
blocking it would prevent the controller from stopping work safely.

Queue candidate and automatic recovery queries exclude unresolved tasks before
applying their limits. Explicit and successor requeue refuse without publishing
assignment/audit changes. Existing running work still consumes the normal global
running limit; an open broker does not create extra dispatch capacity. Completed
dispatch reconciliation also refuses unresolved broker ownership even if a
historical execution already claims to be fenced.

The invariant report exposes bounded invocation/task/state diagnostics for old
or contradictory ownership, with total count and a 128-record detail limit.
Normal active brokers do not themselves make the runtime unhealthy. A closed
record with an unknown operation remains unhealthy. Diagnostics omit binding
payloads, file paths and contents and do not repair state. Cold backup refuses
both open brokers and incomplete operations before creating a destination.

Regression controls exercise the actual execution update API, manual/automatic/
successor continuation, direct SQL refusal and full-table rollback, closed
positive controls, queue fairness and running capacity, lifecycle diagnostics,
and backup refusal/closed success. A separate production persistence regression
reproduced an overly broad initial attempt-insert trigger blocking heartbeat;
the corrected identity guard allows renewal/release and still refuses a new
attempt until closure. The pre-fix failure and final result are retained.

These barriers prevent other lifecycle paths from treating unresolved file work
as settled. They do not prove that a caller's closure assertion stopped a native
writer. A distinct verified broker shutdown/recovery receipt, private transport
and OS isolation acceptance remain required before enabling Codex writes.

## Private stream server primitive

`serve_broker_stream` consumes one already-connected Unix stream and the broker,
so the peer receives no filesystem descriptor or operation callback. The trusted
host must supply the private endpoint; the primitive does not bind or authenticate
a named socket. Version-1 newline-framed requests include the existing immutable
scope and exact sequence. Frames are limited to 1 MiB including the delimiter;
replies are limited to 2 MiB, allowing bounded UTF-8 content to expand under JSON
escaping. Malformed, partial, oversized and repeated requests close admission.

Each operation runs on a blocking worker through `execute_recorded`, including
trusted journal admission and result persistence. Read, operation and reply waits
are bounded by a validated timeout of at most 30 seconds. Cancellation requests
revocation immediately and then waits only for the configured settlement interval.
If the operation remains blocked, the server returns `completed_sequence: None`;
the worker may still be running and ownership must remain unresolved. Neither
that timeout nor dropping the async server future proves that writes stopped.
An RAII owner requests revocation even when the server future is aborted. Normal
completion returns a local sequence observation only, not a durable receipt.

Nine transport regressions use actual Unix streams: real file write/read and
reply ordering; invalid/oversized/partial framing; idle timeout and cancellation
sender loss; stalled admission during cancellation; server-future abort; pipelined
duplicate refusal; blocked large replies after result recording; operation
timeout; and a separate-session peer surviving broker shutdown. The last case
launches an owned Python child with a connected endpoint, observes it alive
after server termination, then proves its new write request is refused and no
file appears. This child is unsandboxed and tests protocol revocation, not OS confinement or
the native MCP proxy. Stalled
callback tests await destruction of the worker's final journal owner and confirm
the callback returned normally before asserting no delayed file effect.

The journal here is a trusted synchronous interface. Its stream tests use markers;
real SQLite/file ordering is separately covered by the store integration tests.
The server has no production launcher call site yet. MCP translation is described
below; private endpoint creation/inheritance, authenticated host wiring, independently
persisted shutdown/result receipts and actual Codex confinement remain required.
Do not enable a native writer by treating `BrokerStreamEnd` as a fence receipt.

## MCP translation primitive

`serve_broker_mcp` now translates an isolated input/output stream into requests on
one host-supplied private Unix connection. It owns no task root or filesystem descriptor,
subprocess launcher, socket discovery or reconnect mechanism. Scope comes only
from the trusted caller; tool arguments cannot override it. This remains a library
primitive, not a configured Codex subprocess or production launcher integration.

The implementation follows the [MCP 2025-06-18 lifecycle](https://modelcontextprotocol.io/specification/2025-06-18/basic/lifecycle)
and [tools subset](https://modelcontextprotocol.io/specification/2025-06-18/server/tools).
It negotiates exactly that implemented version rather than echoing unsupported
versions. Initialization and the initialized notification precede tools. Its two
fixed tools are `conductor_read_text` and `conductor_write_text`; write requires an
explicit `expected_sha256` (null for create-only). Results carry serialized JSON
text with content/digest or written digest. This is compatible with the documented
[Codex stdio configuration surface](https://learn.chatgpt.com/docs/extend/mcp),
and the pinned-CLI controls below now exercise the executable translation path.

Requests are newline-framed, bounded to 1 MiB, and serialized. IDs are bounded and
tracked (at most 100,000 requests); reuse closes the session. Local argument errors
never reach the broker or consume a broker sequence. Replies must match version,
sequence, result shape, byte bounds and content digest before success is emitted.
An invalid/lost/timed-out broker response closes the channel before returning a
fixed MCP tool error: the operation outcome is unknown, and the proxy never retries.
MCP output is bounded to 4 MiB to cover nested escaping of a bounded text result.
Idle input, broker exchange and blocked output waits are individually bounded to
at most 30 seconds. These limits need explicit host lifecycle integration for a
long-lived model session; do not silently reconnect an expired capability.

Eight regressions cover actual file create/read/digest replacement; pre-init and
invalid/forged arguments; duplicate IDs; malformed/partial/oversized input and
notification-only tool calls; malformed/mismatched/digest-invalid broker replies;
broker timeout; proxy abort during an exchange; blocked output and idle input;
and a live proxy attempting a write after host revocation. The latter leaves the
proxy running until the new MCP call, then observes refusal, zero admissions and
no file. Tests use in-process duplex MCP streams and actual Unix broker streams;
this is not a separate-process proxy escape or native Codex isolation test.

Cancellation notifications close the session when consumed. Because calls are
serial, this is not an immediate interrupt of an in-flight request. Host-side
revocation is independent. EOF, proxy cancellation and transport closure do not
prove that an already-admitted operation stopped; the host must settle the broker
and persist independent evidence before fencing. Endpoint authentication/handoff,
actual store journal wiring, durable shutdown/restart reconciliation, CLI startup
pins and native conformance remain required. Native Codex writing stays refused.

## Single-use endpoint and executable proxy

`BrokerEndpoint` binds one Unix socket in an existing canonical, operator-owned
private directory. Its handoff pins parent/socket device and inode, a validated
scope and a random 256-bit capability. The host supplies the bounded handoff only
through `CONDUCTOR_BROKER_HANDOFF`; it must never enter argv, model context, logs,
persisted launch/audit records or user-visible configuration. Kernel peer UID and
a fixed authentication frame (version, capability, scope digest) are checked
before handing the connected stream to the broker. Wrong or stalled first peers
consume the endpoint too; there is no retry/reconnect or replacement capability.
The host must durably register a fresh broker before exporting this handoff.
Creating another endpoint object cannot authorize reopening an old scope.

The socket entry is removed on completion/drop only while its pinned identity
still matches. A replacement file is retained. These checks are not an atomic
filesystem operation against a hostile same-UID path mover. The host must use OS
policy to exclude untrusted same-UID access to the directory, its parents and the
handoff environment. Private permission bits and matching UID do not establish a
peer PID identity, mutual process attestation or that OS policy. This prerequisite
remains unproved for the native adapter; endpoint authentication alone cannot
justify native writer admission.

`conductor-file-proxy` is a real stdio executable with no command-line arguments.
It consumes the environment handoff, connects once, and runs the bounded MCP
translator. It receives no task root or filesystem descriptor. Failed startup and
protocol failures emit a fixed stderr message; stdout carries MCP frames only.
Its blocking stdin reader cannot delay process exit after channel ownership ends.
The library and executable remain outside the production native-writer launcher.

Seven automated endpoint/process tests cover single-use authentication and byte
preservation, wrong token/scope, non-private directories and replaced socket
canaries, stalled authentication and accept-future abort, real proxy create/read
and EOF, malformed environment privacy, and a proxy in a separate process group
remaining alive after broker revocation before its late write is refused. The
initial test fixture used default temporary-directory permissions and was
correctly rejected; the fixture now requests mode0700 at creation. No endpoint
permission requirement was weakened.

Three opt-in tests also run the exact pinned Codex0.163.0 binary with a private
HOME/CODEX_HOME and a local deterministic provider. The real Codex harness starts
the real proxy and discovers both tools; it then executes write/read through the
broker under an explicitly authorized two-tool profile. The host independently
checks two admissions, the actual file bytes, sequence2 and peer EOF. The negative
profile offers the same tools but leaves them unapproved: Codex refuses the write,
no operation reaches the broker, and no file appears. Global approval policy stays
`never`; the positive fixture uses only the documented per-tool
[`approval_mode = "approve"`](https://learn.chatgpt.com/docs/extend/mcp) for those
two tools, with server defaults prompting and an exact tool allow list.

The local provider emits controlled [function-call stream events](https://developers.openai.com/api/docs/guides/function-calling),
not model inference. It verifies that the capability is absent from received model
requests and captured stdout/stderr/tool-inventory logs. The CLI reports fallback
metadata for the fixture model `gpt-5.3-codex`; this is not all-model acceptance.
Discovery alone makes no file calls; positive and denial results are separate.
The first call attempt without per-tool permission failed its positive assertion;
that evidence is retained and the denial is now an explicit negative control.
No real model service, production authentication or original project is used.

These tests do not prove OS confinement, full privacy coverage, live-model behavior,
SQLite journal wiring, durable shutdown evidence, restart reconciliation or
long-lived idle handling. Native Codex writing remains refused. The handed-off
Claude-writer/Codex-reviewer release bundle is unchanged by this development.

## Store-backed stream journal

`conductor_service::RegisteredBrokerJournal` now joins the private stream's
synchronous admission/result ports to the authoritative store. Registration must
commit fresh before the host exports its handoff. Identical registration returns
a refusal, and the registration is consumed by one stream server. Every callback
checks the exact registered scope; only a fresh committed admission permits a
filesystem effect, and only a fresh committed result permits a success reply.
Audit events use generated identifiers and retain invocation/sequence metadata,
not file contents or paths. The existing store transaction owns audit and state
changes together.

The synchronous bridge is private and runs only on the transport's blocking
worker while the owning Tokio runtime remains alive. Store errors become fixed
protocol failures. Dropped registration, aborted serving, pending completion and
ordinary EOF do not close durable ownership or grant a second operation path.
A local completed sequence is still not an independently persisted shutdown
receipt. The caller must pin the actual root to the recorded identity and enforce
exclusive OS access; this API does not establish those properties.

Regressions join real Unix streams, actual temporary files and SQLite, reopen
the database before accepting the observed reply, and cover duplicate admission,
scope mismatch, stale generation, admission audit failure, result audit failure
and dropped registration. A synthetic acknowledged execution establishes the
fixture's initial authority; these tests do not prove a native launch or reboot.
The production native launcher still does not use this bridge. Wiring wrapper
requests to the daemon owner, independent shutdown evidence, long-lived idle
handling and OS confinement remain required before enabling Codex writing.

## Separate wrapper journal channel

`RemoteBrokerJournal` supplies the synchronous filesystem worker's journal port
over an already-authenticated private Unix stream. `RegisteredBrokerJournal`
can consume its fresh store registration to serve that channel in the daemon.
This avoids giving the wrapper SQLite write access or multiplexing database waits
onto its renewal/cancel input. The native runner has not yet been wired to these
ports; the existing release/watchdog protocol and native admission are unchanged.

Version-1 frames are bounded to 4096 bytes and contain only exact invocation
scope, operation sequence, request digest and optional completion digest. The
response hashes the entire canonical request, including phase and result. Both
ends enforce admission/completion ordering and never accept a duplicate as a
fresh acknowledgement. A failed response, timeout or invalid phase consumes the
client connection; there is no reconnect or automatic retry. Synchronous socket
reads and writes use the remaining total exchange deadline, so partial replies
cannot keep extending the wait. The daemon bounds each frame and persistence/
reply operation and cancels independently of runner control.

Three real child-process fixtures own the actual file broker and use the parent
service's SQLite journal: success writes and records exactly once; failed intent
audit creates no file; failed result audit retains the file and pending intent.
Their initial execution acknowledgement and socket handoff are controlled fixture
setup, not native Codex/OS containment acceptance. Additional controls cover
malformed/oversized/unknown-scope/phase requests, replay, cancellation after
admission, incorrect/oversized/missing acknowledgements and slow partial replies.

The host must still authenticate and confine this trusted wrapper channel; never
expose it to the MCP proxy or vendor. The root binding, native launcher handoff,
independent durable shutdown receipt and long-lived idle lifecycle remain open.
EOF or a successful result acknowledgement does not close durable ownership or
prove that filesystem write capability was revoked.

## Actual broker runner mode and lease gate

`conductor-runner --file-broker` now owns the concrete filesystem broker in its
own process. A bounded private stdin launch binds the scope, canonical task root
identity, private proxy directory, authenticated journal handoff and hard runtime
deadline. It opens/pins the root, checks its digest, connects to the separate
journal channel and emits Ready over the private controller pipe. Ready contains
a bearer proxy handoff and must never enter logs, model context or persisted
launch records. The runner serves no operations until an exact launch-hash
Release arrives with a live lease. This mode does not yet spawn a vendor.

Root identity hashes a versioned domain, descriptor device/inode and canonical
pathname. It identifies the opened root, not its contents, and is not a substitute
for host exclusion of other writers/directory movers. Only the filesystem worker
holds file handles. The watchdog receives shared deadline/revocation control
without file access. It handles renewal/cancel separately from journal I/O.

The gate checks the minimum of lease and heartbeat expiry before admission and
again after durable admission, before file effects. Renewal cannot extend the
original hard deadline or revive an expired/revoked gate. Its short deadline lock
is never held over filesystem or journal operations. Control loss, bad renewal,
heartbeat or lease expiry closes admission before waiting for settlement. Terminal
output explicitly flushes before process exit; the initial unflushed implementation
showed an EOF in a process test and is retained in local failure evidence.

Tests use the actual built runner, authenticated endpoints, parent SQLite and
real files. They cover root mismatch before Ready, write/record/cancel, live renewal
past the first deadline, wrong/expired/missing release, control EOF, heartbeat and
lease expiry, wrong renewal and rejected writes after exit. Initial execution
acknowledgement remains synthetic fixture setup. Separate gate controls cover
expiry while admission is delayed, late renewal, hard limits and root replacement.

Stopped carries a local completion watermark. The runner now also exclusively
claims its invocation in a private receipt directory and publishes an immutable,
fsynced shutdown receipt only when it never released operations or settled a
revoked worker with a known watermark. The receipt binds scope, launch/root
hashes and native process identity; it contains no bearer handoff or file data.
Unknown settlement and process death do not produce a receipt. Readers reject
partial, mutated, linked, nonprivate or rebound records. These filesystem checks
still require host exclusion of hostile same-user writers and directory movers.

Actual-runner tests independently observe the child identity and verify reopened
receipts after successful writes, renewal and release/control failures. An owned
SIGKILL before release leaves the exclusive claim and no shutdown receipt. The
SQLite broker remains open throughout these tests; receipt presence alone does
not authorize database closure or task acceptance.

The parent client observes Ready, requires a caller persistence callback before
Release, revalidates process/root identities, and checks the independent receipt
on successful shutdown. Actual-runner client tests cover persisted identity before release, callback
failure without release, and failed completion audit retaining the file and
pending intent without a receipt. The callback now uses the production store ownership transaction. Vendor
admission and pre-owner recovery remain incomplete. Native admission still requires durable parent
ownership and restart reconciliation, vendor configuration/OS confinement and
long-lived idle handling. The existing Claude workflow and Codex writer refusal
remain unchanged.


## Durable parent ownership

Schema 28 records the observed process identity, launch digest and receipt-root
identity before Release. Only a fresh authorization grants release; exact replay
returns no permission, substituted identities fail, and registration with prior
operations cannot acquire an owner retroactively. Owner creation and its audit
record share one owned SQL transaction.

The service verifies a receipt against this stored owner and the pinned receipt
directory before passing its digest and watermark to the store. Owned brokers
cannot use the older caller-asserted closure path. The store checks exact owner,
receipt replay, completion watermark and absence of pending operations, then
commits the receipt digest, broker closure and audit together. Audit failure rolls
back all of them. This closes only the broker; execution fencing and task
acceptance remain separate.

Actual parent/runner/files/SQLite tests exercise this path and reopen the closed
record. Changed process, launch/root hash and observation are refused. Store tests
cover replay, changed ownership/receipt, bypass closure, pending operations and
full-database rollback on authorization/closure audit failures. Initial execution
acknowledgement is still fixture setup. This API is not yet wired into native
Codex vendor admission; older-generation closure requires the explicit receipt recovery protocol below. Existing delivered user databases
are not upgraded by these tests.


## Receipt recovery without a terminal message

The service reconstructs expected receipt fields from the durable binding, parent
owner and completed operation count. It never adopts identity from a receipt or
infers closure from PID absence. Pending effects, missing/altered files, moved
receipt directories and foreign native hosts remain unresolved. With zero
operations it accepts either exact NeverReleased or exact Revoked(0) evidence;
otherwise it requires the exact completed watermark. Receipt reads remain private,
bounded and descriptor-relative.

Same-generation recovery uses ordinary owned closure. Older-generation recovery
requires the exact currently reserved generation, the original authority, exact
persisted binding and owner, unchanged receipt digest on replay, and no pending
operations. All conditions are rechecked inside the closure transaction. A
file_broker.recovered audit records both origin and recovery generations with the
watermark; replay produces no duplicate event. Audit failure rolls back receipt
and closure. No restored PID is signaled, no file operation is replayed, and this
never fences an execution or accepts a task.

Actual runner tests close its stdout/control pipes before release, after release,
and after a real write. They require failed terminal delivery, reopen SQLite,
reserve a newer generation, refuse stale/future/absent proof, and recover solely
from independent receipts. The client peer is retained until stdout is closed so
normal peer EOF cannot race terminal delivery. These are controlled runner and
store generation tests, not a killed production daemon/vendor acceptance run.
Store controls cover pending effects, changed authority/binding/process/root,
audit rollback and single generation-bound recovery audit. Pre-owner failure recovery and native Codex launch/OS/idle composition remain
incomplete.


## Automatic bounded receipt sweep

New ownership records retain the canonical private receipt-directory locator in
addition to its identity hash. The locator is bounded and immutable with the
owner; it is not a capability or file-content record. Older records without a
locator retain their exact encoding and are never backfilled or guessed. Explicit
recovery with a separately supplied matching directory remains available for
those older records; automatic recovery skips them.

The existing opt-in interrupted-workflow recovery loop now sweeps brokers before
writer fencing and continuation. Each sweep reads at most 128 owned open brokers
in invocation order, independently decodes bindings, and shares its cursor across
service clones. A short page resets the cursor for a later sweep. Malformed rows,
missing private receipts and unknown effects remain open without blocking later
rows. One native host observation is shared across the page; each settlement
rechecks persisted owner, directory identity and transactional generation/state.
No sweep relaunches a broker or signals a stored PID.

The actual-runner recovery test now enters the production service startup and
interrupted-workflow recovery path. Its write scenario places 129 malformed,
foreign-key-valid broker records ahead of the genuine record: the first 128-row
page leaves it open, a service clone processes the second page and closes only the
genuine broker. Test execution packets are valid synthetic adapter packets;
initial acknowledgements and unrelated malformed rows remain controlled fixtures.
This is service lifecycle coverage, not a complete production daemon/vendor
acceptance scenario. Ownerless/pre-owner recovery and Codex launch integration
remain required before native writer admission.

Writer receipt reconciliation also defers executions with any open broker or
contradictory pending broker operation, including during startup before the
opt-in sweep runs. This prevents an unresolved broker's SQL fence barrier from
turning ordinary startup into a failed reconciliation. The barrier remains
transactionally authoritative; deferral never establishes a writer fence.

The next native-launch composition must preserve the distinction between endpoint
authentication and durable journal registration. The broker authenticates its
journal connection before emitting Ready, while current journal registration
requires an acknowledged execution. A launcher can authenticate and retain the
private stream without serving it, observe Ready, durably acknowledge the exact
process, then register/serve the journal and persist broker ownership before
Release. This avoids weakening operation admission or fabricating an initial ACK.
The current test fixture registers against a synthetic started execution and is
not evidence that this native ordering has been implemented. Recovery before
parent-observed ownership remains a prerequisite.


## Prelaunch persistence foundation (schema 29, issue #92)

`prepare_file_broker_launch` commits bounded non-secret launch, approved wrapper,
receipt directory and native host metadata against a current prepared execution.
Only a fresh committed intent grants creation; replay never grants another spawn.
Audit failure rolls back the intent. The caller still owns wrapper approval and
native identity observation; metadata validation alone proves neither.

Registration requires the exact open preparation. Release additionally requires
its launch/root/host metadata and the execution's acknowledged process to match.
Owned receipt closure updates preparation, broker and audit atomically, including
recovery through the existing shared closure path. An asserted closure or changed
receipt cannot discharge preparation ownership.

Open preparation participates in task/fencing/backup barriers even before broker
registration. Startup reconciliation defers unresolved preparations; bounded
stale-owner diagnostics expose identity and stage without launch metadata.
Store tests use controlled process identities and generation changes. Actual
native launcher ordering and ownerless-preparation receipt recovery remain open;
these primitives do not establish native Codex writer acceptance.
