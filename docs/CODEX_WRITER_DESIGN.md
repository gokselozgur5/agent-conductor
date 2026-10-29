# Codex writer capability design

Status: protocol and isolated file port implemented; production integration remains proposed.
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
broker shutdown receipt, production transport, inventory/backup/continuation
barriers and native adapter launch remain required before enabling this route.
No execution fencing or task completion is changed by these records.
