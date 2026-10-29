# Native wrapper process observations

Production service launches in an authority-bound database attach one canonical
argument to the wrapper:

`--conductor-owner=v1:<authority-id>:<packet-sha256>:<wrapper-uuid>`

The first two fields are 64 lowercase hexadecimal characters. The final field
is a lowercase UUID-shaped identifier. The authority namespace comes from the
external recovery binding; the packet digest identifies the prepared execution;
the wrapper UUID is the same identifier persisted during acknowledgement. No
credentials, packet text, effect token or vendor arguments enter this OS tag.

The controller checks the packet digest before spawn. The new wrapper validates
its single argument and requires the digest to match the received packet before
sending Ready. Unknown, duplicate, malformed and mismatched arguments refuse.
Durable preparation, recorded acknowledgement, current lease checks, release,
renewal and independent watchdog behavior remain in place. The tag does not grant
permission to execute. The namespace/UUID are correlation fields, not secrets or
an authenticated statement about a process.

Legacy direct wrappers and unbound test Stores can still run without a tag; they
cannot provide scoped correlation. An older wrapper binary may ignore arguments,
so its presence does not establish the new argument validation. Use matching
validated controller/wrapper binaries for that claim. The original argv feature
did not change the database or resume-packet encoding; durable birth metadata
now requires migration 18 as described below.

## Read-only diagnostics

```sh
python3 tools/inspect_runner_processes.py /absolute/path/to/state.sqlite3
```

The tool reads `ps` executable names, then arguments for at most 128 processes
named `conductor-runner`. Each subprocess read is bounded to 4 MiB/five seconds;
the complete observation has a ten-second budget, including SQLite progress.
The database opens read-only and must have a valid authority binding. Only tags
for that namespace are correlated; other scopes' identifiers are not reported.
Output contains allowlisted identities and classifications, never raw argv or
process environments. Untagged/unreadable process counts are host observations
and cannot be assigned to this database.

Classifications distinguish a matching record, a prepared record awaiting ACK,
missing/ambiguous records, wrapper mismatch and an observed process with a fenced
record. Metadata and state are observations, not a process identity proof: argv
can be forged, process names can change, a process can exit between reads, and
PIDs can be reused. The OS observation precedes the SQLite snapshot, so a wrapper
that exited normally can appear beside its newly fenced record. Reobserve and
inspect ownership before diagnosing a contradiction. The tool never signals a
listed PID, alters a task or repairs a record. It explicitly reports neither
liveness nor fencing as proven.

Version 2 additionally samples a kernel start marker before and after reading each
candidate's arguments, and once more after the database correlation. Linux uses
field 22 of `/proc/<pid>/stat`, parsing the parenthesized name separately; macOS
uses `proc_pidinfo(PROC_PIDTBSDINFO)` start seconds/microseconds with exact ABI
size and returned-PID checks. Missing or inconsistent markers discard the
observation. Changed observations have a separate host-level count; unavailable
markers count as unavailable observations. Only scoped matches expose the marker.

`process_start_observation` is an opaque, boot-local diagnostic string and
`start_observation_unchanged` means equality at those three reads. This narrows
observed PID-reuse races but does not pin a process: clock resolution, a later
exit/reuse, executable changes within the same process, forged tags and a host
restart still prevent treating it as a durable ownership handle. Legacy records are not bound to a marker. Newly acknowledged executions carry
the separate durable binding described below; no recovery gate changes. Old
consumers must recognize the new report version. Tests include a real native
child before/after reap, names containing parentheses on Linux, malformed start
fields, and controlled changes around argument and database reads.

A complete native orphan checker still needs trusted process-birth identity,
coverage of escaped/reparented workers, snapshot/race handling and conservative
reconciliation. These observations do not replace those gates or full native
soak acceptance. Empty output does not prove every write surface is fenced.

Tests cover canonical parsing, actual OS visibility, wrong-packet refusal before
Ready or worker writes, scope separation, prepared/matching/mismatched/fenced/
missing/ambiguous records and read-only correlation. Separate native acceptance
binds the observed scope, packet and wrapper UUID to the actual execution, then
checks normal writer/check/reviewer completion and post-completion observations.

The final release candidate completed a real subscribed Claude writer → declared
checks → independent Codex review in 39.40 seconds. During execution its observed
namespace, packet digest and wrapper UUID matched the persisted execution; no
scoped wrapper was observed after completion. Wrong routing, narrowed-subscription
refusal, duplicate publication, restart and original-project preservation also
passed. A separately owned wrapper waiting for a packet was reported as missing
its record and omitted from another namespace; closing its private input exited
without Ready or worker release. These are bounded observations, not generic
escaped-process or process-birth identity proof. Private evidence is retained in
`reviews/agent-conductor-runtime-20261028/process-identity-*`.

## Durable parent-observed binding

Migration 18 adds nullable `executions.process_json`. New service launches observe
native host/boot digests, PID and kernel start marker from the daemon's own child
after Ready. No worker-provided PID or process-start claim is accepted. The parent
retains its unreaped child, refuses an observed exit, commits metadata with the
existing acknowledgement/audit/replay transaction, rechecks the start marker and
child status, then checks the absolute lease deadline immediately before release.
Observation has a three-second deadline, post-ACK revalidation one second, and
ACK persistence retains its existing three-second deadline. Failure closes the
private control pipe and leaves durable intent/observations for reconciliation.

The macOS runtime uses pinned `libproc` 0.14.11 with generated SDK bindings; Linux
reads bounded `/proc/<pid>/stat`. The shared native host-epoch reader retains its identity semantics; its macOS
system helper commands now run with an empty environment. macOS source builds require the SDK/Clang needed by bindgen; installed
binaries use system libproc and do not invoke Python for worker launch.

An authority-bound database refuses a new ACK without metadata. Shape validation
rejects invalid PID/platform/digests/start fields; a changed replay binding is an
idempotency collision. ACK/audit failure rolls back the binding too. Existing
records remain NULL/unknown, and legacy unbound Store test callers retain their
old serialized acknowledgement form. No legacy identity is invented or backfilled.
Old binaries refuse schema 18; retain matching pre-upgrade tools and a verified
cold backup. This is a forward migration, not a downgrade/restore workaround.

The diagnostic recognizes the new column while retaining legacy-schema reads:

```sh
python3 tools/inspect_runner_processes.py /absolute/path/to/state.sqlite3 \
  --host-reader /absolute/path/to/trusted/conductor-daemon
```

Only an explicitly selected trusted daemon is executed to read native host/boot
identity (a separate bounded read before the ten-second process inventory).
`recorded_process_binding` distinguishes unknown legacy evidence, invalid data,
PID/marker mismatch, unchecked/mismatched host boot and matching observations.
Without the host reader, a matching PID/marker remains `host_boot_unchecked`.
Neither a match nor absence authorizes signaling or proves descendant fencing.

This binds observed metadata to ACK, not every crash boundary: a controller can
die between spawn and ACK, leaving only prepared intent and a tagged wrapper. That
case remains conservative. Start timestamps have finite resolution and persisted
metadata does not retain the parent's live child handle after controller loss.
Escaped/reparented/renamed descendants and writable surfaces still require their
own containment and reconciliation proof. Full FR-6/NFR-1 acceptance remains open.

Native acceptance on the migration-18 candidate completed a subscribed Claude
writer, declared checks and independent Codex review in 29.79 seconds. During
execution the persisted PID/start marker matched the kernel observation and the
host/boot digests matched a fresh native reader. Routing refusals, duplicate and
restart behavior, original-project preservation and post-completion observations
passed. A separate fresh-authority fixture imported completed synthetic rows from
schema 17: migration preserved task aggregates and unknown legacy process fields,
the old binary refused schema 18, and the new binary reopened. This is migration
compatibility evidence, not cross-path recovery or proof of orphan fencing.

## Expected identity before spawn (migration 19)

New service launches generate the wrapper UUID before preparing the execution and
persist it as `expected_wrapper_identity` in the preparation transaction. The same
UUID is passed to the wrapper and later required by ACK. A different UUID refuses
without changing the execution, audit or replay receipt. A preparation replay
never grants another spawn; a changed expected UUID is an idempotency collision.
The expected identity survives a failed spawn and database reopen.

This is intent, not an observed process. `wrapper_identity` and `process_json`
remain absent until ACK. Legacy preparations keep NULL and retain their original
serialized form; no identifier is inferred or backfilled. Malformed non-NULL
identifiers refuse at preparation and record decoding. An audit failure rolls
back the preparation and identity together. The new migration requires matching
backup tools and binaries; do not use these tools against the running schema-18
service until the normal verified upgrade procedure is complete.

## Trusted proof that no worker started

A matching tagged wrapper that abandons its Ready/release handshake writes a
private `unstarted-<packet-sha256>.json` beside the attempt tree, then returns.
The publication branch is before the worker task and cannot fall through to
spawn. It covers Ready output failure, lost/invalid release, changed instructions
at release and an already expired grant. Because a grant may arrive before a
later pre-spawn check fails, the observation is precisely `worker_not_started`,
not a claim that a release frame was never received. Earlier malformed launch or
identity failures, and wrappers killed before publication, remain unknown.

The versioned strict receipt binds the authority namespace, packet digest,
expected wrapper UUID and attempt. Publication uses a private create-new pending
file, file fsync, no-replace hard link, pending removal and parent fsync. Readers
reject missing/partial evidence, symlinks, multiple links, wrong ownership or
permissions, oversized files, extra fields and mismatched identities. A crash
leaving both pending and final hard links is conservatively unreadable. The tag
is not a secret or signature; the trust boundary is the trusted wrapper and the
receipt directory outside the restricted writer's allowed root. This does not
protect against a malicious same-user process with access to private state.

Recovery accepts this proof only against the durable expected UUID and current
authority, for the existing restricted Claude writer capability. Any recorded ACK
must agree with the expected UUID. Legacy unknown expected identities cannot use
this path. Accepted proof fences the execution through the existing revision,
audit and replay transaction without inventing an ACK or process birth binding.
The existing explicit continuation and finite automatic recovery policy then
apply; cancelled/paused/operator-blocked work remains protected. Existing
restricted stop/exit receipts still require an acknowledged wrapper.

This does not fence escaped descendants of a worker that started, or prove
recovery when the wrapper died without publishing evidence. PID absence,
prepared state and expected identity alone never authorize a fence or retry.
Local acceptance does not install the candidate in an existing operator runtime.

Native candidate acceptance holds actual Ready before ACK, and separately holds
Release after durable ACK, through a private instrumented relay. After controller
loss the live orphan stays blocked. Closing its control input lets the actual
wrapper publish no-spawn proof; exactly one automatic continuation then completes
with a real Claude writer, declared checks and independent Codex review. Recovery
including final restart takes 37.63 and 29.20 seconds respectively. Prior work and
ACK/birth metadata (or their absence) are preserved, a new attempt and identity
are recorded, and completed restart does not duplicate work. These timings are
fixture observations, not performance guarantees. Both model workflows incurred
unmeasured provider usage. A fresh-authority migration fixture imports completed
schema-18 application rows, preserves their task/ACK/birth records, retains NULL
expected identities, rejects the old binary and reopens with the new binary.
This is migration evidence, not cross-path product restoration.

## Expected identity in read-only inventory

On schema 19 the diagnostic reports `expected_wrapper_binding` separately from
ACK and native birth metadata. It distinguishes `legacy_unavailable`,
`matching_observation`, `identity_mismatch` and `invalid_recorded_identity`.
A wrong UUID for the right packet is `expected_wrapper_identity_mismatch`;
malformed stored identity is `invalid_expected_wrapper_identity`. Neither can
be presented as a matching prepared wrapper. Stored expected identity and process
JSON are bounded in the SQL projection before Python materializes them; malformed
or oversized stored text is not echoed.

A matching expected UUID with no ACK remains `prepared_not_acknowledged` while
prepared, or `unacknowledged_expected_wrapper` after blocking. A fenced no-spawn
record with the same observed wrapper is `fenced_record_process_observed`, even
though its ACK remains absent. The wrapper may briefly remain visible after
publishing its proof; this classification does not establish a surviving worker.
A contradictory recorded ACK still refuses a match. Legacy schemas and NULL
expected identities remain explicitly unknown. These are optional version-2
observations, not permission to signal, fence, release or retry any process.
