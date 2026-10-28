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
validated controller/wrapper binaries for that claim. No database migration or
resume-packet encoding change is introduced.

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
