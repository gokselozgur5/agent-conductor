# Task execution and verification evidence

Select **View evidence** on a task to inspect persisted execution, source-binding,
check, review and completion records. The same read is available through:

```sh
python3 tools/conductor ctl --json task-evidence TASK_ID
python3 tools/conductor ctl --json task-evidence TASK_ID --before SEQUENCE
```

The authenticated HTTP route is `GET /api/evidence?task_id=TASK_ID&before=SEQUENCE`.
Responses contain up to 100 entries, newest audit sequence first, and an exclusive
`next_before` cursor. Refresh returns to the newest page. Unknown query fields,
invalid/overflow cursors and nonexistent tasks are refused. This uses the existing
audit task/sequence index and requires no schema migration.

## What the view establishes

Each entry includes its audit sequence, kind, recorded actor and time. Selected
kinds are execution prepared/started/exited/fenced/blocked, begin verification,
source binding, check result, verifier verdict and completion. General lifecycle
history, checkpoints and reported usage remain in their separate views.

Execution entries link to the task's immutable attempt, lease epoch, packet digest
and runner identity. They also show the **current recorded execution state**,
revision, exit code and update time from the same database read. These fields can
be newer than the audit event being displayed. Neither an exit nor a blocked
state establishes fencing; none of these fields observes a live process or grants
ownership. An absent execution link is explicitly unavailable.

Check entries retain the recorded result even if a structured receipt is absent.
A runtime receipt is shown only for the recorded system check executor, with a
matching task, actor, check ID, source digest and passed/outcome relationship.
Passed receipts also require exit code zero. Receipt fields are the outcome,
exit code and source/definition/output digests. Missing stream digests remain
unknown, not empty-output hashes. This checks internal recorded consistency; it
does not rerun the check or recompute any current file hash.

Review entries show the recorded verdict, source and well-formed recorded output
digest when available. The digest is a historical reference, not proof that the
output file still exists. Completion is a recorded lifecycle event, not a new
acceptance decision made by the evidence viewer. Task versions, audit sequence,
lease epochs and execution revisions are strings where JavaScript integer
precision would otherwise lose information.

## Privacy and failure behavior

Raw packets, executable arguments, command reasons, vendor output, review
explanations, effect tokens, idempotency keys and wrapper identities are omitted.
Unknown receipt fields are removed through typed deserialization and re-encoding.
Audit payloads above 64 KiB are not loaded into the projection; malformed,
misbound or unsupported records display unavailable evidence without turning
an old claim into verification. Identifiers, actor names and source references
remain operator-controlled metadata; this is not general secret redaction.

Reads never change tasks, leases, execution records or acceptance. They never open
artifact paths or execute commands. Pagination is not one transaction across all
pages; new entries require a refresh. The usual transport byte cap still applies.
The browser uses text nodes, clears evidence on disconnect and rejects late
responses after switching tasks, refreshing or closing the panel. A read failure
is displayed as a failure, not as an empty successful result.

## Validation

Controls cover 205 selected records across three pages with an intervening append,
task isolation, authentication, malformed and oversized payloads, unknown HTTP
fields, cursor limits and unchanged task state. Source/check/outcome and actor
mismatches cannot expose a runtime receipt. Unknown/private receipt fields and
review explanations are omitted. Execution reads preserve the difference between
historical events and the latest recorded state, while omitting packet,
checkpoint, wrapper and effect-token contents.

The browser and CLI/HTTP acceptance uses an isolated copy of a previously completed
native workflow plus synthetic pagination records. This validates the viewer;
it is not a new model execution or disaster-recovery rehearsal.
