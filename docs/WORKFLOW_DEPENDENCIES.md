# Dependency-aware verified output handoff

## Requirement and design

An approved successor must wait for declared predecessors to complete independently,
and may start from one explicitly selected predecessor's accepted source. An event
or worker claim cannot authorize this handoff. Original projects, prior artifacts,
checks and instruction authority remain unchanged.

Plans may declare up to 16 unique `depends_on` task IDs and an optional `input_task`
chosen from that set. Submit predecessor plans first. Immutable plans may reference
only already admitted plans with the same project path: this bounds admission and
prevents cycles, self edges and forward references without speculative scheduling.
Multiple predecessors gate execution; one input supplies the whole initial tree.
There is no automatic merge of conflicting predecessor outputs.

The store atomically binds completed predecessor source identities with dispatch
claim and any subscribed-event acknowledgement. Waiting dependencies must not
consume the event or block unrelated eligible queue entries. Continuation keeps
those bindings and its own retained unfinished work. Direct one-shot execution
without durable dependency admission is refused.

Before copying an input, the service verifies completed independent acceptance,
project identity, fenced native attempt and retained packet identity. It reads a
bounded no-link manifest from that attempt's accepted source artifact and matches
the accepted hash. A fresh bounded snapshot of source/tree, including deletion
inventory, must produce that same hash. Changed/missing/linked source is refused
before worker launch. A separate helper handles accepted output; unfinished-task
continuation guards are not weakened.

The successor gets its own private writable copy and records predecessor hashes in
its bounded untrusted context. Its own original instruction pins, checks, authority
and independent review still apply. Copied repository text gains no authority.

## Operator behavior and limits

Use `tools/conductor plan --depends-on PREDECESSOR --input-task PREDECESSOR`, then
submit the generated plan. Dependencies without an input selection are readiness
gates only. All plans remain explicit operator authorizations. The board's
"waiting for dependency admission" label means no dispatch/source binding has yet
been committed; it does not claim the predecessor is still running. A paused,
blocked, rejected or cancelled predecessor cannot satisfy the completion gate.
An independently accepted predecessor may satisfy it even before its dispatcher
receipt is written. A failed input copy retains diagnostic artifacts, finishes the
queue invocation unsuccessfully and creates no worker task; repair and a new
explicit task identity are required. No automatic retry consumes more model calls.

The queue is bounded to 256 pending entries and each plan to 16 dependencies.
Dependency reads are bounded by that queue. The local database and operator are
trusted; this is not protection against a malicious database administrator.
Accepted files are recaptured and compared before use, rather than assumed safe
because a historical hash exists. Original instruction pins remain authoritative,
including when copied repository text contains an instruction-like filename.

## Validation

Store regressions cover earlier/same-project admission, immutable cycle refusal,
missing/self/duplicate edges, dispatch-receipt versus authoritative acceptance,
unrelated eligible work, concurrent claim exclusion, atomic binding/audit rollback,
and restart persistence. A combined event/dependency regression verifies that an
ineligible dependency consumes no wake and a late claim failure rolls back both
source binding and event acknowledgement. Service tests refuse direct dependency
bypass and undeclared input; capture tests preserve deletions and reject changed
bytes, linked capture roots and linked manifests.

A separate launcher/HTTP/Chrome fixture prepared and submitted dependency plans
with dispatch disabled, verified duplicate idempotency and restart, displayed the
pending dependency identity, omitted stored plan bodies, refused unauthenticated
reads and kept the board token out of rendered text, browser storage and daemon
logs. Evidence is retained privately as `dependency-launcher-v1/receipt.json` and
`dependency-browser-v1/receipt.json` under the runtime review directory.

The first native chain copied the exact accepted producer tree and passed the
consumer's content check, but its independent reviewer rejected acceptance because
it lacked the original `result.txt` digest needed to verify preservation. This
failure is retained as `dependency-native-v1/failure.json`, with its recorded
verdict in SQLite. The service now materializes the in-memory starting manifest
as a private, non-replaceable `review-baseline.json` alongside a fresh, hash-matched
candidate capture in one read-only review workspace. Only the reviewer sees this
bundle, with a labelled digest reference. Candidate files live under `source/tree`;
the manifest is review evidence, not an extra project file. Both copied candidate
and baseline evidence are checked again after review, as is the accepted original
source. The existing adapter's workspace-only read boundary is unchanged. The model context remains bounded;
the manifest is capped at 4 MiB. A regression verifies that later work-tree edits
do not change this baseline evidence and that existing files/links cannot be
replaced. The original preservation requirement and checks were not relaxed.

An intermediate attempt to expose a sibling manifest was correctly refused by the
adapter's existing capability check before reviewer launch (`dependency-native-v2`).
The final bundle stays inside the admitted read root; it does not add external read
roots or reclassify evidence as authoritative instructions. That failed attempt and
a subsequent compile diagnostic are retained alongside the final validation logs.

The final real native chain (`dependency-native-v3/receipt.json`) completed both
Claude writer → checks → independent Codex review stages in 123.56 seconds. The
consumer was initially dependency-gated, duplicate submission was idempotent,
and the queue survived daemon restart. Its initial manifest exactly matched the
producer's accepted source identity; the consumer used that content without being
given its generated value in its objective or checks. Original Git HEAD/files and
producer artifacts stayed unchanged. After completion, a deliberately modified
producer capture was refused before creating a new task or execution, and frozen
bindings survived another restart. This is a two-task local synthetic chain, not
multi-host or soak acceptance. Previously accepted immutable adapter pins and
existing per-call caps were retained; actual model spend was not measured.

Final local validation: 195 workspace tests, strict Clippy, formatting, build,
Python/JavaScript syntax and credential/file-size checks passed. Source and binary
hashes are recorded in private `dependency-validation-manifest.json`; original
failures and all intermediate diagnostics remain retained.

The first PR CI run passed Linux but found that the new positive copy fixture used
macOS's noncanonical temporary-directory alias. A local workspace symlink reproduced
the identical refusal. The fixture now resolves its initial root before constructing
the canonical positive source; explicit linked-root/manifest negative controls are
unchanged. Production path validation is unchanged. Before/after alias logs and the
original CI failure are retained; a passing rerun alone was not treated as a fix.

## Continuation baseline requirement

Retained unfinished work is the next attempt's starting work, not the original task
input. Independent preservation review must keep the first attempt's manifest.
Resolve it from the unique durable execution bound to the original `work/tree`,
verify its packet identity/hash and baseline digest, and read the original bounded
private no-link manifest. Never derive original input evidence from current dirty
work. Refuse missing/ambiguous/corrupt original evidence before continuing; older
packets without a baseline digest cannot prove this requirement.

Continuation must keep original path inventory, including files deleted before
interruption, while preserving the retained bytes. Review bundles must label their
manifest as the initial task baseline. The next-attempt advisory inventory may
still describe retained work, but must not replace that preservation baseline.

The initial execution lookup is task-scoped, exact-workspace and bounded to two
matches; ambiguity is refused. The service checks its fence, packet digest,
attempt/epoch/worker/adapter, objective, checks and instruction pins, then requires
one supported original advisory manifest identity. The original manifest must be
canonical, private, regular, single-linked, bounded and digest-matched. This reuses
the existing durable packet; no new authority or migration is introduced.

Regression fixtures demonstrate that retained bytes and newly created files differ
from the initial manifest, keep an original deletion as a tombstone in the next
attempt, and preserve unfinished files without replacing the original evidence.
They reject altered valid manifests as well as malformed/missing/linked evidence,
wrong identity/fence/hash, absent legacy digests and ambiguous workspace records.
An initial ambiguity fixture accidentally left two attempts open; it was corrected
to close the first attempt, retaining the production one-open-attempt invariant.
The original failed test log remains retained.

Real `dependency-continuation-v1` acceptance paused the dependent Claude writer
after `consumed.txt` appeared, confirmed its stop fence, restarted the daemon and
refused a deliberately corrupted initial manifest without a new execution or task
version change. After restoration, queued continuation survived another restart
and completed checks plus independent Codex review in 78.96 seconds. The new
attempt/epoch retained unfinished work, while its review manifest exactly matched
the original task input and excluded the newly written file. Frozen predecessor
identity, old work, producer files and original Git checkout remained unchanged;
stale and terminal continuations were refused. The test used existing accepted
immutable adapter pins/caps; actual model spend was not measured.

Validation for this correction: 198 workspace tests, the expanded baseline
negative tests, strict Clippy, formatting and build passed. This proves the
specified local dependent-writer continuation, not generic unfenced recovery or
remote-host/soak acceptance. Evidence and intermediate diagnostics are retained in
the private runtime review directory.
