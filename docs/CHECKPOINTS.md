# Native worker checkpoints

Writable native attempts receive a bounded progress protocol in their trusted
adapter prompt. Claude assistant text and Codex completed agent messages may
publish a standalone `conductor_checkpoint` JSON envelope. The runtime does not
create checkpoint files in the project, grant additional tools, or infer progress
from arbitrary prose, tool output or process exit.

The record contains schema version, task/attempt/lease identity, increasing
sequence, completed work, current state, next action, open decisions, claimed
running commands and changed artifacts. These fields are **unverified worker
claims**. Commands are not a live process inventory. Questions do not create
formal task decisions. Claims cannot complete a task or authorize an action.
Independent source-bound checks and review remain required.

## Admission and durability

The incremental stream parser accepts complete vendor JSON lines up to 512 KiB.
Checkpoint envelopes are at most 4096 UTF-8 bytes, summaries at most 512 bytes,
and each list at most eight nonempty strings of at most 256 bytes. Sequences are
1 through 4096. Unknown fields, malformed data, wrong identity, duplicate or older
sequences are ignored. The parser may coalesce claims from the same read chunk.

An eight-entry nonblocking output queue prevents progress backpressure from
blocking the worker watchdog. Intermediate claims can be dropped when full;
this is not a lossless event log or a guarantee that every edit has a checkpoint.
The last accepted record is authoritative only as an observation of a claim.
A worker that publishes no valid claim has no structured checkpoint.

Service admission verifies the current wrapper, started execution, packet hash,
current attempt and live monotonic ownership. Persistence uses the existing
atomic execution revision and audit transaction. An audit failure rolls back the
record. Stale ownership is refused and cannot extend the worker lease.

Continuation retains the latest accepted record in the new resume packet, apart
from retained files and the original task baseline. If a later attempt has no
new claim, the inherited record remains available only when its validated,
redacted record matches the older fenced execution (including observation and
packet identity). Missing or corrupt bindings are refused. A legacy
attempt without structured claims receives an explicit absence notice, not a
fabricated progress summary. Workers must recheck claims against retained work.

## Operator access

`conductorctl task-checkpoint TASK_ID` and authenticated
`GET /api/checkpoint?task_id=TASK_ID` expose the same projection. The board's
**View checkpoint** action shows source attempt, lease epoch, sequence and
observation time, with explicit stale/unverified or inherited labels. It renders
all claim content as text. Closing or disconnecting invalidates pending responses.
No raw transcript, process environment or complete resume packet is exposed.

## Validation scope

Tests cover fragmented and malformed streams, bounds, vendor message types,
stale sequence/identity, wrapper and monotonic ownership, SQLite audit rollback,
fenced restart, inherited-origin corruption and stream delivery before exit.
The wrapper backpressure regression checks that expired supervision stops further
worker writes even while checkpoint output is unread. Native and browser
acceptance evidence is recorded separately; synthetic tests do not establish
that every vendor run follows the requested publication cadence.

The delivered local candidate passes 215 workspace tests and strict Clippy.
The repeated descendant-stop diagnostic passed 120 races in 52.79 seconds.
Real Chrome verified authenticated access, CLI parity, literal markup rendering
and stale-response clearing after disconnect. Its database was a synthetic copy.

A real pinned Claude attempt published a checkpoint, wrote `answer.txt`, and was
paused with a confirmed restricted stop. After daemon restart, continuation
received the identical accepted checkpoint and preserved the unfinished file.
The new attempt produced the required files, passed checks and independent Codex
review. Its final checkpoint reached sequence 12; the inherited interruption
checkpoint was sequence 1, so this proves transport and continuity, not a fresh
semantic summary after every edit. No checkpoint project files were created.

Two earlier native probes are retained as failures: one paused after verification
had begun; another raced normal exit and did not establish the requested stop.
Neither is counted as successful pause acceptance. Actual model spend was not
returned. Private evidence: `reviews/agent-conductor-runtime-20261028/`, including
`checkpoint-native-v3/receipt.json`, `checkpoint-browser-v1/receipt.json`,
`checkpoint-workspace-final.log` and `checkpoint-stop-120.log`.

## Credential-shaped claim redaction

The stream boundary and direct service admission replace recognized credential
spans with `[redacted]` in completed work, current state, next action, open
decisions, running commands and changed artifacts. Original bounds and ownership
are validated before normalization. Identity, epoch, sequence and packet hash
are preserved; redaction grants no authority.

The literal policy recognizes Anthropic/OpenAI key prefixes, GitHub classic and
fine-grained prefixes, GitLab personal-token prefixes, and case-insensitive
`Bearer ` values with at least eight token characters; AWS access IDs with their
16-character uppercase/digit suffix; and generic, RSA, EC, OpenSSH or encrypted
PEM private-key blocks. An unterminated recognized private-key block consumes the
rest of that field. Token characters are ASCII letters, digits, underscore,
hyphen, period, slash, plus and equals. Matches can remove credential-like benign
text. Unknown formats, encoded values and secrets split across fields are not
detected.

Existing records are validated and redacted on projection and continuation.
Historical database bytes and backups are not rewritten. Old binaries and direct
access to old records can still expose their original content. This policy does
not sanitize task identity, objectives, event payloads, source files, process
logs or all resume-packet content. It is not a general secret-detection guarantee.

Regression coverage includes a stream test that fails before this fix, all six
free-text fields at direct admission, persisted receipt checks, legacy projection
without database rewriting, inherited correspondence and ordinary-text tampering
refusal. Independent task acceptance remains unchanged.
