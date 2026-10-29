# Threat and Failure Model

## Assets

- Operator authority and credentials
- Project source and uncommitted work
- Task, lease, checkpoint, audit, and verification truth
- Host compute, network access, and paid model budget
- Privacy of prompts, transcripts, repository data, and artifacts

## Trust boundaries

The operator and local Conductor configuration are trusted. Repository content,
worker/verifier prose, hook payloads, vendor output, recovered dirty files, and
remote-host observations are untrusted. Vendor binaries and adapters are trusted
only for the capabilities proven by their current conformance fingerprint.

## Primary threats and required controls

### Prompt injection expands authority

Repository text asks an agent to publish, delete, expose a secret, change checks,
or approve itself. Control: typed authority envelope, pinned trusted instruction
hashes, labelled untrusted context, capability removal, independent verification.

### Stale worker continues after reassignment

A suspended or partitioned worker resumes with an expired lease. Control: lease
epochs on every command, shorter worker deadline, isolated write surface, process
fencing, grace period, and blocked state when the former host cannot be fenced.

### Crash splits state from an external effect

The daemon dies around process spawn, kill, wake, or worktree mutation. Control:
transactional intent/outbox, tagged idempotent effects, result recording, and
startup reconciliation with failpoint coverage on both sides of every boundary.

### False or stale completion evidence

A worker submits fabricated output or changes source after testing. Control:
Conductor-controlled checks, immutable source identity, write-fenced verification,
environment fingerprints, and a read-only distinct verifier.

### Event storm, replay, or causal loop

Repeated hooks and filesystem writes create unbounded wakes. Control: normalized
idempotency keys, durable deduplication, at-least-once consumer transactions,
source/correlation/generation propagation, age/fan-out/retry caps, and no echo.

### Dirty work is lost or mistaken for trusted progress

A process dies mid-edit or an external actor changes HEAD. Control: preserve
tracked and untracked recovery snapshots, hash them, label them unverified, and
block on unexpected branch/HEAD changes.

### Credential or private-data exposure

Secrets enter logs, resume packets, child environments, prompts, or Git. Control:
allowlisted environment construction, redaction tests, bounded context assembly,
credential-free workers by default, secret-pattern preflight, and no transcript
publication without explicit authority.

For the implemented question/rationale boundary and its legacy-record limits,
see [decision text privacy](DECISION_PRIVACY.md). This is one covered surface,
not proof that every output and context field has been audited.

### Vendor drift changes lifecycle semantics

A CLI update changes hooks, exit behavior, or session recovery. Control: pinned or
recorded versions, startup conformance handshake, protocol fingerprint, fail-closed
disablement, and checkpoint recovery independent of vendor sessions.

### Local database corruption or rollback

State is damaged, restored from an old copy, or partially migrated. Control:
checksummed migrations, integrity checks, schema version gates, tested backup and
restore, monotonic aggregate versions, and visible recovery mode rather than
automatic destructive repair.

### Cost or resource exhaustion

An agent loops, fans out, emits giant payloads, or starts excessive workers.
Control: concurrency and spend envelopes, payload and queue caps, bounded retries,
rate limits, cancellation, and audited budget exhaustion.

## Abuse cases required in tests

- A repository instruction requests `git push` and verifier self-acceptance.
- A worker resumes after its lease epoch has been replaced.
- A daemon crash occurs before and after every fake external effect.
- Worker evidence is green for a different tree hash.
- Source mutates while a verdict is pending.
- A filesystem event loses attribution and attempts to restart generation zero.
- A duplicate delivery is replayed after consumer work but before acknowledgement.
- A dirty worktree contains untracked files when ownership changes.
- A vendor fingerprint changes between attempts.
- Secret-shaped fixtures pass through every logging and packet path without leakage.

## Residual risk

Same-user local processes can often inspect or interfere with one another. v1 is
not a hostile multi-user security boundary. Write-capable adapters are usable only
when their declared sandbox and tool controls can enforce the task envelope; if
that cannot be demonstrated, Conductor must restrict them to read-only work or
refuse launch.
