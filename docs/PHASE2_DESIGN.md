# Runtime delivery design

## Objective and acceptance

The operator has authorized continuing beyond Phase 1 to a usable Conductor.
The target remains the complete product workflow: register a real project, assign
work to locally authenticated Codex/Claude CLIs, observe progress on the shared
board, interrupt and recover without losing dirty work, execute declared checks,
and complete only after independent source-bound verification. Hooks, operator
UI, host support and hardening remain in the delivery plan, not silently removed.
The seven-day soak cannot be proved in a single day; usability and full v1
hardening acceptance must be reported separately.

## Runtime boundary

Add `conductor-runtime` outside the pure domain. It owns validated attempt
contracts, immutable resume packets, process supervision and workspace snapshots.
The daemon owns lifecycle decisions and SQLite transactions; the runtime cannot
infer completion from vendor exit status or text. Vendor output is bounded,
untrusted diagnostic data. A successful vendor result is a candidate for checks
and independent review, never a terminal task transition.

An attempt contract binds task, attempt, epoch, worker, exact adapter fingerprint,
workspace identity, starting HEAD, authoritative instruction digests, check
commands and a typed capability envelope. All required fields are validated
before an effect can launch. A packet hash is derived from canonical serialized
contract data. Acknowledgement must bind the same attempt, epoch and hash;
missing/stale/forged acknowledgement refuses work. An adapter wrapper may attest
only delivery/parsing of context, never claim the model obeyed arbitrary prose.

## Workspaces and fencing

Each write attempt uses a Conductor-owned isolated workspace under an authorized
runtime root. Source checkouts and dirty files remain untouched. Record initial
Git identity and snapshots including tracked/untracked changes; reject escaping
symlinks and unexpected branch/HEAD changes. Never reset/clean a recovery surface.
Use a process group and independent watchdog for each attempt. Cancellation or
lease loss must fence the group before reassignment or source capture. A worker
that escapes provable fencing blocks reassignment rather than being assumed dead.

The operator socket and authoritative database are not worker capabilities.
Workers receive only their own work surface and bounded context. Adapter tools,
filesystem boundaries, network and credential scope must be verified with actual
negative tests before enabling writes. Unknown versions/capabilities fail closed.
No unrestricted/bypass launch is an acceptable substitute for a working boundary.

## Durable supervision

Persist runtime intent before creating a process. A uniquely tagged wrapper owns
startup acknowledgement and runtime identity; it cannot release the worker until
Conductor has recorded that identity. Restart reconciles durable intent against
owned wrappers. PID alone is never sufficient identity. Heartbeat and checkpoint
records carry the attempt/epoch and are rejected after fencing. Exit, timeout,
malformed output, unsupported fingerprint and lost acknowledgement are explicit
nonterminal results. Retries are bounded and preserve snapshots and checkpoints.

## Verification

Stop/fence the writer, capture a complete immutable source manifest, execute the
operator-declared checks in a separate bounded environment, and expose the same
source to a read-only distinct verifier. Store exit status, environment identity,
source hash and bounded artifacts. Recheck the source before completion. Changed
source, failed checks, absent decision or missing verdict cannot complete. No
untrusted check command may acquire credentials or mutate the control plane.

## Delivery sequence

1. Validated versioned attempt/capability/resume contract and refusal tests.
2. Durable runtime records, fake wrapper handshake, process/group watchdog,
   cancellation and restart fault harness.
3. Isolated workspace preparation, complete dirty snapshots and source manifests.
4. Pinned Codex/Claude conformance adapters; actual denied-action tests before
   write-enabled integration. Preserve existing local authentication; no API key
   requirement or automatic paid-cloud provisioning.
5. Integrated start/pause/resume/cancel/check/verify operator CLI and real project
   acceptance with each vendor, including an interrupted dirty attempt.
6. Coalesced attributed hooks and bounded wake scheduling through the same API.
7. Local authenticated operator API and board UI, matching CLI semantics.
8. Host protocol and Thor acceptance, then load/restore/redaction/soak and releases.

Each coherent slice runs formatting, strict lint, relevant fault tests and
credential/media checks before an atomic personal-identity commit. Phase gates
must carry exact executable evidence and outstanding requirements. Keep current
progress in STATUS; this document records architecture, not a success claim.

## Adapter evidence

Installed versions observed on 2026-10-28: Codex CLI 0.163.0, Claude Code 2.1.311.
These are discovery snapshots, not automatically trusted compatibility ranges.
Codex non-interactive JSON events and sandbox controls are described in the
[official guide](https://learn.chatgpt.com/docs/non-interactive-mode).
Claude print/structured output and permission controls are described in its
[CLI reference](https://code.claude.com/docs/en/cli-reference).
Conformance must inspect the installed help and exercise behavior; documentation
alone does not prove the local binary enforces a capability.
