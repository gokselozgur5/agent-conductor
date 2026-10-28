# Reassigning retained work

An operator can continue a fenced unfinished workflow under a different registered
worker identity. This preserves the immutable intake plan, original baseline,
retained files, checkpoint, history and required checks. The successor receives a
fresh attempt and higher lease epoch; it cannot approve its own completion.

## Select an admitted successor

Register the successor using its accepted adapter metadata, then provide a
`VendorPin` JSON file containing `kind`, `version`, `executable`,
`executable_sha256` and `conformance_sha256`. Registration alone does not establish
adapter conformance. Use an operator-accepted profile and the actual immutable
executable; never generate these values from worker prose.

```sh
conductorctl --socket /absolute/control.sock reassign-workflow TASK_ID VERSION SUCCESSOR_ID /absolute/successor.json
```

The board offers **Reassign retained work** for finished/interrupted nonterminal
workflow dispatches. Supply the registered agent ID and approved worker profile,
then confirm. A dispatcher may start model work immediately. The queue shows the
selected identity; it does not expose the pin file or credentials. Authentication
continues to come from the daemon environment, not the supplied profile.

Supported successors currently use the admitted restricted Claude file-tool
writer profile. A different registered Claude identity is a real new ownership
holder; this is not evidence for cross-vendor or cross-host writer isolation.
Codex remains available as an independent reviewer. Command-capable Codex writers
and remote-host takeover remain refused until their isolation/fencing is accepted.

## Admission and continuity

Admission requires the exact current task version, a nonterminal task, confirmed
prior write fences and a valid retained packet/checkpoint. The target must differ
from the current worker and the workflow verifier, have registered write access,
and match the registered adapter kind, conformance fingerprint and executable
hash. The actual binary and existing capability contract are checked before
queue admission and again at launch. Registration is checked again at dispatch.

The original plan is never rewritten. A separate assignment record, continuation
queue update and audit event commit together. Audit failure rolls back all three.
Identical replay while the same version remains queued does not create another
audit event. Competing or stale queued requests are refused. The operation grants
no new roots, tools, network policy, acceptance criteria or review authority.

Continuation still copies retained dirty work to a new private attempt tree and
validates the original task baseline. Checkpoint text remains an unverified claim
from its recorded source attempt. Subsequent ordinary continuation keeps the
selected successor; another explicit reassignment can select a different one.
A normal queue `finished` status is not proof that the task completed.

The local socket assumes a trusted local operator. This does not add per-agent
transport authentication or a remote host protocol. Unsupported or uncertain
fences remain blocked; absence of a PID is not a fence.

Migration 0015 stores the selection and makes older binaries refuse startup
rather than silently continue with the original worker. Preserve a consistent
backup before upgrade; do not remove migration records to downgrade.

## Validation

The candidate passes 222 workspace tests, strict Clippy, formatting and release
build. Added regressions cover SQLite audit rollback, replay, stale/conflicting
selection, immutable original plans, restart, unfenced execution, same worker,
unregistered targets, verifier identity and profile mismatch.

An actual Chrome probe on a copied paused native database submitted the form,
confirmed selected-worker CLI parity and cleared pending responses/form data on
disconnect. No model was launched by that browser fixture. An actual older daemon
refused the migrated database. Native successor acceptance is recorded separately.

A real pinned Claude writer was paused after creating `answer.txt`. Its stop was
fenced; the daemon restarted, then the operator selected a different registered
Claude identity using the same accepted profile. Identical queued replay produced
one audit event. After another restart the successor received the exact stored
checkpoint, a fresh attempt/epoch and a copy of retained work. Required checks and
independent Codex review completed successfully in 80.47 seconds after admission.
Original plan bytes and prior work stayed unchanged; stale and terminal requests
were refused. This proves local ownership transfer, not a different-vendor writer
or remote isolation. Actual model spend was not returned.

Private evidence: `reviews/agent-conductor-runtime-20261028/`, including
`reassignment-native-v1/receipt.json`, `reassignment-browser-v1/receipt.json`,
`reassignment-downgrade/receipt.json` and `reassignment-workspace-v2.log`.

Cancellation correction: a SQLite update-hook barrier aborts the request after
the assignment write. The original manual transaction failed this regression,
leaving a selection on the shared writer connection; the owned SQLx transaction
rolls back on drop and permits a subsequent clean reassignment. Both source
variants were explicitly rebuilt after cleaning the store crate. Other manual
transaction owners require a separate audit under issue #43; this test does not
prove those paths safe.
