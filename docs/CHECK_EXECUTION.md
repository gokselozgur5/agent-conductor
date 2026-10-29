# Isolated check execution

`conductorctl run-check` runs an operator-approved command against a disposable
copy of an identified source tree. It does not contact the daemon, record an
authoritative task check, accept a verifier verdict or complete a task. This is
the check executor used to build the native task workflow.

```sh
conductorctl run-check \
  --source /absolute/source/tree \
  --manifest /absolute/snapshot/manifest.json \
  --definition /absolute/check.json \
  --destination /absolute/new-check-directory
```

The manifest is the `SnapshotManifest` produced by runtime source capture. A
check definition supplies exact argv; no implicit shell is introduced:

```json
{"id":"unit-tests","argv":["/usr/bin/true"],"timeout_ms":5000}
```

The example only demonstrates process execution; replace it with the actual
project acceptance command. An explicit `/bin/sh -c` definition authorizes that
shell command. Optional repeated `--toolchain-root` arguments grant canonical
read-only toolchain directories, not network or write access. The current
executor uses macOS Seatbelt or the Linux helper and bubblewrap backend.
See [Linux check setup](LINUX_CHECKS.md) for prerequisites and native-host
evidence. Unsupported hosts or unavailable isolation fail closed.

Each invocation requires a new destination. Capture refuses source links and
special files, verifies the resulting manifest hash against the expected source,
and runs only after that match. Generated files stay in the disposable tree.
A second check gets a fresh copy, so earlier test output cannot influence it.
Network and the original source are not granted write access. Cancellation,
timeout, output overflow and infrastructure failures are nonpassing outcomes.
Ctrl-C requests supervised cancellation before returning.

JSON receipts bind check ID, exact definition hash and source hash to the exit
outcome and bounded stdout/stderr hashes. Raw output is not retained or printed.
Exit status is zero for a passing check, one for a nonpassing runtime outcome,
and two for invalid input, source mismatch or isolation setup refusal.

The manifest is an input identity, not proof that the producing writer was
fenced. Source capture, writer fencing, receipt persistence and independent
verification still belong to the service integration. Disposable check trees
remain for inspection; this command does not delete them. A normal process exit
does not prove all descendants are gone, but their writable authority is limited
to that disposable check tree. Never reuse it as verification source or evidence.

## Service publication

The internal `PersistentControlService::execute_required_check` port connects the
executor to durable task state. It loads exact argv from the persisted attempt
packet rather than taking a command from worker output. It requires matching
packet/execution identity, the same current attempt/worker/epoch, a required check
and a bound `snapshot:<sha256>` source. Capture eligibility accepts a fenced
execution, an identified successfully exited full-capability execution, or an
authenticated account-channel candidate with an imported snapshot. Any other
unfenced execution for the task blocks capture. Account checks additionally bind
the selected source to the imported snapshot digest.

Eligibility to inspect an immutable candidate does not prove that an unrestricted
worker or its descendants have lost machine-wide write authority. It does not
authorize reassignment of the original workspace or retry of an uncertain live
execution. See [full-capability execution](FULL_CAPABILITY_EXECUTION.md).

After execution it reloads those bindings under the lifecycle mutation lock and
requires the task version to be unchanged. Only then does it transactionally
record `RecordCheck` as `conductor-check-executor`, with the hash-bound receipt in
the command audit reason. Runtime nonpassing outcomes record failed checks;
capture/isolation failures and concurrent lifecycle changes publish no result.
The real-process service regression covers successful publication and a task
cancelled while its check is executing. Neither path records a verifier verdict.

The normal daemon workflow dispatches required checks through this port after
capturing the candidate. Independent review is invoked only when every required
check returns `Passed`; a nonpassing check prevents completion. The standalone
`run-check` command still does not publish authoritative task state.

After an unsuccessful workflow, inspect the recorded checks and retained source
before choosing another action. Restart may move an unresolved unrestricted task
from `verifying` to `blocked` while preserving those records. That state change
does not turn a failed check into a pass or grant retry authority.
