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
executor requires macOS Seatbelt; other hosts fail closed.

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
