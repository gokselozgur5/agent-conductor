# SQLite transaction cancellation

Every explicit store transaction now owns a SQLx `Transaction`. `BEGIN IMMEDIATE`
retains writer serialization; dropping an unfinished transaction queues rollback
before the shared writer can execute its next command. Helpers consume the
transaction and commit only a successful result. Domain rules, schema version 15,
audit ordering and idempotency keys are unchanged.

## Reproduced defect and correction

On base `d155ae9`, aborting a caller after a real SQLite write bypassed the manual
result-based rollback helper. Uncommitted changes remained visible on the shared
writer connection. This could poison subsequent operations; the tests do not
claim that a reader connection observed those uncommitted rows.

`crates/store/src/transaction_cancellation.rs` uses an update-hook barrier to pause
the SQLite worker after the selected write, aborts and awaits the caller, then
releases SQLite. It compares all application tables on the writer and a separate
reader to their initial contents, and successfully retries the real operation.
The second batch barrier also verifies rollback of earlier batches.

| Transaction owner | Cancellation barrier | Old manual owner |
| --- | --- | --- |
| `create_project` | project insertion | Partial state reproduced |
| `create_task` | task insertion | Partial state reproduced |
| `persist` | project insertion | Partial state reproduced |
| `persist_sequence` | second project insertion | Partial state reproduced |
| `start_runtime` | generation update | Partial state reproduced |
| `mutate_effect` (through claim) | effect update | Partial state reproduced |
| `publish_event` | delivery insertion after event | Partial state reproduced |
| `publish_repository_observation` | observation update after event/fanout | Partial state reproduced |
| `claim_event_delivery` | delivery update | Partial state reproduced |
| `acknowledge_event_with_state` | delivery update after consumer state | Partial state reproduced |
| `enqueue_workflow` | wake insertion after dispatch | Partial state reproduced |
| `claim_workflow` | dispatch update | Partial state reproduced |

All twelve corrected owners pass the same cancellation controls. Execution
preparation and execution update already owned transactions; new controls verify
both at their execution write. Reassignment already has its own deterministic
cancellation regression. This covers all fifteen explicit transaction owners
found in the store source. Shared-owner controls do not independently inject
cancellation into every domain branch, and single-statement autocommit writes
are outside this inventory.

The `transaction_owners_survive_abrupt_process_exit` test in `transaction_crash.rs` reuses the
fourteen base-owner fixtures plus reassignment and completed-dispatch
reconciliation for 32 process-death cuts: one at the selected
SQLite write before commit, and one after the owner's successful return. Each
child exits without Rust unwinding. The parent reopens SQLite, checks integrity,
and compares every application table through both writer and reader connections
against the pre-operation or committed snapshot. The committed control must
actually change state. This covers the selected write for each selected owner, not
every statement, domain branch, later-added owner or external filesystem/process
effect. It does not simulate power loss or establish retry authority.

## Commit and failure boundaries

Cancellation is not a promise that an operation never happened. Once COMMIT is
issued, SQLite may finish it after the caller disappears. Two commit-hook tests
cancel persistence and event publication while COMMIT is executing, permit the
commit, verify writer/reader agreement and require the retry to return Replayed
or Duplicate without changing any table. Callers must reconcile uncertain
responses using existing idempotency identities, not create a new operation.

Existing semantic tests retain audit-failure rollback, version conflicts,
idempotency collisions and successful commits. The cancellation probes establish
Tokio task-abort behavior on this SQLite/SQLx implementation; they are not power
loss, disk-full, OS-crash, remote-agent fencing or full-product soak evidence.

## Validation provenance

Private evidence is under `reviews/agent-conductor-runtime-20261028/`.
`transactions-before-v4.log` records twelve partial-state failures on the old
production source; `transactions-workspace-v2.log` records the corrected twelve
controls and workspace suite. The store crate was cleaned and recompiled between
variants. Final local validation passes 238 workspace tests, formatting and strict Clippy,
including the execution and lost-response controls.

Earlier failed harness runs are retained: v1 omitted fixture agent foreign keys;
v2 assumed all tables had rowid; v3 proved eleven failures but its ACK hook targeted
a WITHOUT ROWID table, for which SQLite does not call the update hook. The final
ACK barrier targets the delivery update after consumer state. These harness
failures are not counted as product reproductions. No acceptance condition was
weakened to hide them.

## File broker transaction owners (schema 26)

`register_file_broker`, `admit_file_operation`, `finish_file_operation` and
`close_file_broker` each own a SQLx transaction covering the state mutation and
its audit append. `file_broker_tests` deterministically aborts each caller at the
actual SQLite row mutation, compares the complete application-table snapshot and
then proves the same operation can succeed. Separate audit-refusal controls cover
all four owners. This extends the historical owner inventory above; the old
238-test validation count is not the current workspace count.

`file_broker_crash_tests` additionally exits a real child without unwinding at
these four pre-commit mutations and at their post-commit boundaries. A ninth cut
occurs after actual file publication but before result persistence. The parent
reopens SQLite, requires complete rollback or the exact committed state, compares
unrelated ownership tables and audits, preserves file bytes, refuses replay of
committed effects and rejects false closure of an unknown operation. These
process exits are distinct from future cancellation and physical power loss.

## Auxiliary host reconciliation crash boundaries

`host_reconciliation_survives_abrupt_exit_on_both_sides` exercises both host
observation and settlement branches using synthetic native and legacy auxiliary
records. Eight real child exits occur at the audit write before commit or after
successful return.
Reopening must preserve the entire pre-transaction or post-transaction table
snapshot through both writer and reader connections, with SQLite integrity intact.
The after-commit positive control must change state. These selected boundaries do
not prove physical reboot, unrestricted worker fencing, or every statement/branch.
