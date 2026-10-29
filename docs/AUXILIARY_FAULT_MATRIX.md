# Auxiliary ownership fault evidence

Issue #89 contributes to FR-6 and NFR-1; this matrix does not close either full
requirement. A transaction exception, async cancellation, process exit, daemon
SIGKILL and physical host interruption are different evidence classes.

Native fixture names below identify retained local evidence under
`/Users/gok/codex/reviews/agent-conductor-runtime-20261028`. They are not portable
repository tests. Their manifests pin exact binaries and scripts; later source
changes do not silently update those observations. Repository tests are portable
and run through the workspace test command.

| Boundary | Check evidence | Verifier evidence | Remaining limit |
| --- | --- | --- | --- |
| During admission transaction before commit | Abrupt child exit at SQLite audit write | Same, separate role | Not host power loss |
| Admission committed, before any capture or spawn | Abrupt child exit immediately after admission returns | Same, separate role | Exercises store admission, not daemon scheduling |
| Capture/review-copy creation before launch binding | Synchronous failure and transactional no-launch controls | Same preparation refusal family | Dedicated abrupt filesystem-boundary control still required |
| Launch bound, native supervisor has not accepted launch | `auxiliary-unaccepted-check-v1` | `auxiliary-unaccepted-verifier-v1` | Controlled gate process exists; not a zero-OS-spawn claim; same-boot refusal only |
| Native Ready before durable ACK | `auxiliary-preack-recovery-v1` | `auxiliary-verifier-preack-v1` | Controlled Ready relay, versions differ |
| Durable ACK/release authorization before Release delivery | `auxiliary-postack-check-v1` | `auxiliary-postack-verifier-v1` | PID-preserving stdin relay; authorization is not delivery |
| Payload running during daemon loss | `auxiliary-recovery-v1` | `auxiliary-verifier-recovery-v1` | Owned process group; no generic escaped-session containment |
| Private terminal proof before terminal delivery/result publication | `auxiliary-terminal-check-v1` | `auxiliary-terminal-verifier-v1` | PID-preserving stdout relay |
| Settled ownership with result transaction interrupted at first task write | Abrupt child exit; exact pre-publication snapshot restored | Same, separate role | Store fixture; not daemon continuation orchestration |
| During domain publication transaction | Abrupt exits at task and publication-link writes | Same, separate role | Synthetic validated store batch; not a native adapter result |
| After check result, before verifier accepts launch | Prior check publication preserved in verifier unaccepted-launch fixture | Not applicable | Does not isolate the immediate post-commit scheduling boundary |
| After verifier result publication, before task completion | Prior check publication preserved | `verdict-completion-crash-v1`: accepted verdict committed, completion write refused, daemon SIGKILL; one continuation completes and survives restart | Controlled SQL refusal isolates the boundary; not every scheduling interleaving |
| After completed task, before dispatch bookkeeping | `completed-dispatch-crash-v1` reproduces stale queue after SIGKILL | Same completed workflow contains independent verdict | Controlled finish-write refusal isolates boundary; `completed-dispatch-repair-v1` repairs it without model calls or task mutation |

`transaction_cancellation::auxiliary_admission_survives_process_death_on_both_sides`
runs four real child processes, covering both roles and both sides of admission
commit. Before-commit exit occurs in the SQLite update hook without transaction
unwinding; reopening must reproduce the complete prior database snapshot. After
commit, reopening must retain the exact preparation, admission origin and audit,
with pending ownership and no launch/process/release. It also checks database
integrity. This adds process-death coverage to existing async cancellation tests.

`auxiliary_crash_tests::publication_survives_abrupt_exit_and_replay` runs six
real children: each role exits at the task write, at the final publication-link
write, or immediately after the commit returns. Before-commit exits must restore
the entire database snapshot, including the settled invocation and absent result.
After reopening, the same result is applied if necessary and then replayed without
any database change. Task aggregate/version, result audit payload, invocation link,
idempotency outcome and single transition are checked; changed result identity is
refused. Execution, invocation and host evidence remain unchanged.

The publication fixture uses synthetic store batches and settlement metadata. It
does not prove native child settlement or domain-level completion orchestration;
those require their separately identified controls. All child exit cases use
bounded parent waits and independent temporary databases.

The unaccepted-launch fixtures retain pending ownership without a native claim or
receipt through two same-boot daemon restarts. They refuse replacement execution
and preserve existing publication state. This is a successful refusal, not a
completed recovery. The native-host procedure still needs independent physical
boot acceptance; absence of a PID or receipt never supplies that proof.

The postACK fixtures accept an exact private `never_released` receipt while
preserving the independently observed process and recorded release authorization.
They do not erase either field to fit the no-ACK path. Both complete one
continuation, with old settlement audited before replacement execution and no
old-invocation result publication.

Prioritize the remaining abrupt filesystem capture/review-copy boundary next.
Do not aggregate these short native controls into a seven-day run or describe
synthetic host epochs as physical reboot acceptance. Full operational recovery,
privacy, supported Linux hosts and release acceptance remain separate gates.

## Completed task / interrupted dispatch repair

A native workflow exposed a stale queue entry: task completion committed, the
fixture refused the dispatch finish write, and SIGKILL/restart left the completed
task paired with `needs_recovery`. The task and its acceptance evidence were intact.

Startup and ordinary maintenance now reconcile at most 128 old-generation
completed dispatches per sweep, sharing a keyset cursor across store clones.
The task row and bounded aggregate must agree on identity, version and completion;
required checks, independent verdict, closed decisions and ended lease must match
the recorded snapshot source. The ended attempt and matching completion audit are
required. Active leases, unfenced writers, pending auxiliary or legacy ownership
refuse reconciliation. Malformed entries remain unchanged while later valid rows
can still be visited.

Only dispatch status/outcome and a distinct `workflow.completed_reconciled` audit
change, atomically. Task acceptance, original dispatch generation and automatic
recovery count remain unchanged. The outcome records historical accepted source;
it does not claim a new filesystem verification. No dispatcher flag, model access,
new execution or new completion command is needed. Audit failure or cancellation
rolls back the entire page; replay writes no duplicate audit.

The actual failed native database was preserved separately, then reopened with the
fix twice: dispatch became `finished`, task/attempt/lease/execution/effect/auxiliary
records stayed byte-equivalent, with one original completion and one reconciliation
audit. Both test daemons exited. This closes the observed bookkeeping defect, not
the separate verdict-to-completion orchestration gap.

## Accepted verdict / interrupted completion

`verdict-completion-crash-v1` uses the frozen completed-dispatch candidate
`89a4544`. Fixture-only SQLite triggers refuse completion and dispatch finish,
leaving a real accepted independent verdict and successful check publication
committed while the task remains `verifying`. The owned daemon is killed, the
triggers are removed, and a frozen pre-recovery database preserves that boundary.

With explicit fenced recovery enabled, restart performs exactly one continuation.
The new attempt passes its own check and independent review, reaches `completed`
and `finished`, and remains unchanged across another restart. Independent SQLite
verification confirms the two original auxiliary rows/publication links are
unchanged, all four invocations have scope-bound native settlement evidence, both
writer executions are fenced, and only one completion audit exists. Original
project files remain unchanged, excluding Git metadata. Owned daemons are stopped.

This is native orchestration evidence with controlled completion-write refusal,
not an abrupt exit inside the publication transaction or physical host failure.
The continuation re-runs work and verification; it does not promote the historical
verdict directly to completion. Real model calls were used; spend was not measured.
