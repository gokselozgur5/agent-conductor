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
| Settlement commit before domain publication | Store publication preconditions/rollback | Same role controls | Dedicated abrupt process control still required |
| During domain publication transaction | Injected SQL error rolls back domain/link/audit | Same role controls | SQL exception is not abrupt process death |
| After check result, before verifier accepts launch | Prior check publication preserved in verifier unaccepted-launch fixture | Not applicable | Does not isolate the immediate post-commit scheduling boundary |
| After verifier verdict, before task completion | Not applicable | No dedicated abrupt control in this inventory | Still required |
| After completed task, before dispatch bookkeeping | Reopened completion is stable in native recovery fixtures | Same workflow outcome | Orderly final restart is not abrupt termination at this exact boundary |

`transaction_cancellation::auxiliary_admission_survives_process_death_on_both_sides`
runs four real child processes, covering both roles and both sides of admission
commit. Before-commit exit occurs in the SQLite update hook without transaction
unwinding; reopening must reproduce the complete prior database snapshot. After
commit, reopening must retain the exact preparation, admission origin and audit,
with pending ownership and no launch/process/release. It also checks database
integrity. This adds process-death coverage to existing async cancellation tests.

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

Prioritize the remaining publication transaction and filesystem boundaries next.
Do not aggregate these short native controls into a seven-day run or describe
synthetic host epochs as physical reboot acceptance. Full operational recovery,
privacy, supported Linux hosts and release acceptance remain separate gates.
