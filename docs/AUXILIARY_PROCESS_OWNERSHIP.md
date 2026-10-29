# Auxiliary process ownership

Issue #89 tracks checks and independent verifiers that outlive their daemon.
A fenced writer does not establish settlement of these later processes. Actual
native crash controls reproduced automatic continuation while an old approved
check shell remained alive in its disposable sandbox.

The candidate adds schema 20 auxiliary invocation records before check capture
or review-copy preparation and process launch. Each immutable record binds a
fresh invocation UUID, task/version, writer execution/attempt, boot generation,
source identity, definition digest, role and private destination. Preparation
and audit commit together. Identical retries return no launch permission;
changed identity reuse refuses. One pending invocation excludes another for
that task. Admission requires the current verifying task/source, a fenced writer,
no unfenced writer and the current runtime generation.

Schema 21 binds the exact launch digest, wrapper path/hash and private receipt
root before supervisor creation. The parent independently samples the Ready PID,
kernel birth and host epoch, then atomically records that identity and release
authorization before writing Release. Replays grant neither spawn nor release.
Task/version/source/authority/boot ownership is rechecked at both admission steps.
The runtime authority is the existing 64-character digest, not an invocation UUID.

Both native required checks and independent review use this session. The client
renews while awaiting a bounded terminal frame without discarding partial reads.
A terminal event alone cannot publish results: the service must first verify the
private receipt against persisted launch/process identity and unchanged wrapper
pin. Settlement and audit commit together. Restart reconciliation accepts an old
invocation's proof only under the current runtime generation and same authority;
it runs before interrupted workflow admission. Direct claim/requeue, reassignment
and continuation refuse pending auxiliary ownership independently of writer fences.

This remains intermediate issue acceptance. Pre-acknowledgement recovery now
accepts only a private `never_released` receipt matching the persisted invocation,
authority, original boot and launch digest, after rechecking the retained wrapper
pin. The receipt's self-reported process identity remains receipt evidence:
`process_json` stays NULL and `release_recorded` stays zero. A started/settled
process receipt cannot substitute for missing acknowledgement. Current authority
and runtime generation, exact launch binding and atomic settlement/audit are
required; malformed, missing or nonprivate evidence leaves ownership pending.
The supervisor's exclusive invocation claim prevents replay from releasing a
second payload. Recovery never signals a receipt PID or invents a live observation. Synchronous capture, review preparation and launch-definition failures now
settle an unbound invocation with an `auxiliary.not_launched` audit in the same
owning runtime. The transaction refuses any launch binding, process identity,
release authorization or changed generation. It cannot be used as restart proof;
audit failure preserves pending ownership. Errors after launch binding require supervisor evidence. A wrapper killed
before receipt publication, malformed launch input or a failure before it accepts
the launch still leaves uncertainty; absence is never a no-launch receipt. Schema 22 preserves legacy uncertainty separately from invocation evidence.
A noncompleted task with current or historical verification and no auxiliary
invocation receives a `legacy_auxiliary_ownership` marker; an existing pending
invocation without a launch binding also receives one. Task aggregates and old
invocations remain unchanged. Completed history and fresh tasks are not marked;
already bound pending invocations retain their existing receipt recovery path.

The marker blocks continuation, direct claims, dispatch admission, new writer
executions and auxiliary launches. Persisted legacy queue entries are excluded
from selection and active queue capacity so they do not block unrelated work.
`check-invariants` reports `legacy_auxiliary_ownership_unknown` with a bounded
list and total count. Backup/restore validation refuses these markers even when
the writer is fenced. Reopening the database neither duplicates nor clears them.

Before upgrading, finish ordinary legacy verification under the old runtime
where possible. If ownership is already uncertain, preserve the state and
artifacts: do not delete the marker, fabricate an invocation or treat cancellation
as settlement. No acknowledgement or clear-marker command is provided. A verified
legacy settlement/host-recovery procedure is still required before deployment
can be claimed complete. This barrier prevents unsafe reuse; it does not resolve
unknown historical processes or certify arbitrary escaped descendants. Complete boundary coverage, operator/invariant reporting and host recovery
remain required. Actual native crash controls now cover both a running required
check and an independently observed verifier child: after daemon death, private
settlement is audited before the replacement writer is prepared, and each task
completes with one automatic continuation. These two boundaries do not cover
the complete fault matrix.

The shared cold-database validator refuses pending auxiliary invocations for
backup and restore paths, even if no live writer remains. Before merge/deployment,
finish host-recovery policy and operator/invariant reporting; verify both process
kinds at all external-effect boundaries. Process-group absence is not generic
escaped-descendant containment. Keep sandbox enforcement and source immutability
separate from process settlement. Neither a passing store test nor a passing
short native run closes FR-6/NFR-1 or the seven-day requirement.

## Auxiliary supervisor protocol candidate

`conductor-runner --auxiliary INVOCATION_UUID` implements a separate bounded
protocol. Native service check/review calls now use it through `AuxiliarySession`.
Controlled shell tests exercise the transport independently of model behavior.

The parent supplies one private stdin launch frame. It includes authority and
boot identity, an immutable invocation UUID, canonical working/receipt paths,
exact argv/environment, executable digest and finite runtime/output/heartbeat
limits. Credentials remain in memory and pipes; only the launch digest is safe
to persist. The caller must construct the sandbox and keep the receipt directory
outside **all** child write roots. The supervisor itself is not a sandbox.

Before Ready, the supervisor durably claims its UUID with an exclusive private
marker, preventing simultaneous/replayed wrappers from sharing an invocation.
Ready contains the launch digest and native PID/birth/host identity. The parent
must independently observe that child, persist its acknowledgement, and only then
send Release for that exact digest. Executable content and process birth are
rechecked before release. Until then, no payload process exists.

EOF, malformed control or heartbeat expiry cancels the supervised process. A
terminal receipt is published before a terminal event and contains no raw output,
environment or credentials. `never_released` is emitted only on the irrevocable
pre-spawn return; killing the wrapper before publication leaves uncertainty.
`owned_group_settled` requires a successful owned-process result or a bounded
cancellation/timeout/output-limit result with group cleanup. Other errors produce
no settlement receipt. A live group member after leader exit prevents proof.
This observation is not proof that a deliberately escaped new session is absent.

Receipts use private, no-replace, fsynced publication. Reads refuse links,
nonprivate files, malformed/oversized/schema-mismatched content, changed metadata,
and every mismatch against trusted invocation/authority/boot/launch/process
expectations. No diagnostic or receipt reader signals an old numeric PID. Partial
publication is not accepted as completion and cannot be repaired by guessing.

The persisted launch digest covers the final executable, complete argv and
private environment. Secret launch bytes are never stored to support recovery;
its persisted digest and terminal proof are sufficient. The initial definition
record remains distinct from that final launch binding. Check result and verifier
publication retain their task/version/source revalidation after settlement.

Schema 23 links each new native result to one settled invocation and one audit
record. The task update, result audit, idempotency record and publication link
commit together. Publication requires an acknowledged `owned_group_settled`
receipt, unchanged process/launch scope, current task/attempt/source/boot/authority,
a fenced writer and no pending or legacy ownership. A never-released receipt is
recovery evidence only and cannot publish a check or verdict. A link failure rolls
back the result; identical replay adds nothing and a second result identity for
the same invocation refuses. Result audit correlations and reviewer usage use the
auxiliary invocation ID. Older/manual results are not backfilled with invented
provenance.

## Local validation

Build matching workspace binaries before the service's actual-supervisor tests:

```sh
cargo build --workspace
CONDUCTOR_TEST_BIN_DIR="$PWD/target/debug" cargo test --workspace --all-features
```

When using `CARGO_TARGET_DIR`, set the binary directory to that target's `debug`
directory instead. CI builds the binaries first and supplies this explicit path;
tests do not silently use a runner from another checkout or skip the production
supervision path when it is unavailable.

### Terminal delivery after control-pipe closure

After Release, a renewal or cancellation write can fail while the supervisor is
finishing its output. The parent retains the same in-flight framed reader and
stops sending controls after that failure. It allows at most five additional
seconds, still bounded by the original execution deadline, to receive the
terminal frame. Invalid frames, EOF, another Ready, failed supervisor exit and
missing private settlement proof remain refusals. A broken control pipe alone
never establishes settlement or authorizes publication.

Real-pipe regression peers consume Release and the initial renewal, close stdin,
and gate terminal output until the parent has attempted renewal or cancellation.
These are deterministic transport controls, not a claim that every real-worker
shutdown schedule has been observed.
