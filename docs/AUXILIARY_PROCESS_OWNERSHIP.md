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
the launch still leaves uncertainty; absence is never a no-launch receipt. Legacy schema-19 verification work and
schema-20 pending invocations lack supervisor metadata and require an explicit
migration policy before deployment. Complete boundary coverage, operator/invariant reporting and host recovery
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
