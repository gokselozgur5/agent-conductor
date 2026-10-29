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

Normal completion records settlement only after the owned process primitive
returns with group absence confirmed. A clean leader exit now also checks group
absence; it never signals a numeric identity after reaping. Runtime errors with
uncertain settlement leave the invocation pending. Settlement and audit are
atomic and tied to the original runtime generation. Direct claim/requeue and continuation refuse while
any auxiliary invocation remains pending, independently of the writer fence.

This is an intermediate implementation, not complete issue acceptance. Pending
invocations have no durable process identity or restart-verifiable terminal
receipt yet. A crash can therefore leave a task conservatively blocked even
after its check exits. The next stage must provide a tagged wrapper identity,
birth/host binding and release handshake, private terminal proof, bounded
reconciliation and stale-publication refusal for checks and verifiers. Existing
schema-19 verification work has no auxiliary record and needs an explicit
migration/recovery policy before deployment.

The shared cold-database validator refuses pending auxiliary invocations for
backup and restore paths, even if no live writer remains. Before merge/deployment,
finish host-recovery policy and operator/invariant reporting; verify both process
kinds at all external-effect boundaries. Process-group absence is not generic
escaped-descendant containment. Keep sandbox enforcement and source immutability
separate from process settlement. Neither a passing store test nor a passing
short native run closes FR-6/NFR-1 or the seven-day requirement.

## Auxiliary supervisor protocol candidate

`conductor-runner --auxiliary INVOCATION_UUID` implements a separate bounded
protocol. It is tested directly with controlled shell processes; service calls
still use the prior direct process path until the persistence/client integration
is complete. This mode alone does not enable automatic recovery.

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

Next integration must persist the launch digest and independently observed
supervisor identity before Release, retain the wrapper pin and safe receipt root,
renew/control the session, and reconcile exact terminal evidence transactionally
before releasing pending ownership. Bind check/verdict publication to that same
invocation. Do not serialize credentials into the database to reconstruct a launch;
recovery needs its digest and terminal proof, not the secret launch environment.
