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
