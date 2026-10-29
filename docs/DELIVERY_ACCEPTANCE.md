# Delivery acceptance audit

This is an incomplete delivery audit, not a v1 acceptance certificate. The target
remains the complete workflow and all gates in [REQUIREMENTS.md](REQUIREMENTS.md).
No local test count, short native fixture, or successful refusal substitutes for
that scope. Read this current audit before the historical paragraphs in STATUS.

## Capability correction

The current acceptance target includes full shell, build/test, tool and network
capability for the worker, per the operator's explicit direction. Workers use
separate OS accounts with machine-wide access under their account permissions
and the operator-selected Codex/Claude bypass modes. Existing file-only broker and restricted-writer
acceptance must not be presented as completion of that target. The critical
user-visible scenario is a normal CLI-created task dispatched to a full-capability
agent, followed by independent verification and exactly-once completion, including
interruption and recovery. See [the integration plan](FULL_CAPABILITY_EXECUTION.md).

## Candidate identity

Main at `7f24b10c3432867779681e85939ddc6f670a550a` includes merged PR #113
and the preceding broker, auxiliary and native-observation changes. Schema is 29.
PR #113 CI and its post-merge run passed on macOS and Linux. These results cover
the configured CI checks, not Linux native sandbox support or full acceptance.
Delivered bundles, existing runtimes and the separate endurance fixture retain
their own pinned versions; repository merges do not upgrade them.

The service-authority candidate drives an already released broker/vendor from
current task ownership. It checks authority generation, task/attempt/epoch,
monotonic lease and instructions, caps deadlines at the acknowledged launch
ceiling, and latches revocation on failure. Controlled native pause testing proves
process termination and durable broker closure separately. Native daemon Codex
writer admission and complete execution fencing remain unavailable.

## Functional audit

Every row remains partial until the whole requirement has matching acceptance
coverage. The source references identify implementations to inspect, not proof
that every required behavior is accepted.

| Requirement | Present source / bounded evidence | Remaining delivery evidence |
| --- | --- | --- |
| FR-1 intake | Project registration, canonical discovery, approved instruction pins; service workflow dependencies and instruction invalidation tests | Repeat complete operator intake on the final candidate and supported hosts |
| FR-2 resume | Runtime contract, instructions, checkpoint and scoped advisory modules | Complete final-adapter acknowledgement/context and hostile-input acceptance matrix |
| FR-3 Kanban | Domain transitions, task pagination/history, authenticated board | Final operator scenario with all task controls and CLI/UI equivalence |
| FR-4 ownership | Domain leases, execution/effect transactions, snapshots, continuation/reassignment | Command-capable writer containment, cross-host fencing and supported-host acceptance |
| FR-5 progress | Runner checkpoint/renewal protocol and durable checkpoint records | Final native monitoring across interruption and long-running work |
| FR-6 recovery | Auxiliary fault matrix, native writer recovery, completed-dispatch repair, operator recovery tools | Remaining lifecycle/effect coverage inventory, physical host exercise and full combined-candidate acceptance |
| FR-7 events | Store event routing and repository observation; source echo, atomic delivery and bounded queues | Final native multi-agent event/ownership scenario |
| FR-8 wake | Atomic eligibility plus coalesced dispatch notifications; measured wake probes documented in WAKE_SCHEDULING | Final native workload and continuous resource/invariant acceptance |
| FR-9 completion | Source-bound isolated checks, independent verdict, atomic publication, exact-once completion controls | Final combined-candidate scenario including all hostile/failure controls |
| FR-10 authority | Capability refusals, restricted Claude writer, isolated checks and read-only verifier | Complete supported-adapter denied-action suite; generic escaped-session fencing is not proved |
| FR-11 adapters | Pinned Claude writer and Codex/Claude reviewer; vendor conformance controls | Codex writer is explicitly rejected by native_writer_launch; local-auth-only Claude workflow is not established by API-key-backed fixtures |
| FR-12 operator | CLI, authenticated local board, evidence, diagnostics, controls | Final installation/runbook acceptance; remote authenticated encrypted access not established |
| FR-13 audit | Transactional audit, fixed public errors, bounded evidence and reported usage | Full privacy-surface review and final causal-record walkthrough |

A proposed bounded broker route, with actual local tool-discovery evidence and
remaining acceptance gates, is recorded in [CODEX_WRITER_DESIGN.md](CODEX_WRITER_DESIGN.md).
It does not change native admission.

The Codex writer gap is directly enforced in `crates/runtime/src/vendor.rs`:
`native_writer_launch` only admits Claude without workspace commands. This is an
intentional safety refusal, not support for both required writer adapters.

## Non-functional and release gates

| Requirement | Current evidence | Unfulfilled gate |
| --- | --- | --- |
| NFR-1 reliability | Transaction rollback/cancellation/process-death controls and short native SIGKILL fixtures; separate running control-plane soak | Seven continuous days, complete transition/effect coverage and integrated native invariant monitoring are not established |
| NFR-2 performance | Documented board/load and wake measurements on earlier pinned binaries | Recheck final candidate at declared load; do not transfer timing bounds between binaries |
| NFR-3 portability | Rust core has prior macOS/Linux CI history | Current sandboxed_command rejects every non-macOS host; Linux isolated checks, both Linux architectures and release artifacts remain incomplete |
| NFR-4 security/privacy | Local auth, explicit capabilities, literal checkpoint redaction and UI controls | Full logs/events/resume/source/metadata canary audit and encrypted remote transport acceptance |
| NFR-5 evolvability | Versioned migrations and strict protocol/adapters | Final upgrade/rollback/refusal rehearsal across the delivered artifacts |

`crates/runtime/src/sandbox.rs` explicitly returns `Unsupported` for non-macOS
hosts. Compiling the Rust core on Linux cannot establish usable native workflows
there. The current CI matrix does not explicitly cover Linux arm64. The earlier hosted billing failure is no longer the current CI state; the exact
PR #109 and post-merge runs succeeded on both configured platforms. No local Docker, Colima, Lima, Podman or
QEMU executable was found during this audit; that is a tooling observation, not
proof that no Linux execution route exists.

Physical reboot approval and Thor access remain separate external prerequisites.
Do not restart the Mac or the existing soak, clear uncertainty, relax isolation,
or bypass repository policy to produce a green delivery indicator.

## Required acceptance sequence

1. Preserve the combined auxiliary/native-observation acceptance evidence below,
   then complete the remaining lifecycle/effect and long-running acceptance.
2. Close capability gaps for Codex writing and Linux isolation using explicit
   design and real denied-action/fencing evidence before admitting them.
3. Audit every failure case listed in REQUIREMENTS section 6 against exact tests;
   close uncovered cases rather than treating a family test as complete coverage.
4. Complete the privacy-surface and supported-host acceptance suites, installation
   artifacts and operator recovery rehearsal; resolve hosted CI/access blockers.
5. Complete the declared uninterrupted endurance run without combining restarts
   or short fixtures, then execute the section-8 operator acceptance with both
   writer vendors and review the full causal record.

The current frontend is plain JavaScript, while PHASES historically proposed
React/TypeScript and SSE/WebSocket. That implementation-plan discrepancy is not
resolved by the native recovery tests. The final audit must distinguish functional
operator acceptance from unfinished or superseded design choices explicitly.

## Auxiliary observation integration

The initial integration audit found writer-only live observation: auxiliary argv
had only an invocation UUID and the inspector queried only `executions`. The
candidate now emits a scoped auxiliary observation tag and correlates live tags
and stored auxiliary process identities. Supplied tags must match the validated
private launch before the runner claims the invocation or emits Ready. Legacy
untagged transport remains accepted by the runner, but does not become matching
live observation merely because it has a PID.

The combined candidate passes 348 Rust and 112 Python tests, strict Clippy,
formatting and build checks. The first native guard run stopped when its 10-second
observation grace elapsed during verifier preparation; that failed run is retained.
A separate run using the existing configurable 60-second grace completed with
332 samples, matching live observations for both auxiliary roles, sampled board
correspondence, an unchanged original project and one completion. Independent
SQLite checks bind those observations to the two settled invocations. A separately
spawned scoped orphan is detected without database mutation or observer signaling.

This closes the identified live auxiliary observation gap for the tested scenario,
not every failure interleaving, escaped descendant containment, Linux operation or
seven-day acceptance. See NATIVE_OBSERVATION_GUARD for policy and evidence limits.
