# Phase 1 acceptance and handoff

## Scope and gate

Phase 1 is the durable control core, not the full agent-management product.
Implementation baseline: `751ba5b`. Local acceptance: formatting, strict Clippy,
and three full workspace runs of 97 tests passed on macOS. A fresh independent
review at `5d26e14` accepted every historical reproducer family and the additional
effect-directive retry fix; its separate build also passed 96 tests, formatting
and strict Clippy. No reproduced findings remain open. Both macOS and Linux
jobs passed at published `bebddb9` in [CI run 40646746932](https://github.com/gokselozgur5/agent-conductor/actions/runs/40646746932).
Phase 1 is accepted within the boundaries below; subsequent revisions still
require their own exact-SHA CI.

The evidence below names executable tests, not inferred guarantees. Run all with
`cargo test --workspace --all-features`; individual names can be supplied as its
final filter. Tests use temporary databases and fake resources, not user projects.

## Objective-to-evidence matrix

| Objective | Executable evidence | Limit |
| --- | --- | --- |
| Rust domain and protocol | `every_supported_command_has_an_explicit_legal_and_illegal_state_table`, `generated_sequences_match_the_independent_lifecycle_model` | Exhaustive depth-five reference-model sequences (66,430); not every possible execution |
| Versioned store and durability | `migrations_enable_required_sqlite_settings`, `close_and_reopen_preserves_committed_truth_and_idempotency` | SQLite WAL/FULL and reopen; no disaster-recovery rehearsal |
| Atomic lifecycle/audit/outbox | `outcome_audit_effect_and_idempotency_commit_atomically`, `duplicate_transition_rolls_back_the_whole_batch`, `optimistic_conflict_rolls_back_every_append` | Store transactions; no distributed transaction claim |
| Idempotency and concurrency | `identical_command_replays_stored_outcome_without_duplicate_rows`, `idempotency_key_with_different_request_is_rejected`, `concurrent_expected_version_writers_have_one_winner` | Explicit actor/key/request identity |
| Daemon/service/store integration | `persistent_mutations_replay_and_survive_daemon_restart`, `task_lists_round_trip_through_the_real_socket` | Real local socket and SQLite, fake agents |
| Restart and lease recovery | `consecutive_restarts_keep_recovery_heartbeat_usable`, `restart_requires_recovery_heartbeat_before_mutation`, `recovery_requires_a_new_generation_and_refences_each_restart` | Injected clock and reopened store; no real worker process fencing |
| Deadline and stale epoch | `stale_epoch_and_worker_deadline_are_rejected`, `heartbeat_is_allowed_after_mutation_cutoff_but_before_lease_expiry`, `active_lease_requeues_only_after_deadline_and_grace` | Monotonic protocol fence, not filesystem permissions |
| Attempt identity and frozen contract | `reused_attempt_id_with_changed_identity_rolls_back_every_projection`, `resumed_task_updates_future_checks_without_mutating_ended_attempt` | Historical attempt preserved; fresh attempt freezes future checks |
| Automatic reconciliation | `maintenance_requeues_expired_work_and_abandons_its_spawn_intent` | Timer requeues expired leases; does not run a vendor adapter |
| Fake external-effect crash boundaries | `abort_before_intent_commit_leaves_no_partial_state`, `every_committed_effect_edge_converges_after_process_abort` | Subprocess abort at intent/claim/external/observation/done edges; fake worker-spawn inventory |
| Effect ownership and cancellation | `observed_effect_transfers_only_after_original_runner_deadline`, `cancellation_abandons_unexecuted_spawn_intent_atomically`, `ending_attempt_abandons_its_unfinished_effect_in_same_transaction` | No real process spawn/termination |
| Contradictory inventory | `contradictory_and_unmatched_inventory_fail_closed`, `done_inventory_contradiction_blocks_without_illegal_abandon`, `effect_contradiction_directive_blocks_the_owning_task`, `fake_inventory_directive_reaches_audited_task_blocking` | Typed fake directive is applied through the internal port; retry/restart replay and changed-payload refusal are covered |
| Event replay, source echo and caps | `semantic_dedup_ignores_fresh_envelope_id_but_rejects_changed_content`, `source_echo_is_disabled_unless_explicitly_enabled`, `causal_generation_and_fanout_are_bounded_atomically` | Durable semantic delivery, no filesystem watcher in Rust |
| Cancellation/event ordering | `cancellation_serializes_before_task_event_publication` | SQLite writer barrier forces Cancel before PublishEvent; zero event/delivery rows |
| Delivery claim fencing | `stale_claim_token_and_expired_claim_cannot_ack_state`, `event_age_expiry_rejects_ack_and_state_atomically`, `state_effect_and_ack_succeed_or_roll_back_together` | Consumer state and acknowledgement are atomic |
| Independent verification | `worker_cannot_verify_itself_even_if_presented_as_a_verifier`, `worker_cannot_record_checks_or_reject_verification_as_a_verifier`, `completion_rejects_missing_or_stale_evidence` | Identity checks; no actual check execution or source capture |
| Reachable completion | `privileged_source_binding_completes_a_real_daemon_task`, `source_binding_cli_maps_to_privileged_request` | Fake/operator source label through privileged socket; external System commands remain denied |
| Actor binding | `external_actor_roles_are_bound_to_registered_access` | Registered roles, not authenticated principals; socket access is privileged |
| Invariant failures | `invariant_scan_reports_projection_ownership_boot_and_completion_contradictions` | Injected corrupt fixtures expose projection, lease, and self-verification contradictions |
| Transport resource limits | `oversized_responses_become_bounded_structured_errors`, `unread_responses_time_out_and_release_connection_permits`, `completed_connection_tasks_are_reaped_during_operation`, `stalled_overload_handler_is_bounded_and_cancelled_on_shutdown` | Normal connection pool plus one bounded rejection handler; further excess closes immediately; no load/SLA claim |
| CLI error behavior | `json_error_uses_the_structured_nonzero_exit_status`, `structured_errors_have_stable_distinct_exit_codes` | JSON and human modes retain failure exit codes |
| Runtime ownership/shutdown | `runtime_lock_fences_second_owner_and_recovers_a_stale_socket`, `runtime_lock_never_removes_a_non_socket_path`, `stalled_client_does_not_block_shutdown` | Cooperating daemon owners must share the lock path |

## Independent review outcome

The final reviewer reran the handoff's first- and second-round reproducer families:
repeat-restart recovery, attempt identity, task-list encoding, stale events and
ACKs, cancelled intents, invariant corruption, response resource limits, handler
draining, CLI exit status, effect-runner transfer, automatic lease maintenance,
actor binding, Cancel/PublishEvent ordering, ended-attempt contract updates,
self-verification and the real socket source-binding/completion path. All closed
within the scope limits in the matrix above.

That review found one new defect: a repeated effect-contradiction directive used
a new task version with its old idempotency key. `5d26e14` hashes immutable
versioned directive input instead. The reviewer's unchanged failing probe now
passes; changed-reason collision, restart replay and exactly one blocked
transition are verified. External lifecycle commands still hash their complete
versioned command payload. Review acceptance is targeted Phase 1 evidence, not a
comprehensive security audit or full v1 acceptance.

## Cross-platform correction

Initial publication `5770ea7` passed macOS CI but failed two Linux overload tests
in run `40645977411`: closing with unread request bytes produced ConnectionReset.
`751ba5b` replaces immediate best-effort writes with one bounded rejection handler:
limited request draining, awaited timed response writing, and shutdown tracking.
Additional excess connections close immediately; malformed or stalled peers are
not promised a structured response. The regression verifies the extra handler
cannot create an unbounded pool or delay shutdown. Both cross-platform CI jobs
passed at `bebddb9`, closing the initial Linux failure. Independent source review accepted the
fix at `751ba5b`; a fresh daemon build passed all 17 daemon tests, including
separate reruns of both initially failing tests.

## Requirement-by-requirement boundary audit

| Requirement | Phase 1 result and remaining work |
| --- | --- |
| FR-1 intake | Canonical local project/Git identity registration; authorized-root policy, instruction trust and continuity discovery belong to later runtime integration |
| FR-2 resume packets | Deferred to Phase 2; Python spike is not Rust production evidence |
| FR-3 task board | Authoritative lifecycle and task queries; complete dependency/decision/parent-child product model and UI are not claimed |
| FR-4 ownership | Task attempts, epochs and structural uniqueness; actual worktree/write-surface/remote fencing deferred |
| FR-5 progress | Heartbeat/recovery lifecycle; full adapter progress packets and live checkpoint integration deferred |
| FR-6 recovery | Restart, expiry, durable intents and fake inventory recovery; real process/filesystem inventory deferred |
| FR-7 events | Semantic publication and transactional delivery; filesystem attribution/debounce deferred |
| FR-8 wake | Durable fake effect identity and bounded delivery; real wake scheduler/adapter absent |
| FR-9 verification | Independent records and source-label equality; real immutable source, executor and write fence deferred |
| FR-10 authority | Privileged operator socket and typed role restrictions; sandbox capability enforcement remains Phase 2 |
| FR-11 adapters | Explicitly absent; no vendor API calls or process launches |
| FR-12 interface | Rust CLI; HTTP and dashboard deferred |
| FR-13 audit | Core transitions, effects and events persist audit evidence; vendor usage/cost unavailable |
| NFR-1 reliability | Core transaction/restart/crash tests; all-real-transition crash suite and seven-day soak remain v1 gates |
| NFR-2 performance | Unmeasured targets; no latency, idle CPU or sustained-load guarantee |
| NFR-3 portability | macOS/Linux CI gate; ARM64/AMD64 release artifact coverage remains Phase 6 |
| NFR-4 security | Local socket, role/fence refusals and credential preflight; comprehensive adapter/packet/log redaction suite deferred |
| NFR-5 evolution | Migration and protocol versions, required-field refusals; full resume/adapter/event forward-compatibility contracts remain their owning phases |

These deferred items remain requirements. Phase 1 acceptance does not satisfy
full v1 acceptance or authorize launching write-capable agents.

## Reproduction and publication gates

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features
cargo test --workspace --all-features
cargo test --workspace --all-features
git diff --check
```

Run the credential-pattern preflight in `.github/workflows/ci.yml`; inspect staged
and tracked paths for private media/weights. Verify the unpushed author and
committer addresses are `gokselozgur5@gmail.com`. After independent acceptance,
publish clean `main` to the private repository and inspect both Rust CI jobs at
the exact pushed SHA. Preserve atomic commits when correcting CI failures.

## Fresh-session entry

Read `AGENTS.md`, `REQUIREMENTS.md`, `DESIGN.md`, `THREAT_MODEL.md`, `PHASES.md`,
`STATUS.md`, and this file. Check current Git status and remote CI rather than
assuming an earlier snapshot is current. Preserve dirty work. Continue only the
phase authorized by the operator; do not infer adapter or paid-service authority
from this document.
