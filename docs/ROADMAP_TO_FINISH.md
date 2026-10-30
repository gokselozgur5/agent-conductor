# Roadmap to finish Agent Conductor

Status: active implementation plan, 2026-10-30. Baseline: `99183c0`.
The previous 75% estimate and 8/5/4/2/4/2 remaining-work allocation are planning
estimates, not measured completion, elapsed-time forecasts or acceptance evidence.
A stage is done only when its exit evidence is recorded against its exact source.

## Finish definition

One operator request in one shared room reaches a configured coordinator, produces
bounded authorized delegation, executes with the intended accounts, returns
results/blockers to the coordinator, and produces a visible consolidated response
without operator relay. Managed task completion still requires independent
acceptance and required checks. Restart cannot duplicate native work; uncertain
execution remains blocked until the existing recovery authority permits action.
The delivered release must also satisfy the original functional/non-functional
requirements; working chat alone does not complete the product.

## Work packages and dependencies

| Package | Slices | Depends on | Exit evidence | Current state |
| --- | --- | --- | --- | --- |
| P1 Delegation (estimated 8%) | D1 strict decision contract and persisted author provenance; D2 atomic assignment graph, root/parent binding and cumulative limits; D3 native prompt/output integration and authorized queue admission | Existing room topology and native reply storage | One default coordinator request creates authorized subordinate work; forged authors, peer/ancestor targets, changed replay, excessive fan-out/depth and crash duplicates are refused | D1 locally validated; D2/D3 pending |
| P2 Reporting loop (5%) | R1 durable result/blocker correlation; R2 wait-for-children and one coordinator continuation; R3 bounded correction rounds and explicit terminal/blocked outcomes | P1 D2/D3 | One operator request produces worker results and a consolidated coordinator response; restart and duplicate receipts cannot repeat rounds; exhausted budgets surface as blocked | Pending |
| P3 Account execution (4%) | A1 reuse account-worker transport for conversations; A2 own-HOME/auth/profile enforcement and failure handling; A3 distinct-account real-vendor acceptance | P1 contract for delegated inputs; transport audit can begin independently | Native agents demonstrably execute under intended distinct accounts without credential copying or a same-account fallback | Pending; personal account selection/login remains external prerequisite |
| P4 Operator visibility (2%) | U1 per-recipient reason/state/causality projection; U2 assignment tree, reports, blockers and final summary; U3 reconnect/layout/privacy acceptance | D2 for U1; P2 for U2 | Browser explains who received what, why and with which outcome; refresh preserves identity; text cannot execute markup | Room setup/addressing exists; delivery visibility pending |
| P5 Combined acceptance (4%) | V1 controlled end-to-end and fault matrix; V2 real Codex/Claude and distinct-account scenarios; V3 original requirements audit, load, privacy, supported hosts and endurance | P1/P2/P3/P4; audit and harness work can proceed in parallel | Exact-source evidence for every required gate, with skips/blockers visible; no substitute of fixture results for real vendor acceptance | Earlier bounded evidence exists; final-candidate acceptance pending |
| P6 Delivery (2%) | L1 atomic commits and issue/PR metadata; L2 exact-head CI and release kit verification; L3 upgrade rehearsal, operator runbook and live activation | Validated slices for L1; P5 for final release acceptance | Verified artifact hashes, preserved state/identity, passing post-upgrade scenario and accurate handoff; no interruption of active user turns | Four baseline local commits remain separate from installed preview |

Critical path: D1 → D2 → D3 → R1/R2/R3 → combined native acceptance → release.
Account transport investigation, acceptance inventory and UI projection design may
run in parallel. Integrate one validated slice at a time; never merge conflicting
schema edits from independent workers without an explicit integration review.

## First executable slice: D1

- Parse a complete, versioned JSON delegation decision, not commands hidden in prose.
- Accept target and instruction fields only. Author, parent/root identity, launch
  identity and budgets come from authoritative service state, never model output.
- Resolve the source reply through its completed native turn and immutable stored
  author; reject operator text or an agent-looking message without that binding.
- Validate each target against the configured direct reporting relationship.
- Bound decision bytes, instruction bytes and recipients; reject duplicates and
  unknown fields. These are per-decision bounds, not the durable cumulative limits
  that D2 must implement.
- Add regressions for forged authority, hierarchy bypass, malformed/oversized
  decisions and stable parsing. D1 performs no launch or persistence mutation.

D2 will persist a separate authorized assignment identity and its causal graph in
one SQLx-owned transaction. Do not turn agent prose into operator-authored messages
or broadly remove the operator-only filters in queue/reservation/runner code.
D3 can execute only an assignment authorized for that exact recipient. The
existing no-retry reservation and room-wide unknown-turn exclusion remain. D2 must
accept the source reply ID and re-read/validate provenance and authority within
its admission transaction; public D1 proposal values are not authorization tokens.

## Original gates that remain mandatory

- Reconcile all FR-1 through FR-13 rows in [delivery acceptance](DELIVERY_ACCEPTANCE.md)
  against the final candidate, including independent verification/publication,
  event/wake behavior, interruption, recovery and audit/privacy surfaces.
- Seven uninterrupted days and the required transition/effect failure coverage.
  The monitor must cover native orphans/unfenced surfaces and bounded memory/queue
  growth. A control-plane-only soak does not prove native worker endurance or full v1.
- Final-artifact board p95 below 100 ms at the declared 100-project/10k-task/100k-event
  load, wake p95 below 500 ms and idle CPU measurement; preserve measurement
  conditions and failed runs.
- Supported-host native and release acceptance, full privacy canary coverage,
  authenticated encrypted remote access where required, and upgrade/rollback
  or explicit refusal rehearsal. Do not transfer older binary evidence to a new kit.
- Rehearse actual same-host reboot recovery when the operator authorizes disruption.
  Cold backup of native-chat state remains refused, including replied turns, until
  an accepted write-fence/recovery procedure exists; do not weaken that refusal.
- Unrestricted descendant fencing/recovery policy cannot be inferred from a reply,
  process-group exit or timeout. Preserve existing refusals while resolving the
  remaining operator policy and host/account access prerequisites.

## Slice delivery discipline

For each slice: pin source → implement → meaningful regressions → format/strict
Clippy → relevant broader checks → credential scan → atomic commit → record
source/evidence hashes and remaining gaps. Track implementation, tested candidate,
CI-verified release and installed behavior separately. Refresh GitHub/CI/runtime
state before external delivery; dated documents are not current service state.

## Progress log

- Baseline `99183c0`: shared room routing/configuration and controlled browser
  fan-out tested locally; real autonomous delegation is not implemented.
- D1 locally validated: strict typed decision parser, settled native-reply provenance
  lookup and read-only service inspection. Six parser, four conversation integration
  and two turn/provenance tests pass; strict store/service Clippy, formatting and
  changed-source credential scan pass. An initial test-only Clippy warning was
  corrected without suppressions. Independent source review found no D1 blocker;
  added negative controls for state/author/room mismatches.
- Next executable slice: D2 atomic assignment graph and cumulative root/depth/round
  budgets with cancellation/restart/replay regressions. D1 cannot launch work and
  is not completion of P1. No live upgrade or new real-vendor call was performed.
