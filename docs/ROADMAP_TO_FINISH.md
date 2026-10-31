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
| P1 Delegation (estimated 8%) | D1 strict decision contract and persisted author provenance; D2 atomic assignment graph, root/parent binding and cumulative limits; D3 native prompt/output integration and authorized queue admission | Existing room topology and native reply storage | One default coordinator request creates authorized subordinate work; forged authors, peer/ancestor targets, changed replay, excessive fan-out/depth and crash duplicates are refused | D1/D2/D3 locally validated with controlled processes; real-model shared-room proof pending (P5 V2) |
| P2 Reporting loop (5%) | R1 durable result/blocker correlation; R2 wait-for-children and one coordinator continuation; R3 bounded correction rounds and explicit terminal/blocked outcomes | P1 D2/D3 | One operator request produces worker results and a consolidated coordinator response; restart and duplicate receipts cannot repeat rounds; exhausted budgets surface as blocked | R1/R2/R3 locally validated with controlled processes |
| P3 Account execution (4%) | A1 reuse account-worker transport for conversations; A2 own-HOME/auth/profile enforcement and failure handling; A3 distinct-account real-vendor acceptance | P1 contract for delegated inputs; transport audit can begin independently | Native agents demonstrably execute under intended distinct accounts without credential copying or a same-account fallback | A1 reusable runtime API implemented; worker/service wiring and personal account acceptance pending |
| P4 Operator visibility (2%) | U1 per-recipient reason/state/causality projection; U2 assignment tree, reports, blockers and final summary; U3 reconnect/layout/privacy acceptance | D2 for U1; P2 for U2 | Browser explains who received what, why and with which outcome; refresh preserves identity; text cannot execute markup | U1 locally validated and rendered in headless Chrome with controlled fixtures; U2/U3 pending |
| P5 Combined acceptance (4%) | V1 controlled end-to-end and fault matrix; V2 real Codex/Claude and distinct-account scenarios; V3 original requirements audit, load, privacy, supported hosts and endurance | P1/P2/P3/P4; audit and harness work can proceed in parallel | Exact-source evidence for every required gate, with skips/blockers visible; no substitute of fixture results for real vendor acceptance | Read-only gate matrix/verifier implemented; final-candidate acceptance pending |
| P6 Delivery (2%) | L1 atomic commits and issue/PR metadata; L2 exact-head CI and release kit verification; L3 upgrade rehearsal, operator runbook and live activation | Validated slices for L1; P5 for final release acceptance | Verified artifact hashes, preserved state/identity, passing post-upgrade scenario and accurate handoff; no interruption of active user turns | Four baseline local commits remain separate from installed preview |

Critical path: D1 → D2 → D3 → R1/R2/R3 → combined native acceptance → release.
Account transport investigation, acceptance inventory and UI projection design may
run in parallel. Integrate one validated slice at a time; never merge conflicting
schema edits from independent workers without an explicit integration review.


## Parallel execution lanes

The operator approved three concurrent implementation lanes, with one integration
owner. Lane 1 owns D2 then D3; lane 2 owns account conversation transport; lane 3
owns the acceptance matrix and evidence tooling. The integration owner reviews
shared contracts, schema compatibility and combined tests before atomic commits.
Reporting, UI and delivery take the next available lanes as their dependencies
land. Distinct file ownership prevents concurrent schema/service edits.

D3 follows D2 with exact `(source_reply_id, agent_id)` authorization in queue,
reservation and runner admission. Its prompt must use the stored assignment
instruction. Ordinary replies remain information only. Persist examination outcomes
for ordinary/authorized/rejected replies so malformed output cannot starve later
work; distinguish semantic refusals from database failures. Add blocked admission
for missing configured recipients, preserve unknown-turn exclusion, and prove
coordinator → subordinate execution without operator relay. Supervisor follow-up
turns and reporting-round budgets remain P2 rather than implied D3 behavior.

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

The machine-readable gate inventory and evidence verifier are documented in
[finish acceptance evidence](FINISH_ACCEPTANCE.md). A passing evidence-contract
check is not independent proof that producer observations are true.

## Slice delivery discipline

For each slice: pin source → implement → meaningful regressions → format/strict
Clippy → relevant broader checks → credential scan → atomic commit → record
source/evidence hashes and remaining gaps. Track implementation, tested candidate,
CI-verified release and installed behavior separately. Refresh GitHub/CI/runtime
state before external delivery; dated documents are not current service state.

CI lanes (2026-10-31): pull requests run debug checks on macOS, the primary
platform, in about five minutes. Linux x86-64/arm64, the release build and the
runtime kit run on every push to main and on manual dispatch; a red main is
fixed before the next merge. Release candidates still require every host.

## Progress log

- Baseline `99183c0`: shared room routing/configuration and controlled browser
  fan-out tested locally; real autonomous delegation is not implemented.
- D1 locally validated: strict typed decision parser, settled native-reply provenance
  lookup and read-only service inspection. Six parser, four conversation integration
  and two turn/provenance tests pass; strict store/service Clippy, formatting and
  changed-source credential scan pass. An initial test-only Clippy warning was
  corrected without suppressions. Independent source review found no D1 blocker;
  added negative controls for state/author/room mismatches.
- D2 adds atomic assignment batches, persisted root/parent/author, depth at most
  four, at most eight recipients per decision and 32 assignments per root request.
  Reporting-round limits remain P2. Queue/reservation operator filters are unchanged;
  authorization alone does not launch work.
- A1 adds a reusable UID-authenticated conversation channel with exact durable ACK
  before Release, bounded frames and supervised cancellation. It is not selected
  by the persistent worker or conversation service yet. Tests use controlled
  executables/current UID, not distinct personal vendor accounts.
- Lane 3 adds 24 deliberately pending finish gates and a read-only evidence-contract
  verifier, including native-endurance sample checks. No final candidate is pinned.
- Next: D3 exact-recipient execution and durable reply-examination outcomes; A2
  worker/service transport integration; final native monitor and evidence producers.
  No live upgrade or new real-vendor call was performed by this parallel batch.

- Parallel batch validated: D2 eight store tests, five service tests (including
  strict-contract parity across eleven decisions), A1 eight controlled transport
  tests, schema upgrade and backup fixtures, and strict store/service/runtime
  Clippy passed. Evidence-tool final suite passed sixteen tests, including a
  reproduced and fixed hash/read race. These are local slice checks, not complete
  P1/P3/P5 or installed-product acceptance.

- D3 locally validated (`bada6c2`..`9ee0704`, twenty commits of at most 90 lines):
  schema 39 records one examination outcome per settled room reply (ordinary,
  authorized, rejected with reason; database failures are returned, not
  recorded); queue admission and launch reservation accept a native reply only
  for its exact authorized recipient; the runner prompt carries the stored
  instruction with supervisor, root request and depth; agents with direct
  reports receive the strict JSON delegation contract; a recipient without a
  configured profile is a visible blocked job and no longer disables the room.
  Controlled regression: one default coordinator request → coordinator decision
  → one worker launch with the stored instruction → supervisor information
  delivery; restart and replayed routing add no launch. Store 173 tests,
  conversation_native 9 tests; strict Clippy and formatting pass at every commit.
  Four store and three service mutations each turned a new test red. This is a
  controlled-process proof; real Codex/Claude delegation remains P5 V2, and
  supervisor follow-up turns on reported results remain P2.

- P2 R1/R2 locally validated (`2850cc3`..`3d2ebda`, each commit at most 90
  lines): schema 40 `conversation_continuations` opens one continuation per
  delegation batch once every recipient is blocked, failed or replied with
  something other than an authorized delegation; a recipient that delegated
  further settles after its own continuation; unknown outcomes never settle.
  Only an opened continuation admits the author's turn on its own decision. The
  continuation prompt carries the reports as information deliveries and
  withholds the delegation contract; a delegation in that reply is recorded as
  rejected and launches nothing. Controlled room: operator request → coordinator
  decision → worker result → coordinator consolidated reply, three launches, no
  launch after restart. Correction rounds and their budget remain R3.

- P2 R3 locally validated (`d5eaa79`..`de19cf1`, each commit at most 50 lines):
  schema 42 adds a correction round to delegation batches. A delegation made in
  the author's continuation keeps the root and depth and advances the round; a
  third correction is recorded as rejected with "correction round budget
  exhausted". The continuation prompt offers a correction only while budget
  remains. Controlled room: two corrections, seven launches, refused third.

- P4 U1 locally validated: the history RPC returns every recipient of each
  message (assigned, delegated, informed) with reason and delivery/execution
  state, plus the examination outcome of agent replies; the room renders this
  as text, including a refused decision's reason. Store, service and DOM tests
  cover it, and the chat DOM tests now run in CI. Rendered Chrome acceptance,
  the assignment tree view (U2) and reconnect/layout checks (U3) remain.

- P4 U1 rendered acceptance at `b3c8ea4`: headless Chrome drove the board
  against a debug daemon with two controlled CLI fixtures (no model calls). One
  default request produced decision, worker result and consolidation (three
  launches). Each message showed its recipients, reason and state and the
  authorized decision; markup stayed text; layout fit; the view was identical
  after reload; no token in storage, URL or body; disconnect cleared the room.
  The same probe against the pre-#224 daemon (`3d7c8fd`) failed on every
  recipient check while delegation still ran. Private evidence:
  `reviews/signal-evidence-20261030/room-visibility-v1` and `-negative-v1`.
  Observed for U2: a decision renders as raw JSON, and an information delivery
  to an agent that never runs again stays "pending".
- P4 U2 in the terminal: the history RPC names the message each agent reply
  answers (from the settled turn), and `conductorctl chat tree` shows each
  request with its tasks, reports, unanswered recipients (blocked, unknown,
  failed, working) and the coordinator summary. Checked on the live real-model
  room; board rendering of the tree remains open.
