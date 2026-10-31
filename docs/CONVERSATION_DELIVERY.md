# Operator conversation delivery

Status: a current-account manual chat preview is installed at localhost:8801.
It is a locally built debug preview, not a completed command center. Real Codex
and Claude replies work; foreign-account conversation transport remains open.

## Required command and communication room

The operator clarified the product requirement on 2026-10-30: one conversation
must include all agents, with a command hierarchy and a separate communication
chain. Selecting an agent manually for each turn does not satisfy this requirement.

- The operator is the top-level authority. Conductor coordinates assignments,
  delegation and reporting using configured agent roles; vendor names do not
  imply a fixed hierarchy.
- One shared conversation shows operator requests, agent replies, assignments,
  handoffs and coordination updates with their actual authors and recipients.
- The operator can address an individual agent or all participants within that
  conversation. Sending a normal request must not require selecting each worker.
- The communication center routes relevant decisions, results, blockers and
  dependency changes to affected agents automatically. Informing an agent is
  distinct from assigning it work or changing command authority.
- Persist message causality, recipients, delivery state and acknowledgements.
  Reconnection must not duplicate assignments or lose pending notifications.
- Agents can report to their assigned coordinator and exchange relevant context
  across reporting lines. The operator can inspect why a message was routed.
- Do not blindly broadcast every event or create reply loops. A notification
  alone must not trigger unlimited native invocations.

Acceptance requires a single operator request to produce coordinated native
agent work, an automatically routed result or blocker that changes the relevant
agent's context, and visible responses in the same room without manual relay.
Direct and room-wide addressing, restart delivery, duplicate prevention and
unchanged task acceptance authority also require verification. These behaviors
are requirements, not implemented capabilities of the current manual preview.

## Implementation boundaries

- Persist conversations and immutable, idempotent messages in the authoritative
  SQLite database. Conversation text does not change task completion authority.
- Authenticate browser message submission with the existing operator boundary.
  Browser callers cannot publish an agent-authored reply.
- Bind each conversation to its selected project and configured agent/account.
  Use the accepted native account execution path, including its own HOME and
  unrestricted CLI profile. Do not substitute a restricted adapter.
- Persist a turn's execution identity before launching. A lost connection or
  coordinator restart must not silently launch the same native turn twice.
- Publish actual parsed vendor replies with their execution identity. Distinguish
  queued, running, replied, failed and outcome-unknown turns. A reply alone never
  means that a coordinated task passed independent acceptance.
- Show agent handoffs and resulting task identities in the same conversation.
  Keep task checks and independent review when requests become managed work.
- Render text as text, paginate history, and keep authentication tokens out of
  transcripts, URLs and logs. Never fabricate replies or successful executions.

## Delivery evidence required

1. Browser submission produces a real Codex reply using the installed account.
2. A subsequent message retains context; a Claude handoff receives the relevant
   context and returns its own real reply.
3. Browser refresh and daemon restart preserve conversation and execution
   identity. Replayed submission creates one message and one native invocation.
4. Unknown native outcomes remain visible without an automatic duplicate launch.
5. Invalid credentials, forged agent replies and cross-conversation pagination
   are rejected or isolated; displayed model text cannot execute browser markup.
6. Existing native workflow queue, independent acceptance, account ownership and
   event chaining remain functional.
7. Atomic validated commits, issue/PR metadata, exact-source CI and installed
   artifact verification precede a claim that localhost:8801 includes working chat.

Implemented infrastructure includes message persistence, authenticated operator
RPC, shared native reply parsing, and a one-shot launch reservation with atomic
reply publication. Changes are split into storage, runtime, service, protocol,
daemon, UI and launcher commits. Track full delivery in issue #216.

A service runner now binds recorded operator messages to the current account's
full-capability CLI, carries prior conversation context, and publishes parsed
native replies. Its controlled-process regression passed, including retained
context, replay after restart, and failed-turn non-retry. Durable queue admission,
configured agent selection and daemon supervision are now implemented candidates;
their controlled native integration regression passed. The daemon accepts a bounded JSON array
of operator-selected profiles through `--conversation-agents` with `--dispatch`.
The browser composer now supports project creation, configured agent selection,
message submission with stable retry identity, paginated server history and
execution status. Six DOM tests pass. Rendered Chrome acceptance passed with
controlled CLI fixtures and with the operator's real Codex and Claude: the second
agent doubled a randomly selected integer from the first agent's reply, history
survived reload, and no duplicate launches or browser credential storage were
observed. The tested daemon SHA-256 is
`6469cbd62b35a2d80515cf3040fe5bd9bf03b165ae2687959bc972084fd696c8`.
These are same-account observations, not foreign-account or release acceptance.
The subsequent live preview check verified native replies and credential
boundaries, but found horizontal overflow in the populated workflow queue.

## Operator startup

Pass a private profile file using `conductor run --dispatch --conversation-agents
/absolute/path/to/agents.json`, or set the `conversation_agents` path in the
private operator configuration. The launcher validates private file ownership
before bootstrap. Without `--dispatch`, configured conversation execution is
refused. Profiles select the agent ID, OS account, accepted vendor pin, PATH and
timeout; browser requests cannot replace them with arbitrary commands.

After connecting to the board, choose an existing conversation or enter an
absolute project folder and select **New conversation**. Choose the agent and
send a message. Switching agents within the same conversation passes prior
messages to the selected agent. Unknown execution outcomes remain retained and
are never automatically retried.

The same room runs from the terminal over the daemon's local socket:

```
conductorctl --socket <sock> chat new --project /abs/dir --lead lead   # prints the ID
conductorctl --socket <sock> chat send <id> "task for the coordinator"
conductorctl --socket <sock> chat send <id> --to worker "..."          # or --all
conductorctl --socket <sock> chat log <id> --follow
conductorctl --socket <sock> chat tree <id>                           # assignment tree
```

`chat new` refuses a project folder that does not exist. `chat log` prints each
message as `author → recipient (state)`, shows an authorized delegation as its
tasks and a refused one with its reason; `--json` prints the raw messages.

Native conversation replies do not prove that unrestricted descendants stopped
writing. Cold backup therefore refuses state containing native conversation turn
records, including replied turns, until a separate verified recovery procedure
exists. This preserves the existing full-capability recovery boundary; it is not
an automatic reconciliation mechanism.

## Shared-room candidate (not installed)

The candidate persists operator-configured membership, reporting relationships,
recipient snapshots and pending information deliveries. The browser can configure
a room with a selected coordinator and all configured agents, address the
coordinator, an individual member or everyone, and reload the stored topology.
Retries preserve message identity, recipients and configuration. The topology is
immutable after creation; editing an existing room is not implemented.

The dispatcher recovers unadmitted assignments after restart, reserves one native
turn per message and recipient, and automatically routes recorded replies to
supervisors and subscribers. Relevant information enters a recipient's next turn
without launching a reply loop. An uncertain native outcome still blocks further
launches in that room. Routed requests report queued before job admission.

A rendered Chrome fixture passed with one operator request, two actual controlled
child processes, automatic prior-result context, reload, text-only rendering,
authentication and credential checks. This is not a real-model shared-room proof.
Service regressions cover restart delivery, immutable topology and no duplicate
launches; migration tests preserve schema-35 launch identities and unknown-state
exclusion. Deterministic cancellation verifies atomic route/inbox rollback.

The candidate now delegates without operator relay: a coordinator reply that is
exactly one strict `conductor.delegation.v1` JSON object is examined once,
authorized against the stored topology and ancestry, and each named recipient
is admitted for one native turn carrying the stored instruction. Any other reply
is ordinary information for the operator and supervisor; a rejected decision is
recorded with its reason and launches nothing. A recipient without a configured
profile is a visible blocked job. This is proven with controlled processes only.

When every delegated recipient has settled, the delegating agent receives one
continuation turn with the reports and writes a consolidated response. It may
instead correct the work with a new delegation, at most twice per batch chain;
a third is recorded as rejected. Controlled processes prove these rounds.

Remaining: visible per-recipient delivery reasons/status and examination outcomes
in the browser, real-model shared-room acceptance, foreign-account conversation
transport, complete delivery checks and installation.
