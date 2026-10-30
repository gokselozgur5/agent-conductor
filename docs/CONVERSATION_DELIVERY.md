# Operator conversation delivery

Status: current-account candidate validated in Chrome with real Codex and Claude;
not installed in the operator's active runtime yet. Foreign-account conversation
transport remains open.

The operator currently has to upload a prepared workflow plan. There is no
conversation entry point on the installed board. Delivery requires entering a
request in the browser, seeing actual native agent replies, retaining context
after refresh/restart, and handing work between configured agents.

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
These are same-account observations, not foreign-account or installed release
acceptance.

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
