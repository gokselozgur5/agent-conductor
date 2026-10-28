# Operator inventory

The board's **Projects, agents and events** section reads the same versioned
service as the CLI. Select Projects, Agents, Events, Subscriptions, Deliveries or Reported usage.
Refresh returns to the first page; Next page advances in ascending ID order.
These are persisted metadata snapshots, not live process or ownership verdicts.
Inventory refresh is explicit after the initial connection; task polling remains
independent.

```sh
./tools/conductor ctl --json inventory projects
./tools/conductor ctl --json inventory agents
./tools/conductor ctl --json inventory events
./tools/conductor ctl --json inventory subscriptions
./tools/conductor ctl --json inventory deliveries --after EVENT_ID:SUBSCRIPTION_ID
```

Use the exact `next_after` returned by the preceding response. Each response
contains `type: inventory`, `resource`, up to 100 `entries`, and nullable
`next_after`. The authenticated HTTP equivalent is
`GET /api/inventory?resource=events&after=CURSOR` with URL-encoded parameters.
Unknown resource names, extra HTTP fields, empty/NUL/over-4096-byte cursors are
refused. The existing loopback/Host/Origin/authentication and response-size
boundaries apply. There is no operator write operation. Reported usage adds migration 16 for its partial audit index; see [REPORTED_USAGE.md](REPORTED_USAGE.md).

## Fields and interpretation

- Projects expose registered IDs, canonical paths and registration times.
- Agents expose registered adapter/access mode and protocol/capability identities.
  Registration alone does not establish that an adapter is currently running.
- Events expose source, correlation, causation, generation, topic, project/task/
  attempt/lease references, path and creation/expiry times. Event payloads and
  source-session/idempotency fields are omitted.
- Subscriptions expose agent, topic/project/path filters, echo and active flags.
- Deliveries expose event/subscription/target IDs, persisted status, retry count,
  availability/lease/ACK times and stale reason. Claim tokens and consumer state
  are omitted. A recorded `leased` status can outlive its wall-clock deadline;
  only the control service decides eligibility or authorizes acknowledgement.

IDs and other stored text are rendered with text nodes. Disconnect clears the
inventory, and a late response cannot restore it or overwrite a different
selected resource. Empty collections are displayed as empty pages.

Pages are keyset reads, not a transaction spanning the entire traversal. New IDs
that sort before the current cursor require a first-page refresh. This is not a
chronological event feed, total-count query, search interface, or retention policy.
Rows are bounded in count; unusually large metadata can still hit the existing
transport byte limit. Payload omission is not general redaction of user-supplied
identifiers/paths. Task lease observations and reported usage are documented separately; deeper
execution evidence remains product work.

## Validation

Service tests traverse 105 agent rows without duplicate/omitted IDs and reject
invalid cursors/resources. Another test verifies event causality and leased
delivery metadata while excluding payloads and claim tokens. A real release
CLI/HTTP/Chrome fixture traverses 105 agents, events and deliveries plus projects
and subscriptions; checks API parity/authentication/invalid queries; renders
markup literally; and discards late responses on resource change or disconnect.
No model invocation is needed for this read-only feature. Evidence is retained
under `reviews/agent-conductor-runtime-20261028/inventory-*` in the operator
workspace.

Final local validation passes 247 workspace tests, strict Clippy, formatting,
release build and credential checks. The final release Chrome probe repeats the
acceptance after the last CLI help change and binds its binary hashes in
`inventory-browser-v2/receipt.json`.

Reported usage is the sixth collection; its invocation identity, missing-data and
vendor-claim boundaries are documented in [REPORTED_USAGE.md](REPORTED_USAGE.md).
