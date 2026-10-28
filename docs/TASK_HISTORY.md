# Task history

## Requirement and design

Operators need the persisted lifecycle history of a selected task alongside its
current state. Read the existing transactional audit journal, newest first, in
pages of 100. An exclusive sequence cursor makes older-page traversal stable
while new events arrive; refresh explicitly returns to the newest page.

The store owns the indexed read, the service validates cursors, the protocol
carries a metadata projection, and the authenticated board renders plain text.
Do not expose audit payloads: they can contain full commands, instructions or
checkpoint content. This view shows sequence, kind, actor, correlation and time;
it is not a semantic-event inbox, a log viewer or a complete secret-redaction
system. Identifiers are operator-controlled metadata. Reads never claim events,
change leases or launch work. No schema migration is needed.

Disconnect clears history and invalidates outstanding requests. Background task
refresh does not replace an explicitly selected history page. History errors are
shown locally rather than representing an empty page as a successful read.

## Verification

The SQLite/service/HTTP regression traverses 206 task entries across three pages,
with nonmonotonic display timestamps and an intervening append. It checks strict
sequence order, task isolation, omitted payload content, unauthenticated refusal,
invalid/overflow cursor rejection, missing-task refusal and unchanged task state.
The full workspace has 186 passing tests; strict Clippy and formatting pass.

This is lifecycle audit metadata, not a claim that all remaining operator
requirements or the historical supervision timeout have been resolved.

An isolated real-daemon/CLI/headless-Chrome probe passed first/older/newest page
navigation, HTML-like actor text without element injection, payload omission,
token/storage boundaries, and disconnect during a delayed history response.
Private receipt: `reviews/agent-conductor-runtime-20261028/task-history-browser-v1/receipt.json`.
No model call or live operator database mutation was used for this acceptance.
