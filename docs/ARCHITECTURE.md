# Architecture

## Control-plane boundary

Agent Conductor owns durable coordination, not project implementation. Vendor adapters translate lifecycle commands and reports; they do not define lifecycle truth.

```text
operator
   |
   v
coordinator --- SQLite event log/state
   |                 |
   |                 +--- reconciler
   |                 +--- verifier gates
   |                 +--- wake deliveries
   |
   +--- Codex adapter
   +--- Claude adapter
   +--- remote/Thor adapter
```

## Task completion

A task reaches `completed` only when a non-owner verifier records every required check as true. Rejection returns the task to `queued` while retaining its evidence and checkpoint for diagnosis and continuation.

## Recovery

Workers hold expiring leases. Heartbeats renew leases only for active tasks owned by that worker. The reconciler re-queues expired work, clears ownership, marks the missing agent offline, and preserves checkpoint/evidence fields.

## Events and hooks

Hooks publish facts such as `file.updated` or `git.commit.created`; they never launch all workers directly. Subscriptions filter by topic and optional path glob. An idempotency key makes repeated watcher notifications harmless. Source echo is disabled by default and event generations are bounded.

The first watcher is deliberately polling and one-shot. It hashes stable file contents, stores its snapshot atomically, ignores runtime directories, and publishes idempotent create/update/delete events. A later daemon layer will debounce and coalesce rapid scans before it requests an adapter wake-up.

## Trust boundary

Completion evidence is not authority for merge, publication, payment, deletion, credential use, or any other external effect. Those actions retain explicit operator gates.
