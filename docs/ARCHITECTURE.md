# Architecture Overview

The production Phase 1 design is specified in `DESIGN.md`; security and failure
assumptions are specified in `THREAT_MODEL.md`. This page is the short map.

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

## Project intake and Kanban

An operator registers a canonical project directory. The coordinator discovers the applicable instruction chain, a small set of conventional continuity documents, read-only Git status, and project tasks. That bounded packet is the adapter's resume input.

The Kanban board is a projection of authoritative task states, not a separately edited copy. Claiming, running, verifying, rejecting, completing, blocking, and cancelling a task move the same record between columns.

## Recovery

Workers hold expiring, epoch-fenced leases. Heartbeats renew leases only for
active tasks owned by that worker. Expiry alone does not prove a worker is dead:
the reconciler preserves dirty state and refuses reassignment until the previous
write surface is fenced or explicitly blocked.

## Events and hooks

Hooks publish facts such as `file.updated` or `git.commit.created`; they never launch all workers directly. Subscriptions filter by topic and optional path glob. An idempotency key makes repeated watcher notifications harmless. Source echo is disabled by default and event generations are bounded.

The first watcher is deliberately polling and one-shot. It hashes stable file contents, stores its snapshot atomically, ignores runtime directories, and publishes idempotent create/update/delete events. A later daemon layer will debounce and coalesce rapid scans before it requests an adapter wake-up.

## Trust boundary

Completion evidence is not authority for merge, publication, payment, deletion,
credential use, or any other external effect. Those actions retain explicit
operator gates and must be prevented through adapter capabilities, not prose.
