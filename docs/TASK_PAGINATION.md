# Bounded operator task pages

The full-list board route failed all20reads with HTTP413 at the required synthetic
scale of100projects,10,000tasks and100,000semantic events. Failed-response latency
is not a performance success. The fixture contains no active workers and enables
neither dispatch nor filesystem observation.

`GET /api/tasks` now uses `ListTaskPage`: at most100tasks plus one internal
sentinel, ordered by immutable task ID. `next_after` is an exclusive cursor;
`after` and optional `project_id` are query parameters. A new ID before the cursor
appears after returning to the first page, not by shifting later pages. This is
live keyset pagination, not a frozen database snapshot. Priority remains visible
but does not determine page order. Background refresh retains the current page.

The board provides **First page** and **Next page**. Task versions and controls
come from the displayed page; other tasks remain accessible by navigating pages.
Disconnect resets the cursor. History selection remains separate.

```sh
./tools/conductor ctl --json list-task-page
./tools/conductor ctl --json list-task-page --after LAST_ID --project-id PROJECT
```

The legacy `list-tasks` socket command retains its full-list contract and transport
size limits. This change does not claim arbitrary-sized task bodies fit the1MiB
HTTP boundary; pages retain that refusal. Task IDs are data, not integer offsets.
No schema mutation or lifecycle action is needed to read pages.

## Evidence

The service regression traverses206existing tasks in100/100/6pages while a new
lexically earlier task is inserted. Existing IDs remain unique/ordered, the new
ID appears on first-page refresh, project filtering works, and task state stays
unchanged.187workspace tests pass. Strict Clippy passed after method-reference
and documentation-markup corrections; formatting/build/JavaScript syntax pass.

The same100/10,000/100,000 fixture now returned200for20/20reads, with measured
successful-request p95 of4.75ms on the development Mac. Complete traversal found
all10,000unique IDs; project filtering returned the expected100tasks. This measures
API reads for simple synthetic tasks without active workers/watchers, not browser
render latency, idle CPU, wake latency or arbitrary large task payloads. The
original20/20HTTP413failure is retained alongside the successful result.
Private evidence: `reviews/ac-board-load-v1/receipt.json` and
`reviews/ac-board-load-v2/receipt.json`.

An isolated real daemon/CLI/Chrome run passed100/6/first-page navigation while
preserving task-history paging, authentication, text rendering and delayed-response
disconnect behavior. Receipt: `reviews/agent-conductor-runtime-20261028/task-pagination-browser-v1/receipt.json`.
No model calls or user-runtime database changes were used in these probes.
