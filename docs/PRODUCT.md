# Product: leaders and soldiers in one chat

Status: operator-approved direction, 2026-10-31. This supersedes the finish
definition in [ROADMAP_TO_FINISH.md](archive/ROADMAP_TO_FINISH.md) where they differ.

## What the operator uses

One chat window in the terminal. The operator types; two leaders answer in the
same stream. `@fable` or `@astra` addresses one leader, an unaddressed message
goes to the default leader. `/tasks` shows the work tree at any time. Long work
continues in the background; the operator can ask for status or redirect it.

## Roles

```
                 operator
          ┌─────────┴─────────┐
   Fable (Claude lead)   Astra (Codex lead)    read, plan, delegate, review
          │                   │
   Opus / Sonnet         Codex developer       write code
   Haiku                 Codex small model     mechanical work
          └──── cross-vendor reviewer ────┘    Claude work reviewed by Codex and vice versa
```

- **Leaders never write.** They may read files, diffs and check results so they
  can judge work, but they cannot edit, commit, push or run mutating commands.
  Their context stays on decisions, so the expensive model is spent on decisions.
- **Every task has one owning leader.** The other leader reviews it. Two leaders
  never co-own a task.
- **Soldiers are chosen by the kind of work:** Opus or Sonnet (Codex developer)
  for software development, Haiku (Codex small model) for mechanical work such as
  bulk renames, formatting, log scanning, simple edits and summarizing test runs.
  Creating or deleting a file is a tool call, not a model task.
- **Nobody accepts their own work.** "Done" is decided by a reviewer soldier of
  the other vendor, never by the soldier that did it or by reading its report.

## Work organization

Every piece of work is a durable task under its owning leader: ordered queue,
state (queued, running, review, done, blocked with reason), parent task, owner,
model and result. Tasks may run for hours across many turns; a turn is not the
unit of work. Restart never duplicates or loses a task.

## Defaults pending operator confirmation

- Astra is the OpenAI model used through Codex.
- One room works on one project; the operator can open several rooms.
- Push, merge and deleting files outside the task's own scope need the
  operator's approval in the chat.

## Out of scope for now

Separate OS accounts per agent, the seven-day soak, load targets, release-kit
acceptance and browser acceptance from the previous roadmap. They return only
if the operator asks for them.
