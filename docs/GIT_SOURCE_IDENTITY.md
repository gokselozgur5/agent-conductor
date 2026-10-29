# Task Git source identity

A newly prepared workflow that reads its initial input from a Git project freezes
that project's canonical root, directory device/inode, resolved HEAD commit and
symbolic HEAD reference. Detached HEAD is represented explicitly. The identity
is part of the attempt packet hash; repository prose and hook payloads cannot
replace it. Preparation refuses a commit or reference that no longer matches
the captured starting snapshot.

Maintenance and the existing terminal/verdict validation paths compare the
frozen identity with a fresh coordinator-side observation. A changed or
unavailable source blocks the current task with `git_source_changed`. The check
validates the task, attempt, lease epoch and persisted packet hash. An obsolete
attempt cannot block a newer owner. Repeated maintenance leaves an already
blocked task unchanged. Dirty files alone do not change this Git identity and
are neither reset nor deleted by invalidation.

A continuation preserves its previous packet's binding and refuses a changed
source before preparing a successor. Inputs copied from an accepted predecessor
snapshot have no inferred live Git binding. Legacy packets without the optional
field retain their original serialization and do not acquire an invented source
identity. This mechanism does not retrofit missing bindings into old work.

Blocking invalidates task progress; it does not establish a write fence around
an unrestricted worker. Old executions, their files and any unresolved ownership
remain subject to the existing reconciliation rules. Restoring a branch does not
automatically resume a blocked task or authorize a conflicting retry.

Observation is repeated, not an atomic filesystem lock. It cannot prove that a
repository never changed between observations, and it does not freeze arbitrary
remote account filesystems. The bound source is the coordinator's input Git
project; the account worker still operates in its separately captured workspace.
