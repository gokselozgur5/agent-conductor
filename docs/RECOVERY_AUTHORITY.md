# Independent runtime generation reservations

The daemon retains its boot-generation high-water mark outside the directory
that operator backup/restore replaces. An older SQLite snapshot therefore cannot
reissue a generation that a newer daemon reserved, including a reservation whose
startup crashed before the database committed.

For canonical database `/parent/state/state.sqlite3`, the private authority
directory is `/parent/.conductor-authority-<sha256-of-canonical-database-path>`.
It contains a permanent `ownership.lock` and an atomically replaced
`authority.json`. Do not delete, relocate or roll this directory back alongside
state. It is independent recovery infrastructure, not a disposable cache.

## Startup ordering

1. Hold the existing runtime lock, then the independent authority lock for the
   daemon's entire lifetime. Changing the socket or runtime-lock argument cannot
   bypass the authority lock for the same canonical database.
2. Open/migrate the database and read its authority binding and generation.
3. Reserve `max(database_generation, external_high_water) + 1`. Publish the private
   record through a flushed temporary file and flush its directory and parent.
4. In one SQLite transaction, bind the authority identity, require that exact
   reservation to exceed the database generation, and perform ordinary boot
   recovery. Only then expose the socket, board, watcher or dispatcher.

Migration 17 records the immutable authority identity. A legacy database with no
binding is adopted on its first successful reserved startup. Once bound, the
ordinary unreserved Store startup API refuses it. A different binding, missing
record/lock, invalid permissions, links, oversized/corrupt records and generation
overflow refuse startup. A known authority with a missing database never creates
a new empty database in its place. An interrupted first initialization with no
complete authority record also refuses; it is not silently reset.

An unused reservation is deliberately skipped. Failure to commit SQLite does not
roll the independent record backward. SQLite rejection rolls back the binding
and all boot-recovery changes together. Losing the response does not authorize
reusing the number.

## Operator integration and compatibility

Cold backup, restore and publication repair also hold this independent lock when
the database is bound. New snapshots record its identity, stable directory/lock
identities and original database path. Verification allows the high-water mark
to advance; it does not freeze the record's changing bytes. The record is not
bundled inside the state snapshot. A retained/copied bound database cannot start
at another path with a freshly invented authority.

Migration 17 is a semantic compatibility boundary. Older binaries must not open
an upgraded database. Keep a consistent pre-upgrade backup and its matching
binary/tools; after upgrade, create a new snapshot with the matching tools.
This does not make migration 16 snapshots directly restorable by migration 17
tools, whose schema verification remains exact.

## What remains for disaster recovery

This record establishes generation history and exclusive daemon admission. It
does **not** establish that a writer surviving a crashed daemon has stopped.
No missing/corrupt-state restore or force flag is enabled by this change.
Full recovery still needs a separately proven process ownership boundary, such
as verified host restart with startup exclusion, plus a retained valid authority
record. Cross-host relocation, simultaneous loss of state and authority, cloned
host identities and physical power-loss certification are not covered.

The trusted local operator owns these files. Hashes and identifiers do not defend
against that operator maliciously rewriting both database and authority history.

Validation includes durable unused reservations, a rolled-back database counter,
exclusive ownership, binding mismatch, lost record/database, private-file/link
refusals, integer limits and atomic rejection in SQLite. Real CLI integration
reopens the actual daemon after lowering only the synthetic database counter and
observes a generation above the retained high-water mark. It also checks backup
and startup refusal while the authority lock is held, direct daemon refusal for
a retained bound database, and both interrupted-restore repair outcomes.
