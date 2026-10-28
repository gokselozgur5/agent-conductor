# Cold operator-state snapshots

`tools/conductor backup DESTINATION` creates a private, verified state snapshot.
Stop the operator runtime first, after pausing active work and confirming write
fences. The command does not stop workers or grant a fence: it refuses an occupied
runtime lock, any execution without a persisted fence, active tasks or interrupted
running dispatch. Keep the source state intact if it refuses; reconcile using the
existing lifecycle controls first.

```sh
python3 tools/conductor --state /absolute/operator/state backup /absolute/backups/new-snapshot
python3 tools/conductor verify-backup /absolute/backups/new-snapshot
```

The destination must not exist, must be outside the state directory, and must
have a canonical existing parent. The source must be private and operator-owned.
The snapshot copies regular owned state files and directories, including private
configuration, board login token, plans and local evidence artifacts. Symlinks,
hard links, special files and special permission bits are refused. Attempt work
and plan artifact paths outside this state root are refused. Bounds are 100,000
entries and 10 GiB; paths and modes are checked again after copying.

SQLite's backup API exports a consistent database. The exported database is
sealed in DELETE journal mode without WAL/SHM dependencies. Integrity and all
migration checksums must match this checkout. A private manifest binds copied
bytes, sizes, modes, original state root and migrations; it is published last.
Failed exports remain as inspectable partial directories without a published
manifest. Existing destinations are never overwritten.

`verify-backup` works even when the original state directory is absent. It checks
the private manifest, full payload inventory, hashes, modes, schema checksums,
integrity and conservative quiescence conditions. It does not launch a daemon,
dispatch a task, change authority or read the original project.

## Recovery limits

This is a cold state snapshot. Existing, stopped state can be restored with the
[same-path recovery command](OPERATOR_RECOVERY.md); full state-loss recovery is
not yet accepted and issue #59 remains open.
Binary directories, lock/socket files and database sidecars are excluded. External
project checkouts, accepted vendor executables, approved instruction files and
credentials outside state need their own recovery procedure. A successful hash
check proves snapshot consistency, not authenticity against an attacker who can
rewrite both payload and manifest, or availability of those external resources.

Database packets retain absolute paths and identities. Do not relocate the state,
rewrite packet paths or start a second dispatcher from a copied database. New
version 2 snapshots record external prerequisites for explicit `--external`
verification; these records do not include external project/toolchain contents.
Migration 17 snapshots also bind the independent recovery authority's stable
identity and lock. Its high-water mark may advance and is not copied into the
snapshot. Retain that external directory; see
[RECOVERY_AUTHORITY.md](RECOVERY_AUTHORITY.md). Snapshot tools require their exact
supported schema, so preserve matching tools for pre-upgrade snapshots.
Recorded interrupted publications can now be repaired through `repair-restore`;
missing/corrupt-state recovery has a separate
[prepared host-restart workflow](DISASTER_RECOVERY.md), requiring authority
format 2 and a fresh compatible snapshot. Actual host-restart acceptance remains
open under issue #59.

Validation covers state preservation, WAL export, manifest verification,
tampering, live-lock refusal, active/unfenced work, unsupported schema, links,
external artifact roots, nested destination and overwrite refusal. An isolated
real CLI/daemon probe passed live refusal, stopped backup and verification. It
made no model calls and did not stop the user's running operator runtime.
