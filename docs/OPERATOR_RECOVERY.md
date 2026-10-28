# Same-path recovery with retained current state

This workflow restores a verified snapshot over an **existing, stopped,
quiescent** operator state. It retains current data separately,
rotates the board token and does not launch a daemon or dispatch a task.

```sh
python3 tools/conductor verify-backup /absolute/backups/snapshot --external
python3 tools/conductor --state /absolute/operator/state restore /absolute/backups/snapshot --retain-current /absolute/operator/state-before-restore
python3 tools/conductor --state /absolute/operator/state run
```

Inspect the restored tasks before choosing `run --dispatch`. Restoration is an
intentional return to the snapshot's history: later task changes remain in the
retained archive and are not automatically merged into the restored database.
Use the token command to obtain the new board login token. Old browser sessions
must authenticate again. No vendor credentials are created or restored.

## Preconditions and identity

New snapshots use manifest version 2. They bind the current Conductor executable
bytes, registered project/Git common-directory device/inode identities and the
executable/instruction pins and toolchain directories required by queued or
nonterminal native plans. Completed historical plans do not require obsolete
vendor installations just to retain their results. External files are recorded,
not bundled. Project contents and toolchain contents are not fully backed up by
these identity records.

`verify-backup` checks the payload without reading external prerequisites.
`verify-backup --external` additionally verifies the recorded dependencies and
rederives the complete required inventory from the saved configuration/database;
a missing manifest entry cannot silently bypass that check. Version 1 snapshots
remain verifiable but cannot be used by the restore command.

Restore requires the exact original canonical state root and a new sibling path
for retained current state. Both snapshot and current database must have supported
migration checksums, valid integrity, no active/recovery lease, no unresolved
effect, no active task/running dispatch, and verified fences for all executions.
Missing or changed prerequisites, replaced project identities, relocation and
occupied runtime locks are refused. Projects nested inside state are unsupported:
copying them would break registered filesystem identity.

## Publication and recovery evidence

The payload is copied and checked in a private sibling staging directory. Required
executables excluded from the snapshot but located under the state root are
preserved from current state only after fingerprint checks. The restored runtime
generation is raised to the maximum of the snapshot and current generations;
normal daemon startup then advances it. Task/attempt epochs and lifecycle rows
are not rewritten, and no live lease is revived.

The existing runtime lock inode is linked into the staged directory and remains
held during the directory switch. Current state is marked as an archive before
moving, so the launcher will not start it as another dispatcher. A failed publish
attempt restores the original directory when possible and releases its temporary
lock link. The retained directory preserves later work. A private restoration
receipt records snapshot identity, generation floor, token rotation and retained
path. File contents and directory entries are flushed before publication.

## Remaining disaster-recovery limits

Issue #59 remains open. The command refuses a missing, corrupt or unprovably
quiescent current database because it cannot establish the absence of orphan
writers or the highest prior boot generation from an old snapshot alone. There
is no force option that invents fencing evidence. Full state-loss recovery needs
an independently established ownership boundary and generation history.

A process or power failure between directory renames can leave an archive and
staging directory with the original path absent. Keep both directories and their
receipts; do not launch the archive or rewrite packet paths. A failure after publication explicitly reports that state was already restored;
inspect the restoration receipt and retained archive before retrying. Automated repair of
that interrupted publication and full-host/state-loss recovery are not yet
accepted. This command is not a zero-downtime backup, arbitrary migration downgrade
or cross-host migration tool.

Unit controls cover the continuous lock, retained bytes, generation floor, token
rotation, failed publication rollback, legacy/missing-state refusal, external
fingerprint and project-identity changes, missing dependency entries and retained
state-local executables. Real native fixture acceptance and its limitations are
recorded in the private restore validation manifest.

## Native acceptance evidence

A previously completed Claude/checks/Codex fixture was restored without changing
its durable task records or evidence bytes. A separate real writer was interrupted
after producing output, blocked and fenced, backed up, restored and explicitly
continued. It completed under a new attempt with checks and independent Codex
acceptance; the retained pre-restore bytes remained unchanged. Final source also
reopened a fresh snapshot of that completed result and passed invariants.

An earlier fixture required a particular sequence of editing operations that its
final files could not establish. The reviewer rejected it; that rejection is
retained, not relabelled as a recovery success. A source project nested inside the
state root was also refused because copying would change its registered inode.
The accepted fixture uses a separate original project and an inspectable final
file requirement. Private receipts: `restore-operator-native-v4-receipt.json`,
`restore-final-receipt.json` and `restore-operator-validation-manifest.json` under
`reviews/agent-conductor-runtime-20261028/`.

The real operator integration test starts and stops the actual Rust daemon,
refuses backup while it owns the lock, restores with a missing current config,
advances beyond the pre-restore boot generation, rejects the old HTTP token and
accepts the new token. It runs on both CI hosts without vendor/model calls.
