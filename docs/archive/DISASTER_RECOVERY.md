# Prepared same-host recovery after a host restart

This operator workflow restores a supported cold snapshot when the current
database is missing or corrupt. It requires the independently retained recovery
authority, the same host and filesystem identities, and all recorded external
prerequisites. It preserves an existing original directory, rotates the board
token and never starts tasks.

**Acceptance boundary:** native identity reads, startup quarantine, same-boot
refusal and actual daemon reopen have been tested. Positive boot transitions in
the automated recovery tests are injected only in the test process. A real
host-restart recovery rehearsal is still pending; issue #59 remains open. Neither
physical power-loss nor full-host/remote-effect recovery is claimed.

## Operator steps

Use a snapshot created after a host-bound daemon startup (authority format 2).
Keep the authority directory outside state. If runtime executables, adapter pins,
instruction files or registered project directories were also lost, restore those
exact external prerequisites first. They are not bundled in the cold snapshot.
Do not relocate projects, rewrite packets or replace recorded filesystem IDs.

Stop the remaining daemon if it is reachable. Preparation cannot take authority
from a running daemon, and it does not infer that orphan writers have stopped.

```sh
python3 tools/conductor verify-backup /absolute/backups/snapshot --external
python3 tools/conductor --state /absolute/operator/state prepare-recovery /absolute/backups/snapshot --retained /absolute/operator/state-before-recovery
```

Preparation verifies the snapshot and authority binding, preserves a hash-pinned
native host reader outside state, records the current host/boot epoch, and
durably quarantines daemon admission. Repeating the same preparation returns its
existing identity; it does not reset the required boot boundary. A different
snapshot or archive path is refused while a request is pending.

Save unrelated work and restart the host through its normal controls. These
commands never restart the machine. A daemon restart, process exit, elapsed wall
time, sleep or hibernation is not accepted as a host restart. Automatic daemon
startup remains blocked while the recovery is prepared.

```sh
python3 tools/conductor --state /absolute/operator/state recover-state
```

Completion requires a different native boot epoch on the same recorded host. It
takes the independent lock, validates the pinned reader/snapshot/prerequisites,
copies and verifies a private candidate, retains any existing original directory,
and publishes the restored tree. The generation floor comes from the retained
authority; normal startup reserves a higher generation. Lost post-snapshot task
history is not reconstructed or merged. Existing unreadable/corrupt database
bytes remain in the retained directory for inspection.

Start without dispatch, obtain the new board token and inspect restored tasks:

```sh
python3 tools/conductor --state /absolute/operator/state run
python3 tools/conductor --state /absolute/operator/state token
```

Reconcile any effects outside the snapshot before explicitly enabling dispatch.
This workflow is for local native process/file ownership. It does not undo or
prove completion of post-snapshot remote effects, fence another host, or certify
network-filesystem writes. Losing both state and the independent authority is
not supported by this recovery path.

Before publication staging, preparation can be cancelled without changing state
or granting any fence:

```sh
python3 tools/conductor --state /absolute/operator/state cancel-recovery
```

After staging, retry `recover-state` instead. Retained archives and abandoned
candidates are preserved, not deleted automatically.

## Publication ownership and crash behavior

The external authority contains the recovery request throughout publication.
The daemon checks it before creating state/socket/lock directories, including
when the original state root is absent. A raw daemon with different socket/lock
arguments cannot bypass quarantine for the same canonical database.

Preparation does not touch the current database. A complete staged candidate is
bound by directory identity and a full hash/mode inventory before either rename.
A crash during copying leaves an abandoned candidate; retry preserves it and
starts a new candidate. A crash between renames resumes using the retained
original and verified candidate. A crash after publication verifies the same
published directory before clearing quarantine. Repeated completed recovery does
not publish again. Unexpected paths, altered candidates, changed prerequisites
and corrupt recovery evidence leave quarantine in place.

The original directory receives an archive guard before moving. An earlier valid
guard is preserved rather than rewritten. Task/attempt identities and paths stay
unchanged; no lease or terminal result is manufactured. The publication receipt
records both boot epochs, snapshot digest, generation floor and token rotation.

## Native identity and compatibility

`conductor-daemon --host-epoch` prints application-specific SHA-256 identifiers
for the host and boot without opening runtime state. There is no production
environment variable, CLI override or force option for the native epoch.

Linux reads `/etc/machine-id` and `/proc/sys/kernel/random/boot_id`. The kernel's
boot ID is specific to its running instance; the machine ID normally persists
across boots. See [systemd boot identity documentation](https://www.freedesktop.org/software/systemd/man/247/sd_id128_get_boot.html)
and [machine-id documentation](https://www.freedesktop.org/software/systemd/man/262/machine-id.html).

macOS reads the platform UUID through the system `ioreg` utility and
`kern.bootsessionuuid` through `sysctl`. Apple's source distinguishes the boot
session from sleep/wake identity and keeps it unchanged through hibernation. See
[Apple's boot-session contract](https://github.com/apple-oss-distributions/xnu/blob/f6217f891ac0bb64f3d375211650a4c1ff8ca1ea/iokit/IOKit/pwr_mgt/IOPM.h#L222)
and [boot-session initialization](https://github.com/apple-oss-distributions/xnu/blob/f6217f891ac0bb64f3d375211650a4c1ff8ca1ea/iokit/Kernel/IOPMrootDomain.cpp#L3847).

Readers reject missing, malformed, nil or oversized identities and command
timeouts. These are trusted OS identities, not remote attestation. Cloned host
identities, machine reinstallation, deliberate modification of private authority
files and filesystem identity changes are not repaired automatically.

Authority format 2 permanently binds the host. The previous format-1 daemon
refuses it, so it cannot ignore quarantine. Pre-upgrade snapshots remain
payload-verifiable but their old authority protocol/binary prerequisites cannot
be used as a format-2 disaster-recovery snapshot. Preserve matching pre-upgrade
tools and take a fresh verified snapshot after upgrading. Never roll the
independent high-water record back to make an old binary start.
