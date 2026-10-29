# Sampled native acceptance guard

`tools/native_observation_guard.py` provides `NativeObservationGuard` for isolated
acceptance harnesses. Construct it with the existing SQLite database, a native
host/boot identity read through the trusted daemon, and explicitly selected
immutable source roots. Call `sample()` with a fresh daemon `check-invariants`
response throughout a real workflow. The caller owns the loop, cadence, evidence
publication and cleanup; this module never starts or stops a process.

The guard checks duplicate active/recovery-pending leases, daemon invariant
health, stable daemon generation/authority, bounded process inventory, persisted
process identity contradictions and selected protected tree hashes. Missing or
racy process observations remain explicitly unhealthy during a bounded grace
window; persistent uncertainty refuses the run. Final acceptance requires a
healthy sample without pending contradictions or uncertainty. Tagged observations without matching
durable identity, and recorded identities with missing/contradictory tags, must
clear within the configured grace period. Process disappearance or PID reuse for
a historical record is not itself evidence of an orphan. No PID is signaled and
no lifecycle mutation or recovery permission is issued.

Each selected tree is limited to 4,096 entries and 64 MiB per observation, with a
five-second deadline. Symlinks and special files refuse. Names, regular-file bytes
and permission modes contribute to the digest; file contents are not reported.
All selected roots must remain immutable, including Git metadata if present.
Observation gaps, clock/suspend gaps and samples exceeding the cadence budget
refuse. Any exception permanently latches failure in that guard instance. A later
clean sample cannot erase a failed run.

Acceptance includes a real Claude worker, executed checks and independent Codex
verification while the guard samples live tagged and recorded processes and the
unchanged original project. Separate negative controls plant an actual tagged
wrapper with no execution record, then change a protected file from another
process. Both stop acceptance and latch failure. Synthetic database/invariant
inputs in these negative controls are distinct from the actual workflow proof.

This is a sampled detector, not continuous kernel enforcement. Writes restored
between observations can be missed. Hash equality does not prove a surface is
unwritable, path observations are not pinned filesystem handles, and PID markers
are not process handles. It does not enumerate untagged descendants without
stored identities, inject stale mutations, cover
every lifecycle/effect crash, or certify seven days of native operation. Every
sample explicitly keeps `full_v1_accepted` false. Existing control-plane endurance
runs remain separate and must not be restarted or relabeled as native runs.


## Stable board projection observations

Pass `board_reader(after)` to the guard to compare every page of the operator
`/api/tasks` response with durable task aggregates. The callback must bound its
transport time and response bytes. The guard limits rows to 10,000, aggregate
bytes to 64 MiB, each stored aggregate to 1 MiB, and observation time to five
seconds. Duplicate tasks, missing/cyclic cursors and malformed pages refuse.

SQLite `data_version` is read before the database snapshot and after all API
pages on the same connection. Any intervening commit makes the observation
`concurrent_change`, even if values were changed and restored. This is unhealthy
uncertainty under the existing finite grace window; it cannot count as final
acceptance. With no intervening commit, any aggregate or membership difference
fails and latches the run. An omitted reader is explicitly `not_checked`, never
an implicit claim that board consistency was verified.

Controls cover multiple pages, missing/duplicate/modified rows, concurrent
commits with restored values, oversized stored data and invalid cursors. Native
acceptance uses authenticated local HTTP pages during an actual workflow;
page transport uses a three-second timeout and 4 MiB response bound. This proves
sampled API/database correspondence, not browser rendering or atomic snapshots
across arbitrary continuous writers.

## Protected-tree digest framing

Tree digest version 2 domain-separates the format, includes root permission bits,
and encodes each relative path with its byte length, an explicit file/directory
type, fixed-width permission bits and a separate fixed-width SHA-256 digest for
regular-file contents. File bytes cannot impersonate another entry's metadata.
An empty file and an empty directory remain distinct even with identical modes.

Regression controls reproduce two collisions in the earlier unframed byte
stream: crafted file content versus an additional file, and empty file versus
empty directory. Both must differ under version 2. This is a correction to the
acceptance guard, not the daemon or filesystem fencing. Earlier sampled native
runs retain their original evidence and digest implementation; do not relabel
them as version 2 runs. Future runs establish fresh baselines with pinned guard
source bytes.
