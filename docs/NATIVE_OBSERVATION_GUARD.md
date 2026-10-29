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
stored identities, compare every board projection, inject stale mutations, cover
every lifecycle/effect crash, or certify seven days of native operation. Every
sample explicitly keeps `full_v1_accepted` false. Existing control-plane endurance
runs remain separate and must not be restarted or relabeled as native runs.
