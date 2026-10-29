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

## Unattended observation loop

`run_native_monitor.run_native_monitor` runs an existing guard for an explicit
bounded duration and interval. The caller supplies a fresh invariant reader,
explicit runtime files to pin, and a new private output directory. It owns the
isolated daemon, real workload submission, transport timeouts and cleanup. The
monitor has no task-launch or process-stop authority.

```python
receipt = run_native_monitor(
    guard, read_runtime_invariants, new_output_directory,
    duration_seconds=120, interval_seconds=0.5,
    pinned_files=[daemon_binary, control_binary, runner_binary, workflow_plan],
)
```

The guard must have a board reader. Durations are bounded to seven days, sample
counts to 150,000 and flushed JSONL evidence to 128 MiB. `run.json` records source
and runtime file hashes; every sample is flushed and fsynced, and `status.json`
is atomically replaced with private permissions. Existing output refuses rather
than appending another run. Pin changes, missing native observations, uncertain
terminal samples, reader exceptions and monitor violations fail the final receipt.
Arbitrary exception text is omitted. Interrupted runs remain failed or visibly
unfinished; a stale `running` file alone never establishes process liveness.

A duration below seven days always yields `completed_short_native_probe`, even if
a delayed run lasts longer. A full requested seven-day run can yield only
`completed_sampled_native_soak`, with `full_v1_accepted` still false. Neither status
proves complete fault coverage, arbitrary descendant containment, continuous
workload activity, whole-process-tree resource bounds or revocation of write capability.
Keep the existing control-plane endurance run separate.

## Selected daemon resource limits

Pass a `NativeResourceGuard` as `resources=` to the unattended loop. Its database
must match the observation guard. A requested seven-day run refuses without this
resource guard; short compatibility probes explicitly record null resource limits
when it is absent. Limits are copied into the run receipt and cannot change during
sampling. PID and kernel start marker identify the selected daemon in that receipt.

The resource guard verifies the kernel start marker before and after each sample,
reads bounded native RSS, and caps both absolute RSS and growth from the first
observed baseline. It counts SQLite database/WAL/SHM bytes plus explicitly selected
log files, pending/leased event deliveries, and unfinished workflow dispatches.
Terminal event history is not pending backlog. Query and process-read deadlines,
missing database/process observations, malformed RSS and special files refuse.
Failures latch; the observer never stops or signals the measured process.

Defaults are 512 MiB RSS, 64 MiB RSS growth, 2 GiB selected state/log bytes, 256
pending deliveries and 256 unfinished dispatches. Callers must select limits and
log paths appropriate to the isolated test before starting. Samples record actual
measurements and the baseline. These are selected-daemon/selected-file limits,
not total process-tree memory, all artifact storage, queue age or leak attribution.

Controls include every resource refusal and PID changes. A separate owned Python
process allocates 32 MiB after baseline; the native RSS observation exceeds an
8 MiB growth limit and permanently fails. This demonstrates detection, not a leak
in Conductor. Native workflow acceptance records the same measurements while the
actual daemon dispatches and verifies work.

## Escaped-session writer control

The actual runner is tested with an explicitly controlled fake-adapter launch
whose child starts a new session. After the runner reports `stopped/cancelled`
and exits, that child remains alive and writes to the owned test workspace only
when the test releases a separate gate. The protected-surface guard detects that
post-stop write and fails. The bounded child then settles; no model is invoked.

This is a counterexample to treating process-group cancellation as arbitrary
process-tree fencing. The fake adapter is a test primitive, not native vendor
admission. An independent adapter-boundary regression requires command-capable
Claude and Codex writers to return `VendorError::Capability` before constructing
a launch. Neither the stop event nor this diagnostic grants recovery authority.

The negative guard control uses synthetic durable scope and runtime-health input.
It proves detection for a selected protected workspace, not discovery of every
untagged descendant or containment of arbitrary native commands. The real native
workflow acceptance remains restricted to the admitted file-writer capability.

## Durable verification-source bindings

Call `guard.bind_verification_surface(snapshot_root, source_identity)` only after
reading the source identity from trusted task state and selecting its authorized
snapshot path. The method verifies the exact manifest bytes against the durable
`snapshot:<sha256>` identity before accepting a baseline. It validates the bounded
manifest schema, sorted unique relative paths, declared deletions, file hashes,
sizes and executable bits, and rejects links, escapes and extra files.

Every later sample repeats manifest/content verification and compares the framed
tree digest. A binding cannot be replaced with a new source to excuse a changed
tree. Binding or observation errors permanently latch failure. Up to 16 explicitly
selected snapshots can be watched; source/path discovery remains the harness's
responsibility. An absent binding is visible as zero `verification_surfaces` and
must not be claimed as verification-tree coverage.

The native harness reads the task's persisted verification source, binds the
published `artifacts/source` snapshot, and binds `review-input/source` only after
its later review-baseline publication marker exists. This avoids treating an
unfinished snapshot copy as a completed published source. Both roots remain
watched after task completion. No baseline is accepted solely because it was the
first version observed. These checks are sampled integrity observations, not a
kernel write fence or proof that mutations restored between reads cannot occur.

## Repeated stale-lease control

`NativeStaleControl` creates one fresh synthetic project, worker and task on an
explicitly owned test daemon. Setup obtains a real grant, pauses/resumes the task,
and obtains a newer epoch. Each monitor sample submits a heartbeat with the old
attempt/epoch and the current aggregate version. It requires the specific stale
identity conflict and exact aggregate equality before renewing the current grant.
Wrong refusals, changed ownership and failed renewals permanently fail the control.

Pass the initialized control to `run_native_monitor(stale_control=...)`; seven-day
runs require it. Samples retain each rejection/renewal result and the run identifies
the control task. The caller explicitly cancels this synthetic task after monitoring
and owns daemon cleanup. The monitor never selects an existing user task.

This repeated synthetic control exercises the real daemon's lease boundary beside
native work. It does not repeatedly crash native workers or replace the separate
actual-native prior-lease recovery proof. It cannot establish arbitrary descendant
fencing, unobserved mutation absence or full fault-matrix acceptance.

## Auxiliary invocation correspondence

New auxiliary supervisors carry `--conductor-aux-owner=v1:AUTHORITY:INVOCATION:HASH`
alongside their invocation argument. The trusted supervisor checks that tag
against validated launch input before its private claim or Ready. Only public
identities and the already-recorded launch digest appear in argv; no raw payload,
source, credential or command text is added. Tags remain forgeable observations,
never authority to signal, release work or settle ownership.

The inspector discovers renamed tagged processes, takes independent start-marker
reads, and correlates bounded preparation, launch, admission-host and process
records. It also observes recorded PIDs independently so tag loss cannot silently
hide a current process. Pending disappearance, PID reuse, wrong launch/binding,
unacknowledged launch, settled-but-still-observed process and historical absence
remain distinct. Other-authority tags are excluded. Legacy schema or malformed
records cannot count as healthy integrated coverage. Legacy untagged processes
cannot be assigned an authority from argv alone; there is no claim of universal
untagged orphan detection.

The guard uses its configured finite grace for contradictory/uncertain auxiliary
observations, keyed by invocation so changing classifications cannot reset the
window. Only matching observations and unavailable historical settled processes
are clean; disappearance never settles a pending invocation. Reports expose
bounded identities/classifications, not stored JSON or host command lines. The
unattended monitor pins the auxiliary inventory module with its other sources.

Native `aux-observation-native-v1` failed with a 10-second grace during verifier
preparation. The separate v2 run uses an explicitly selected 60-second grace;
no default or invariant was relaxed. Samples recorded about 19.54 seconds from
first to last pre-ACK observation, including about 9.39 seconds with a visible
supervisor. Source performs executable validation before spawn and before Ready;
these samples do not isolate hashing cost or establish a universal timing bound.
V2 completes Claude writing, isolated checks and Codex review with 332 samples,
both auxiliary roles observed live, matching board projection and one completion.
Independent verification binds recorded PIDs/start markers and settlement scope.
A real additional scoped process without a database record is detected; only the
test harness terminates its own process. All owned daemons are stopped. This is
short sampled acceptance, not an uninterrupted seven-day run.
