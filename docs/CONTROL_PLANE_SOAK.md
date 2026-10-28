# Continuous control-plane endurance exercise

`tools/soak_control_plane.py` owns an isolated real daemon, SQLite database and
loopback board. It exercises the durable control plane continuously. It does
**not** start vendor workers, enable dispatch, observe repositories, perform
physical reboots, or satisfy full v1/native-process soak acceptance.

```sh
python3 tools/soak_control_plane.py \
  --bin-dir /absolute/path/to/validated/binaries \
  --output /absolute/path/to/new/private/run \
  --socket-dir /absolute/path/to/existing/short/socket/directory
```

Defaults request seven days with one cycle per minute. Use
`--duration-seconds 60 --interval-seconds 1` for a preflight. Existing output is
refused; an interrupted run is never resumed or combined with another run to
manufacture continuous duration. Preserve the output and the independently
created generation-authority directory beside it. Do not move/delete authority
records to restart a fixture.

Each cycle creates one synthetic task, claims/starts its first attempt,
pauses/resumes, claims a second epoch, requires an explicit stale-epoch conflict
and unchanged aggregate, then cancels. No executable worker is attached. It
publishes an event twice, requires semantic deduplication, consumes/acknowledges
one delivery with versioned consumer state, and refuses duplicate/source-echo
consumption. These mutations use the real CLI/service; the harness never writes
the database directly.

After the cycle it checks runtime invariants, traverses all authenticated board
task pages and compares aggregates against read-only SQLite state. This comparison
is made while the harness has quiesced its own writes; it is not a general
concurrent snapshot protocol. It also checks exact task/event counts, no live
leases, no execution/dispatch records, no pending deliveries and no daemon child
processes. Unexpected files/symlinks in the private state root fail the run.
A child-process absence check does not detect arbitrary escaped/reparented native
workers; no such workers are authorized in this fixture.

## Limits and evidence

- At most 12,000 requested cycles; each adds one task and one semantic event.
  Durable history is expected to grow and is retained, not pruned to hide growth.
- Daemon RSS defaults to a 512 MiB ceiling, with a separate 64 MiB growth allowance
  above its first completed observation. These are exercise budgets, not universal
  product memory guarantees. Controller RSS is not measured.
- Total state files must remain within 2 GiB and daemon output within 16 MiB.
- Observation gaps exceeding three configured intervals plus ten seconds fail.
  A wall/monotonic difference above five seconds fails, including the initial and
  final observation windows. Suspension and clock jumps are not counted as healthy
  observation time. These are sampled checks, not continuous kernel tracing.
- A changed boot generation or early daemon exit fails. Binary hashes are captured
  initially and verified at the end. Use immutable validated binaries throughout.
- Board credentials are generated in memory, passed only in the child environment
  and HTTP headers, and omitted from receipts. Vendor credentials are not inherited.
  Output/receipts are private; status replacement and samples are flushed to disk.

`run.json` binds configuration, binary/script hashes and owned PID. `samples.jsonl`
retains every successful sample; `status.json` records `running`, `failed`,
`completed_short_probe` or `completed_control_plane_soak`. A short requested run
cannot become a seven-day result because it was delayed. Every receipt explicitly
sets `full_v1_accepted` to false. A stale `running` file is not proof of liveness;
inspect the original monitor handle and owned daemon before acting. Missing or
interrupted observations cannot be reported as success. SIGINT/SIGTERM stop the
owned daemon and record failure; SIGKILL/power loss may leave a stale receipt and
require separate ownership/recovery inspection. The harness never kills an
arbitrary PID taken from a file.

Tests inject each monitored contradiction, enforce duration classification and
private status replacement, run a real short daemon exercise, refuse state reuse,
and corrupt an isolated real task projection to prove the monitor fails. CI runs
these controls on macOS and Linux. This is not proof of all external-effect crash
boundaries or the complete NFR-1 checker: tagged orphan processes, native unfenced
write surfaces, supported host isolation and native fault/soak coverage remain
separate required work.
