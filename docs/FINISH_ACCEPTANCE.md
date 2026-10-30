# Finish acceptance evidence

`finish_acceptance.json` inventories the six roadmap packages and all thirteen
functional/five non-functional requirements. Every row is initially pending and
there is no final candidate. This is deliberate: historical tests and the current
shared-room fixture do not establish finished-product acceptance.

Run the read-only verifier with an existing private evidence directory:

```sh
python3 tools/verify_finish_acceptance.py --evidence-root /absolute/private/evidence
```

Exit 0 means every declared gate has mechanically valid evidence bindings and
checks. Exit 1 means incomplete, failed, malformed or mismatched evidence. The
tool does not run models, modify files, stop processes, access the network or
install anything. Its JSON output lists accepted gates and errors. It is not an
independent verifier of the truth of a producer's observations: inspect the
underlying recordings and test implementations before accepting their claims.
Hashes establish identity, not trustworthy provenance or scientific independence.
Even independently authored receipts and attachments cannot be proven truthful
by hashing. Successful output explicitly has scope
`evidence_contract_consistency_only`, not a product acceptance verdict.
Required check lists must match the matrix committed in the pinned source; an
uncommitted receipt update cannot weaken them. Changes to those lists remain a
reviewed requirements decision.

## Candidate and receipt format

Set `candidate.source_commit` to the actual clean checkout HEAD. Set
`candidate.artifacts` to a nonempty map of artifact names to `{path, sha256}`
references. Paths are relative to the explicit evidence root; traversal and
symlinks are refused. Receipt JSON and soak samples are read into bounded buffers
once, and those exact hashed bytes are parsed; replacing the pathname after
hashing cannot substitute a different decision or observation stream. All shipped binaries/assets requiring acceptance belong in
this manifest. The only allowed uncommitted checkout change is the matrix itself,
so evidence can be recorded without pretending that uncommitted source was tested.
Keep the evidence directory outside the source checkout.

An accepted row has `status: "accepted"` and `evidence` containing one or more
receipt `{path, sha256}` references. A receipt uses this contract:

```json
{
  "schema": "conductor.finish-evidence.v1",
  "gate_id": "P1",
  "source_commit": "<actual candidate commit>",
  "artifact_sha256": {"<artifact name>": "<actual SHA-256>"},
  "scope": "native",
  "passed": true,
  "checks": {"<required check name>": true},
  "attachments": {"<recording name>": {"path": "<relative path>", "sha256": "<actual SHA-256>"}},
  "metrics": {}
}
```

This example is a format description, not evidence. Every receipt must bind the
same source and complete artifact map, include hashed underlying recordings and
pass its reported checks. Combined receipts must cover every named check in the
row. Failed, skipped or unknown check values do not close a gate. Fixtures may be
retained as useful development evidence, but cannot close final native gates.
P5, P6 and NFR-1 require `scope: "full"`; other gates require `native` or `full`.
A scope label alone cannot bypass the additional checks below. Historical receipt
formats are not automatically promoted; create a reviewed manifest referencing
their actual bytes only when their source and scope meet the final requirement.

## Additional measured gates

- P3: `worker_uids` must contain at least two distinct numeric identities, and
  `vendors` must include `codex` and `claude`. Check recordings must independently
  establish own-HOME/auth and no credential copying.
- NFR-2: `projects >= 100`, `tasks >= 10000`, `events >= 100000`,
  `board_p95_ms < 100`, `wake_p95_ms < 500`, and a nonnegative measured
  `idle_cpu_percent`. These checks do not invent an idle CPU performance bound.
- NFR-3: `accepted_hosts` includes `macos-arm64`, `linux-arm64`, `linux-amd64`.
- NFR-1: `scope: "integrated_native"` within metrics, `uninterrupted: true`,
  `requested_duration_s >= 604800`, positive `rss_limit_bytes`, and nonnegative
  `queue_limit`. A hashed `soak_samples` attachment is required. Each JSONL sample
  records `elapsed_monotonic_s`, `boot_generation`, `invariants_passed`,
  `native_monitoring`, `rss_bytes`, and `pending_queue`. It also requires an
  `invariant_violations` map containing exactly `leases`, `projections`,
  `tagged_orphans`, `stale_epochs`, and `unfenced_surfaces`, each an integer zero.
  These counters represent violations observed in each monitoring interval;
  omitting a counter is not zero coverage. Observations start within
  one second, strictly increase with no gap over 190 seconds, retain one positive
  boot generation, satisfy their declared resource bounds and extend at least
  seven days between first and last observation. Native-monitor and invariant flags must be true throughout. Merely
  setting a duration/complete flag or supplying a control-plane-only receipt fails.

The production monitor must actually observe all required native process and
ownership invariants; this reader cannot manufacture that missing monitor.
Account selection/logins, authorized physical reboot/host access, the final
native endurance run and final artifact/CI evidence remain prerequisites. The
existing native-chat backup refusal must remain until an accepted recovery/write
fence procedure exists. The matrix does not authorize disruption or publication.
