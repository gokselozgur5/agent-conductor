# Adapter-reported usage

Select **Reported usage** in the operator inventory, or run:

```sh
./tools/conductor ctl --json inventory usage
```

The authenticated equivalent is `GET /api/inventory?resource=usage`. Existing
100-row ID-keyset pagination and response limits apply. Reports are immutable
observations, not invoices, account totals or task acceptance. The interface does
not add token categories or infer prices.

## Sources and meaning

Only a unique final top-level native terminal event supplies accounting fields.
Claude `result` reports can provide input/output/cache-read/cache-creation token
counts and `total_cost_usd`; its bounded `system/init` model field is retained when
valid. Codex `turn.completed` or `turn.failed` can provide input/output/cached-input
counts. Codex cost and model remain unknown when not reported by these envelopes.
No model/effort value is inferred from an adapter binary version or user defaults.

Counts are nonnegative integers rendered as decimal strings to avoid JavaScript
rounding. Reported USD cost is a normalized nonnegative JSON number, bounded to
one billion; it remains a vendor claim. Missing fields are null or absent, not
zero. Explicit zero is preserved. Invalid individual fields are omitted and
named in `invalid_fields`; unknown fields and nested tool/model text are ignored.
Statuses distinguish reported, partial, missing, invalid, incomplete and ambiguous
streams. A terminal failure may still carry a report. Reports do not turn failures
into successful candidates.

Parsing is bounded to 2 MiB output, 512 KiB per line and 10,000 events. Duplicate
terminals or content after a terminal are ambiguous. Truncated streams cannot
establish final accounting. No raw prompt, tool content, model prose, credential
field or complete output is stored in a usage observation.

## Identity, durability and gaps

Every row binds task, attempt, lease epoch, agent, role, adapter identity, packet
hash and output hash. Worker invocation identity is the existing execution token;
each reviewer process launch receives a distinct invocation ID. Reprocessing the
same invocation is idempotent. Different invocations remain distinct even when
their output bytes are identical; changed metadata for one identity is refused.

The service captures reports after a settled, identity-checked worker terminal
observation (including controlled stops), and after a reviewer process returns,
before interpreting its verdict. These observations do not renew leases or alter
lifecycle authority. Their single-statement immutable audit insert is independent
of later task transitions; a task can fail after a report is persisted. A committed
insert whose response was lost is recovered by the same idempotency identity.

Older tasks cannot be backfilled: their raw output was deliberately not retained.
A daemon crash, unobserved terminal, process timeout/output-limit error without
returned bytes, or lost reviewer result can leave no report. Incomplete output
may only yield an incomplete-status row. This is partial per-invocation telemetry,
not a complete billing ledger; absence must never be interpreted as free execution.

Migration 16 adds a partial audit index for usage inventory, avoiding scans through
unrelated audit history. Older binaries refuse that database's newer migration;
retain the pre-upgrade backup for rollback. It does not rewrite older audit data.

## Validation

Parser controls cover both formats, explicit zero and maximum integer precision,
missing/invalid/partial/failure fields, overflow/fractions, nested prose, duplicate
terminals, truncation and bounds. Store controls prove immutable replay/collision,
unchanged task state, restart durability and use of the partial index. A service
regression distinguishes two identical-output invocations from replay of one.

Operational evidence is retained under
`reviews/agent-conductor-runtime-20261028/usage-*` in the operator workspace.

Final validation passes 255 workspace tests, strict Clippy, formatting, release and
credential checks. The real pinned Claude → checks → Codex workflow completed in
29.31 seconds and retained two hash-bound invocation reports through restart.
Claude reported token categories/model/cost; Codex reported token categories while
model/cost remained unknown. The original project was unchanged. These are vendor
claims, not independently measured spend. The release CLI/HTTP/Chrome readback
shows both reports with exact integer strings, authentication, unknown-not-zero
labels and disconnect clearing. A copied migration-16 database was actually
refused by the previous release. Receipts and binary/evidence hashes are bound in
`usage-validation-manifest.json`; no additional model calls were used for browser
or compatibility checks.
