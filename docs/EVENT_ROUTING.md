# Event subscription matching

Publication, delivery leasing (including expired-lease retries), and workflow
wake admission use the same current subscription predicate. Matching is literal
and case-sensitive. A trailing `.*` selects a topic family: `file.*` matches
`file.created`. Other underscores and percent signs have no wildcard meaning.
`*` alone is not a global subscription.

Path scope is a raw textual prefix, not a directory-boundary check. Use a trailing
slash to select a directory: `/workspace/a_b/` does not match
`/workspace/axb/file` or `/workspace/A_b/file`. A prefix `src/a` also matches
`src/abc`. Optional project scope and source-echo suppression still apply.

Updating a subscription does not rewrite durable delivery history. Before a
pending delivery or expired lease is claimed, its current subscription must be
active, belong to the target agent, and match the event. The same recheck occurs
inside atomic workflow wake admission. An old ineligible delivery remains
unclaimed and does not starve a later eligible event. It can expire under the
existing policy, or become eligible again if the filter is restored in time.

Narrowing a subscription does not revoke an already-issued live claim token.
Acknowledgement retains its existing target, token and expiry contract. Wake
admission still requires an operator-approved plan, non-echo subscription,
exclusive claim and generation below eight; matching grants no new authority.

## Validation

Three regressions fail against base `4644213`: literal prefix matching, leasing
a delivery after narrowing its subscription, and waking a queued workflow from
that stale delivery. The fixed event suite passes all sixteen tests, including
positive literal underscore/percent/case controls, later eligible-event progress,
unchanged in-flight acknowledgement and atomic wake behavior.

Evidence is retained under
`reviews/agent-conductor-runtime-20261028/literal-routing-*` in the operator
workspace. No schema migration is required. Older binaries still implement the
old routing semantics; database compatibility is not equivalent routing behavior.
This correction does not close the remaining causal/recovery scope in issue #19.

The release daemon/CLI restart probe completed a pinned Claude writer → checks →
independent Codex workflow in 31.46 seconds. Wrong underscore/case/topic events
created no delivery. A previously queued event became ineligible after narrowing,
was neither leased nor consumed after restart, and remained pending. The correct
literal event triggered one completed execution; replay and another restart did
not duplicate it. The original project remained unchanged. Receipt:
`literal-native-v1/receipt.json`; source script: `literal_native_probe.py`.
All 245 workspace tests, strict Clippy, formatting and release build passed.
Model spend was not measured. This is one bounded local acceptance, not a soak.
