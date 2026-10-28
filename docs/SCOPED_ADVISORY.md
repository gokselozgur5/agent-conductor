# Scoped advisory intake

The operator can discover continuity and instruction-shaped documents outside a
Git project without granting those documents authority or widening agent tools.
The existing snapshot advisory inventory remains unchanged.

## Operator flow

```sh
tools/conductor inspect --workspace-root /absolute/workspace

tools/conductor plan --from-plan /absolute/accepted-adapters.json \
  --task example --objective-file /absolute/objective.txt \
  --checks-file /absolute/checks.json \
  --workspace-root /absolute/workspace \
  --document AGENTS.md --document README.md --document docs/PROJECT_STATUS.md
```

`--workspace-root` is opt-in. Without it, inspection and generated plans retain
their existing behavior. `--document` requires a workspace root and replaces the
default candidate list. Discovery walks only the selected root through the project
in outer-to-inner order, inspecting the same named candidates in each directory.
It does not recurse through unrelated directories or walk above the boundary.

Defaults: AGENTS.md, CLAUDE.md, README.md, STATUS.md, PHASES.md, TRACKING.md,
ARCHITECTURE.md, REQUIREMENTS.md, DECISIONS.md, HANDOFF.md, TODO.md and docs/STATUS.md.
Custom relative paths support repository-specific equivalents. Missing candidates
are skipped; files with unsupported types, permissions or content fail discovery.
The CLI equivalent is `conductorctl inspect-workspace PROJECT --workspace-root ROOT`
with repeated `--document PATH` options. Direct CLI paths must be canonical and
absolute; the launcher resolves the explicitly supplied root first.

## Frozen data and authority

The launcher stores selected absolute paths and SHA-256 hashes in the optional
`advisory_sources` plan field. These pins identify data, not approved instructions.
Submission, dispatch and continuation verify the selected bytes. A changed or
missing source refuses the operation; the plan is not silently refreshed.
Generating a new plan is the way to select revised content.

Before assigning the worker, the broker captures verified source text into a
labelled `scoped_advisory_sources` envelope. That exact data is included in the
persisted worker packet and cloned into the independent review packet. Native
agents receive no extra read roots, write roots or command permission. The source
files are not copied into the candidate project. In-flight advisory context is a
frozen snapshot; later changes do not retroactively edit that packet. Active
invalidation of authoritative instruction pins remains a separate mechanism.

`--instruction FILE` continues to mean explicit operator approval of an
instruction source. An AGENTS.md or CLAUDE.md filename found by discovery cannot
promote itself into that list. Text, embedded commands and role claims inside the
advisory envelope are untrusted. The trusted operator remains responsible for
selecting a workspace whose named documents may be sent to the configured models.

## Boundaries and compatibility

- At most 32 ancestor directories and 32 configured candidate paths.
- At most 64 distinct selected files, each at most 16 KiB of UTF-8 text.
- At most 32 KiB of total source text and 64 KiB of encoded context, including JSON
  escaping and paths. Excess fails explicitly instead of silently omitting sources.
- Descriptor-relative reads reject symlinks at every component, hardlinks and
  special files. Relative candidates reject absolute paths, empty components,
  dot/dot-dot and `.git` components. No arbitrary directory enumeration occurs.
- The existing whole-packet bound still applies to the combined objective, checks,
  decisions, checkpoints and context; these limits do not reserve unlimited space.
- Empty fields are omitted, preserving old plan encodings. Older daemons reject
  the unknown nonempty plan field rather than treating advisory pins as authority.
  There is no schema migration; operator binaries should be upgraded together.

## Validation

Regressions cover order, missing candidates, custom names, pinned-byte change and
removal, out-of-root selection, traversal, directory/file symlinks, hardlinks,
special files, non-UTF-8 input, depth/count/text/JSON bounds, optional serialization
and separation from authoritative instructions. Local validation passes 242
workspace tests and strict Clippy. A real CLI/launcher probe verifies custom
outer-to-inner discovery, changed-source refusal before queue admission, duplicate
submission, queued restart and actual old-binary refusal. A native Claude/checks/
independent Codex task completed in 39.07 seconds using the selected external data.
Its persisted packet contains exact source text and only the existing workspace
read root; the original Git state and source files are unchanged. Evidence is
`scoped-advisory-v1/receipt.json` under the private runtime review directory.
Synthetic fixtures contain no user documents; model spend was not measured.
This does not complete remote host isolation or the full-product soak gate.
