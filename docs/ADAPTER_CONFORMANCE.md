# Native adapter conformance

## Trust boundary

The vendor CLI harness is trusted to hold authentication and contact its model
provider. The model-facing tools receive a separate restricted capability set.
This matches the native harness boundary; it does not claim that the entire CLI
process is confined by the standalone command sandbox in SANDBOX.md.

Conductor constructs fixed argv, disables customization/approval bypasses, pins
binary content and an operator-accepted receipt, and never treats vendor output
as task completion. Authentication stays in the child environment; debug output
redacts environment values and prompts. The runtime does not read key files.
CLI session persistence is disabled. Vendor infrastructure may retain requests
under its service policy; these probes contain only synthetic fixture material.

## Observations on 2026-10-28

| Harness | Version | Executable SHA-256 | Observed scope |
| --- | --- | --- | --- |
| Claude Code | 2.1.311 | `85d8c08707fcfd1e78ffa0204eefbc6ec9e4fe22f375fb2767ef85d957ed3161` | Real model Write succeeds inside; actual outside Read and Write tool calls return errors |
| Codex CLI | 0.163.0 | `8813367285524ac798dd96d1b071aea30d58708c7226fd236cd275cf1cfdee31` | Native sandbox and real model shell call write inside; outside read/write are denied |

Claude used `--restricted`, `--safe-mode`, empty strict MCP configuration, only
Read/Write/Edit/Glob/Grep tools, `acceptEdits`, and no interactive permissions.
The first existing-login attempt failed before tools because no CLI login was
available. Two later synthetic API-key probes ran with a $0.50 per-call cap;
the first receipt parser failed after the process completed, so only its fixture
state was retained. The repeated probe has complete sanitized evidence: one
successful inside Write, two outside tool errors, two permission denials, no
outside marker and no canary in output. Actual billed dollars were not retained
and are not inferred from the cap. The key was never stored in the receipt.

Codex used existing authentication, no daemon, approval `never`, no user config or
execpolicy rules, ephemeral JSON output, project-doc loading disabled, web search
disabled and a named restricted filesystem profile. That profile denies root,
permits minimal system runtime reads and the workspace, denies extra temp roots,
and disables tool network access. The real model completed one shell command;
its command event reports both read and write denial and exit zero. The fixture
inside file exists, outside marker does not, and the canary is absent from output.

Sanitized receipts and reproducible private probe scripts live under the workspace
review directory `reviews/agent-conductor-runtime-20261028/vendor-probes/`.
Production command construction is in `crates/runtime/src/vendor.rs`. The service
has a pinned restricted-Claude launch/observation port; it is not yet exposed
through the daemon or CLI.

## Outstanding gates

Additional local probes exercised read-only modes. Claude advertised exactly
Read/Glob/Grep, read the fixture and left source unchanged. Codex ran a synthetic
probe through its actual model shell: fixture reads succeeded while workspace
writes, live TCP connections and a live Unix control socket were denied with
PermissionDenied. Sanitized receipts are `claude-readonly-v1/receipt.json` and
`codex-readonly-network-v2/receipt.json` under the same private evidence root.

Bounded native output parsing rejects malformed/truncated streams, duplicate
terminal events, nonzero exits and vendor failures. Tool/model prose stays data.
Service regressions bind candidates to the released packet, persist invalid
results as blocked, and prove that successful exit does not complete or fence a
task. These protocol fixtures are not real vendor integration acceptance.

Full adapter acceptance still requires plugin/config prompt injection,
interruption/resume, source/check binding and full daemon integration evidence. Claude
currently has no command execution capability; checks belong to the trusted
bounded check executor. The initial command builder keeps its per-call $0.50 cap.
Binary hash validation is not protection against a malicious same-user process
replacing the executable between validation and OS execution.

## Primary references

- [Codex named permission profile compilation](https://github.com/openai/codex/blob/main/codex-rs/core/src/config/permissions.rs)
- [Codex approvals and security](https://learn.chatgpt.com/docs/agent-approvals-security)
- [Claude Code CLI reference](https://code.claude.com/docs/en/cli-reference)

Installed CLI help and the executed probes determine the observed local behavior;
current upstream documentation alone does not certify these pinned versions.

## Restricted writer finalization

The internal source-finalization port accepts a normal restricted-Claude writer
exit only when the durable wrapper record is `exited` with code zero, the accepted
candidate matches the stored packet, current task ownership remains valid, and
the observed tool envelope contains only Read/Write/Edit/Glob/Grep with no
observed command tool. Adapter admission also forbids workspace commands. Its
audit records packet, output, binary and conformance hashes.

This is a capability-based write fence under the existing trusted, pinned CLI
assumption: model-controlled writes occur through confined file tools and the
owning CLI has exited. It is not a generic OS descendant-death proof, protection
against a compromised vendor binary or hostile same-user host process, or
acceptance of a command-capable Codex writer. Interrupted/malformed executions
remain blocked. Tool-envelope parsing alone is insufficient.

The service then captures the private attempt tree outside the writer root,
including new files and baseline deletions, and binds its manifest digest to the
verification state. Capture failures preserve partial artifacts; no acceptance
verdict is recorded. Source finalization uses two lifecycle transactions (begin,
bind); a crash between them can leave an unbound verification state requiring
reconciliation. End-to-end native acceptance and automatic recovery remain open.
