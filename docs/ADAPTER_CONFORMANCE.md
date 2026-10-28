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
Production code is `crates/runtime/src/vendor.rs`; no real vendor is enabled in
the service launcher yet.

## Outstanding gates

These are preliminary write-tool checks, not full adapter acceptance. Read-only
verifier denial, network/control-socket attempts through each actual harness,
plugin/config prompt injection, interruption/resume, output protocol validation,
source/check binding and full daemon integration still require evidence. Claude
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
