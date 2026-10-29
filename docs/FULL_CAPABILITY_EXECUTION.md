# Full-capability agent execution

## Accepted direction

The operator requires agent freedom, including shell, builds/tests, tool selection
and network use. Conductor is the coordinator, not a replacement coding harness.
The historical two-file-tool broker profile remains useful evidence about bounded
file effects but is insufficient for the requested product.

## Pending execution boundary

One operator question is open: use a separate execution environment with full
capabilities and selected account access, or run with the existing user account
across the host. No answer is inferred from elapsed time. Do not install/start a
VM or container, change host permissions, or introduce a permanent restricted
fallback as a substitute for that answer.

## Required integration

- Native workflow construction currently hard-codes `allow_workspace_commands`
  to false. Replace that assumption with the operator-selected execution profile;
  preserve the frozen profile through retries and reassignment.
- `native_writer_launch` currently admits only restricted Claude writers. Add the
  full-capability route for Codex and Claude under the selected execution boundary.
  Do not reuse `file_only_tools` as evidence for a command-capable run.
- Service-created execution intent, current leases, durable acknowledgement and
  process ownership remain required. They coordinate execution; they do not
  constitute OS containment or prove that all descendants have stopped.
- Make stop/recovery semantics truthful for the selected boundary. A process-group
  exit is not proof that escaped descendants stopped. Preserve uncertainty and
  prevent conflicting reassignment when the previous writer may still be active.
- Feed captured source into isolated checks and independent review. A worker's
  prose, exit status or self-reported tests cannot complete its own task.

## Acceptance scenario

Use the normal operator launcher and CLI to register a project and create a task.
The actual agent must inspect context, execute a useful shell command, run the
project's check and make the requested change with its normal tools. Verify network
use through a controlled allowed endpoint under the selected boundary. Interrupt
mid-change, restart Conductor, then resume or reassign without losing work or
allowing concurrent owners. An independent agent reviews the resulting source;
the board reaches completed exactly once with the full causal record.

Keep the installed preview and existing user processes unchanged until an explicit
upgrade. Report this scenario's actual outcome and the next concrete blocker;
commit/test counts and unsupported percentage estimates are not completion proof.
