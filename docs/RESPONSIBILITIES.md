# Responsibility boundaries

The delivery target is one working project-to-verification workflow. Additional
abstractions must remove an actual coupling or support an exercised second
implementation. More modules, tests or commits do not establish product readiness.

## Ownership

| Owner | Responsible for | Must not decide |
| --- | --- | --- |
| Domain | Task transitions, ownership epochs and completion invariants | Vendor commands, filesystem operations or process execution |
| Store | Atomic durable state, audit and idempotency | Whether model prose is correct |
| Conductor service | Assignment, lifecycle coordination and evidence binding | Implementing the worker's engineering solution or approving its own result |
| Worker adapter | Translating a frozen contract into the selected model's supported invocation and interpreting its event protocol | Widening authority, changing acceptance requirements or completing a task |
| Worker agent | Its assigned implementation, bounded scope, checkpoint and candidate result | Verifying its own work or assigning itself wider authority |
| Workspace runtime | Isolated working copies, source identities and write-boundary evidence | Declaring tests passed |
| Check executor | Exact approved commands, bounded execution and source-bound receipts | Selecting acceptance criteria or granting a reviewer verdict |
| Independent verifier | Reviewing the same source and check evidence against the assigned acceptance requirements | Writing implementation files or reviewing its own worker attempt |
| Daemon | Hosting services, transport, scheduling and shutdown | Duplicating task transition rules |
| CLI / board | Operator input and authoritative state presentation | Maintaining an alternative task state machine |

## Contracts, not implicit cooperation

A worker receives one frozen attempt contract: task and attempt identity, epoch,
workspace, capability boundaries, instructions, required checks and deadline.
Its output is a candidate tied to that contract. Model prose cannot redefine the
contract. A checkpoint preserves unfinished work; it is not a success signal.

The check executor consumes an approved definition and identified source and
produces a receipt. The verifier consumes the same source and receipts and
produces an independently attributed decision. Only the domain can accept the
combination as completed. Changing models must preserve these observable
contracts and refusal semantics; unsupported capabilities are explicit failures.

## SOLID in this repository

- Single responsibility: keep vendor flags/event decoding in runtime adapters;
  lifecycle transitions stay in domain, persistence in store.
- Open/closed: another accepted adapter should fit the attempt/result boundary
  without adding vendor branches to lifecycle decisions.
- Substitution: adapters must honor the same capability/refusal contract. A
  command-capable adapter is not a substitute for a file-only writer until its
  write fencing is verified.
- Interface segregation: worker, check and verifier operations are separate
  capabilities; do not require every model to implement every role.
- Dependency inversion: coordination consumes execution/evidence contracts;
  vendor authentication and CLI flags remain outside domain policy.

## Current gaps, not a new framework

The existing domain/store boundary and check executor already follow most of
these ownership rules. Native writer admission now lives in the runtime adapter factory; the service
delegates admission and coordinates lifecycle. The runtime command builder still
dispatches on vendor kind. Keep subsequent vendor-specific rules inside that
boundary rather than adding vendor conditionals to service. Do not introduce a plugin registry,
DI container or general workflow language solely to remove one conditional.

Sequential durable dispatch, restricted-writer source finalization and independent
verification now compose into an exercised native workflow. Explicit fenced-workspace continuation now preserves prior work in a fresh
attempt; generic command-writer fencing and automatic continuation remain incomplete. Hooks and
host support stay in scope. An interrupted verification-phase workflow has passed retained-work continuation;
unfenced mid-writer interruption remains outside that evidence.
