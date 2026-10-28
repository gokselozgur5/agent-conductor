# Command sandbox evidence

The runtime has an explicit macOS command sandbox constructor. It uses
`/usr/bin/sandbox-exec`, a deny-by-default profile, one canonical writable work
root, explicit read-only tool roots, an allowlist of system runtime paths, and
an explicit child environment. Profile parameters carry paths, so quotes in a
workspace name cannot inject policy syntax. Network access is denied, including
TCP and Unix control sockets. Other platforms and inference-only network policy
currently return an unsupported error; they never fall back to unrestricted
execution.

## Executed evidence

`crates/runtime/tests/sandbox.rs` launches real sandboxed processes on macOS:

- A shell writes inside its workspace.
- Reading and writing a sibling fixture is denied.
- A workspace symlink cannot grant access to an outside fixture.
- A child shell inherits the denied outside-write boundary.
- A compiled probe receives permission-denied errors connecting to a live
  loopback TCP listener and a live Unix control socket outside the workspace.
- Unsupported inference-only authority is refused explicitly.

The initial profile caused even system programs to abort. An echo-only A/B
probe established that a literal read of `/` is required for startup on this
host. Allowing that directory itself fixed startup; recursive root access was
not added. All negative probes still pass. The implementation also allows the
sealed macOS runtime image for dynamic loading. These are local observations,
not a claim that all macOS versions or tools have been tested.

## Remaining gates

This boundary does not prove process termination, revoke existing descriptors,
or provide a live vendor adapter. A process can fork; unknown descendant fencing
still blocks task reassignment. Real Codex/Claude CLI startup, credential access,
inference transport, denied-action conformance, read-only verification and
controlled check execution need their own evidence. The current service accepts
only controlled fake workers. Do not advertise the command tests as completed
vendor conformance or full-product acceptance.

The owner-selected read roots are capabilities, not automatically safe defaults.
The adapter builder must refuse control-plane/credential roots and pin the actual
binary before launch. Never give an agent the control database or operator socket.
Ignored source files remain outside the prepared working copy.

## References

The [upstream Codex Seatbelt base policy](https://github.com/openai/codex/blob/main/codex-rs/sandboxing/src/seatbelt_base_policy.sbpl)
provides a primary-source example of inherited process policy and same-sandbox
signal boundaries. [Codex sandbox documentation](https://learn.chatgpt.com/docs/agent-approvals-security)
describes the platform split. These references informed the implementation;
the local positive and negative probes above are the actual evidence for it.
