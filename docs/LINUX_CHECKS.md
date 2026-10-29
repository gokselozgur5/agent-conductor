# Linux project checks

This backend applies to required project checks, not full-capability agent workers.
Codex and Claude workers retain the selected account's shell, tools, network and
HOME authentication. Independent vendor reviewer acceptance is a separate gate.

## Host requirements

The runtime must have `conductor-check-sandbox` beside its other binaries and
`/usr/bin/bwrap` installed by the operator. The host must permit unprivileged user,
mount, PID, IPC, network and UTS namespaces and support seccomp. The bubblewrap
build must support `--disable-userns`, `--seccomp` and `--remount-ro`.

On Debian/Ubuntu, the distribution package is named `bubblewrap`. Package generation
includes the Conductor helper but does not install or change system dependencies.
An unavailable dependency or rejected namespace setup fails the check. There is
no unrestricted fallback and Conductor does not change host security settings.

## Execution boundary

Each check receives its own source-bound copy. The copy is the writable host
mount; selected toolchain roots and system binaries/libraries are read-only.
Synthetic parent directories and the sandbox's own `/proc` and `/dev` mounts are
also read-only. Host `/proc`, `/sys` and `/dev` cannot be supplied as workspace or
read roots. Host home and temporary directories are not implicitly exposed.

A sealed in-memory seccomp program denies socket operations, including pathname
Unix sockets, and io_uring alternatives. It rejects incompatible syscall ABIs.
The single-threaded helper closes inherited descriptors other than stdio and its
filter before invoking bubblewrap. The supervised caller supplies null stdin,
captured output pipes and an explicit environment. Missing or invalid setup never
launches the check directly.

These controls do not grant a fence for an unrelated unrestricted worker. The
existing lease, capture, source hash and independent acceptance rules still apply.

## Acceptance

The runtime test suites `sandbox` and `checks` cover allowed workspace writes,
denied outside reads/writes and symlink traversal, live TCP and Unix sockets,
source binding and unchanged source files. Linux-specific probes cover inherited
file descriptors, io_uring denial, incompatible ABI termination and rejected host
kernel filesystem roots. Run them after `cargo build --workspace` so the helper
is available beside the test executables.

Exact source `ae57be781a847be423dc44009f22f0bb2d1f51a4` passed these actual Linux
boundary and source-binding tests in [CI run 40948331899](https://github.com/gokselozgur5/agent-conductor/actions/runs/40948331899).
The macOS regression job passed in the same run. Local strict Clippy checks also
passed for both Linux x86_64 and arm64 targets. Compilation for an architecture
is not runtime acceptance. Native arm64 evidence was subsequently obtained below;
personal vendor reviewer acceptance remains a separate gate.

The observed Linux runner was Ubuntu 26.04 x86_64 with bubblewrap package
`0.11.1-1ubuntu0.3`. This is the tested host, not proof that every distribution
or bubblewrap version supports the required features and namespace policy.

Source `f8fb6b74925772df10bcc9119ba8208768fcfe92` passed the full native arm64 job
in [CI run 40949494563](https://github.com/gokselozgur5/agent-conductor/actions/runs/40949494563),
alongside macOS and Linux x86_64. The job explicitly verified `aarch64`, ran the
isolated filesystem/socket/syscall/descriptor and source-binding tests, and passed
the controlled receiving-worker test under UID 65534. This is runtime evidence
on the selected hosted image, not personal vendor account acceptance.

The upstream [bubblewrap manual](https://github.com/containers/bubblewrap/blob/main/bwrap.xml)
defines the namespace, mount and seccomp options. Its
[limitations](https://github.com/containers/bubblewrap#limitations) explain why
exposed host sockets require more than a read-only mount or network namespace.
