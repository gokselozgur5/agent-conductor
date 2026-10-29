# Native runtime kits

The CI workflow builds separate native kits for macOS arm64, Linux x86_64 and
Linux arm64. A kit contains the four release binaries, operator tools, migrations
and documentation. It excludes local state, credentials and vendor adapter pins.
These are CI artifacts, not a complete v1 release or vendor conformance approval.

Select a successful CI run for the exact source revision you intend to use and
download its `conductor-OS-ARCH-SHA` artifact from that run's summary. Inspect the
run's host information: Linux binaries are built on the recorded Ubuntu runner;
compatibility with older distributions is not established. Use Python 3.11+, Git,
and the host dependencies described in [Linux checks](LINUX_CHECKS.md).

Extract the artifact ZIP into a new directory. Before extracting its tar archive,
compare the archive's SHA-256 with `archive_sha256` in
`runtime-kit-receipt.json`. Retain the receipt and successful CI run identity.
The manifest provides integrity checking; it is not a signature or an independent
trust source. Only execute tools from a trusted build.

After extracting the tar archive, enter its top-level directory and run:

```sh
python3 -E -s tools/package_runtime.py --verify .
python3 -E -s tools/conductor --help
```

Follow `README.txt` for initialization and account setup. Run each receiving
worker under its selected OS account with that account's own HOME and vendor
login. An accepted plan must select the actual worker and independent reviewer;
the kit does not synthesize accepted pins. Full-capability workers retain their
account's normal tools and network access.

CI runs debug and release tests, re-extracts each archive, verifies its complete
file set and hashes, and starts and gracefully stops an empty-project coordinator
before uploading. The smoke test makes no vendor calls and does not prove vendor
authentication or a complete task workflow.

Never overwrite a running installation. Stage upgrades in a separate directory
and preserve the existing state and accepted evidence. Final upgrade/rollback,
personal vendor acceptance and endurance gates remain tracked in
[delivery acceptance](DELIVERY_ACCEPTANCE.md).

To produce the same format from a clean source checkout:

```sh
cargo build --locked --release --workspace
python3 tools/package_runtime.py --bin-dir target/release --destination target/dist/conductor-native
```

Archive generation is deterministic for identical input bytes, source revision
and destination basename. This does not claim reproducible compiler output.
Local callers are responsible for supplying binaries built from that revision;
CI establishes this relationship through its build steps.
