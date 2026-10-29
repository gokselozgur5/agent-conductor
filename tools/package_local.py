#!/usr/bin/env python3
"""Build a host-local operator bundle from built binaries and accepted adapter pins."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import shlex
import sys
import subprocess
from adapter_pins import accepted_pins

REPO = Path(__file__).resolve().parents[1]
BINARIES = ('conductor-daemon', 'conductorctl', 'conductor-runner')
LAUNCHER = '''"""Run this bundle without rebuilding or locating an adapter template."""
import os
from pathlib import Path
import sys
root = Path(sys.argv[1]).resolve().parent
args = sys.argv[2:]
command_index = 2 if args[:1] == ['--state'] else 0
command = args[command_index] if len(args) > command_index else ''
if command == 'transfer-snapshot':
    executable = str(root / 'bin/conductorctl')
    os.execv(executable, [executable, *args[command_index:]])
if command == 'init' and '--bin-dir' not in args:
    args += ['--bin-dir', str(root / 'bin')]
if command in ('plan', 'account-check') and '--from-plan' not in args:
    args += ['--from-plan', str(root / 'accepted-adapters.json')]
os.execv(sys.executable, [sys.executable, '-E', '-s', str(root / 'tools/conductor'), *args])
'''


def launcher(interpreter):
    # Shell quoting covers spaces/apostrophes in host-specific interpreter paths.
    return ('#!/bin/sh\nexec ' + shlex.quote(str(interpreter)) + ' -E -s -c '
            + shlex.quote(LAUNCHER) + ' "$0" "$@"\n')


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def package(bin_dir, accepted_plan, destination):
    """Copy exact artifacts; packaging does not certify new adapter capabilities."""
    interpreter = Path(sys.executable).resolve(strict=True)
    bin_dir = bin_dir.resolve(strict=True)
    template = json.loads(accepted_plan.read_text())
    pins = {k: template[k] for k in ('worker', 'verifier', 'model_timeout_ms')}
    for optional in ('toolchain_roots', 'full_capability', 'account_worker'):
        if optional in template:
            pins[optional] = template[optional]
    pins.update(accepted_pins(template))
    for name in BINARIES:
        if not (bin_dir / name).is_file() or not os.access(bin_dir / name, os.X_OK):
            raise ValueError('build every runtime binary before packaging')
    commit = subprocess.check_output(['git', '-C', str(REPO), 'rev-parse', 'HEAD'], text=True).strip()
    dirty = bool(subprocess.check_output(['git', '-C', str(REPO), 'status', '--porcelain'], text=True))
    destination = destination.absolute()
    destination.mkdir(mode=0o700, parents=True, exist_ok=False)
    for directory in ('bin', 'tools', 'docs'):
        (destination / directory).mkdir(mode=0o700)
    for name in BINARIES:
        shutil.copy2(bin_dir / name, destination / 'bin' / name)
    shutil.copytree(REPO / 'crates/store/migrations', destination / 'crates/store/migrations')
    for path in REPO.joinpath('tools').iterdir():
        if path.is_file() and (path.suffix == '.py' or path.name == 'conductor'):
            shutil.copy2(path, destination / 'tools' / path.name)
    for name in ('LOCAL_OPERATOR.md', 'DELIVERY_ACCEPTANCE.md', 'OPERATOR_RECOVERY.md', 'FULL_CAPABILITY_EXECUTION.md'):
        shutil.copy2(REPO / 'docs' / name, destination / 'docs' / name)
    (destination / 'accepted-adapters.json').write_text(json.dumps(pins, indent=2) + '\n')
    (destination / 'accepted-adapters.json').chmod(0o600)
    (destination / 'conductor').write_text(launcher(interpreter))
    (destination / 'conductor').chmod(0o700)
    (destination / 'README.txt').write_text('''Conductor local preview (macOS native workflow)

Initialize once for a committed Git project:
  ./conductor init --project /absolute/project --port 8788
Start the board and explicitly submitted workflows:
  ./conductor run --dispatch
Copy the board credential in another terminal:
  ./conductor token | pbcopy
Read health/tasks/workflows:
  ./conductor status

Prepare a task using this bundle's accepted worker / independent reviewer:
  ./conductor plan --task first --objective-file /absolute/objective.txt --checks-file /absolute/checks.json
  ./conductor submit state/operator/plans/first.json

A checks file is a JSON array, for example:
  [{"id":"tests","argv":["/absolute/test-command"],"timeout_ms":60000}]
Use actual project checks. Submission authorizes model execution and those checks.
An optional --anthropic-key-file /private/key on run loads an existing owned mode0600
key into the daemon environment only. Model credentials are not included here.

For a successor, add --depends-on first --input-task first to plan. Accepted output
is retained under state/operator/artifacts; original project files stay unchanged.
Ctrl-C stops the foreground runtime. The same run command reopens its saved state.
Ambiguous interrupted executions require reconciliation before another attempt.
Use --state /short/private/path before the command for a separate runtime.

This is a host-local preview, not complete v1 acceptance. Distinct-UID deployment
acceptance, started unrestricted recovery and endurance remain open.
When full_capability is present, workers use normal shell/network tools and their
selected account's own HOME/login. For local dispatch, launch the runtime as that
account. When account_worker is present, start the receiving process as the worker
account before submitting tasks:
  ./conductor account-check --role worker
  ./bin/conductor-runner --account-worker SOCKET COORDINATOR_UID
Use the template's socket path and the coordinator account's numeric UID. The
receiver-owned socket parent must already be traversable by the coordinator; the
artifact directory must already exist. Conductor changes neither permissions nor
credentials. Run the daemon as the coordinator account; worker credentials stay
with the receiving process. See docs/FULL_CAPABILITY_EXECUTION.md for setup.
Before submission, inspect the selected endpoint from the coordinator account:
  ./conductor account-check --role coordinator
These checks do not connect, read credentials or launch models. A present socket
is not proof of a live or authenticated peer; runtime admission checks still apply.
Accepted input snapshots remain usable after daemon restart; this does not make
an interrupted unrestricted execution eligible for automatic continuation.
The launcher uses the packaging Python interpreter directly, independent of PATH.
Python and adapter executable paths are host-specific. Planning checks local
adapter hashes; receiving-account executable bytes are checked there before ACK.
Do not replace this bundle's binaries while its runtime is running.
See docs/LOCAL_OPERATOR.md and docs/DELIVERY_ACCEPTANCE.md for the full boundaries.
''')
    files = {str(p.relative_to(destination)): digest(p) for p in sorted(destination.rglob('*')) if p.is_file()}
    manifest = {'source_commit': commit, 'source_dirty': dirty, 'host_local_preview': True,
                'python_executable': str(interpreter), 'python_sha256': digest(interpreter),
                'adapter_source_sha256': digest(accepted_plan), 'files': files}
    (destination / 'manifest.json').write_text(json.dumps(manifest, indent=2) + '\n')
    return manifest


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--bin-dir', type=Path, required=True)
    parser.add_argument('--accepted-plan', type=Path, required=True)
    parser.add_argument('--destination', type=Path, required=True)
    args = parser.parse_args()
    os.umask(0o077)
    result = package(args.bin_dir, args.accepted_plan, args.destination)
    print(json.dumps({'destination': str(args.destination.absolute()), 'source_commit': result['source_commit'],
                      'source_dirty': result['source_dirty'], 'files': len(result['files'])}, indent=2))
