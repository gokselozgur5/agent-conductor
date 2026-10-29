#!/usr/bin/env python3
"""Run only the opt-in account test as root on an ephemeral Linux CI runner."""
import json
import os
from pathlib import Path
import subprocess
import sys


def main():
    if sys.platform != 'linux' or os.environ.get('GITHUB_ACTIONS') != 'true':
        raise SystemExit('This privileged fixture is restricted to Linux GitHub Actions')
    built = subprocess.run(
        ['cargo', 'test', '-p', 'conductor-runtime', '--test', 'account_cross_uid',
         '--no-run', '--message-format=json'], check=True, stdout=subprocess.PIPE, text=True)
    executables = []
    for line in built.stdout.splitlines():
        record = json.loads(line)
        if (record.get('reason') == 'compiler-artifact'
                and record['target']['name'] == 'account_cross_uid'
                and record.get('executable')):
            executables.append(Path(record['executable']).resolve(strict=True))
    if len(executables) != 1:
        raise SystemExit('Expected one compiled cross-UID integration test')
    # Do not forward credentials, Cargo configuration, or the caller's HOME.
    # RUNNER_TEMP can have private ancestors that correctly exclude nobody.
    # The Rust fixture creates its own owned directory here; never chmod runner HOME.
    temporary = Path('/tmp').resolve(strict=True)
    subprocess.run(
        ['sudo', '-n', '/usr/bin/env', '-i', 'PATH=/usr/bin:/bin',
         f'TMPDIR={temporary}', str(executables[0]), '--ignored', '--exact',
         'distinct_uid_worker_stages_executes_and_returns_exact_candidate', '--nocapture'],
        check=True, timeout=60)


if __name__ == '__main__':
    main()
