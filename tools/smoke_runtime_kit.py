#!/usr/bin/env python3
"""Boot an extracted runtime kit with an isolated empty project; no model calls."""
import argparse
import json
from pathlib import Path
import signal
import socket
import subprocess
import sys
import tempfile
import time

sys.dont_write_bytecode = True


def operator_ready(status):
    # The registration-only bootstrap also reports ready before exec of the
    # long-lived daemon. Wait for its configured watcher before signaling it.
    health = status['health']
    return health['status'] == 'ready' and health['repository_watcher'] == 'running'


def smoke(kit):
    from package_runtime import verify

    kit = kit.resolve(strict=True)
    verified = verify(kit)
    with tempfile.TemporaryDirectory(prefix='ac-kit-') as temporary:
        root = Path(temporary).resolve()
        project = root / 'project'
        project.mkdir()
        (project / 'README.md').write_text('Runtime kit smoke fixture.\n')
        subprocess.run(['git', 'init', '-q', str(project)], check=True)
        subprocess.run(['git', '-C', str(project), 'add', 'README.md'], check=True)
        subprocess.run(['git', '-C', str(project), '-c', 'user.name=Fixture',
                        '-c', 'user.email=fixture@example.invalid', '-c', 'commit.gpgsign=false',
                        'commit', '-qm', 'Runtime smoke fixture'], check=True)
        with socket.socket() as reservation:
            reservation.bind(('127.0.0.1', 0))
            port = reservation.getsockname()[1]
        cli = [sys.executable, '-E', '-s', str(kit / 'tools/conductor'),
               '--state', str(root / 'state')]
        subprocess.run([*cli, 'init', '--project', str(project), '--bin-dir', str(kit / 'bin'),
                        '--port', str(port)], capture_output=True, check=True, timeout=15)
        with (root / 'daemon.log').open('wb') as log:
            process = subprocess.Popen([*cli, 'run'], stdin=subprocess.DEVNULL,
                                       stdout=log, stderr=log)
            try:
                deadline = time.monotonic() + 30
                while True:
                    if process.poll() is not None:
                        raise RuntimeError('packaged runtime exited before readiness')
                    result = subprocess.run([*cli, 'status'], capture_output=True,
                                            text=True, timeout=15)
                    if result.returncode == 0:
                        status = json.loads(result.stdout)
                        if operator_ready(status):
                            assert status['tasks']['tasks'] == []
                            break
                    if time.monotonic() > deadline:
                        raise RuntimeError('packaged runtime readiness deadline')
                    time.sleep(.1)
            finally:
                if process.poll() is None:
                    process.send_signal(signal.SIGINT)
                try:
                    process.wait(timeout=20)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=10)
                    raise RuntimeError('packaged runtime did not stop gracefully') from None
            if process.returncode != 0:
                raise RuntimeError(f'packaged runtime exited with status {process.returncode}')
        assert subprocess.check_output(['git', '-C', str(project), 'status', '--porcelain']) == b''
    verify(kit)
    return dict(verified, health='ready', empty_tasks=True, graceful_stop=True,
                original_project_unchanged=True, model_calls=False)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--kit', type=Path, required=True)
    print(json.dumps(smoke(parser.parse_args().kit), indent=2))
