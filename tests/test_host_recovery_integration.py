"""Actual native identity/quarantine; positive reboot transitions are simulated."""
import json
import os
from pathlib import Path
import signal
import socket
import subprocess
import sys
import tempfile
import time
import unittest
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'tools'))
import disaster_recovery
import recovery_authority


@unittest.skipUnless(os.environ.get('CONDUCTOR_TEST_BIN_DIR'), 'requires built Rust operator binaries')
class HostRecoveryIntegration(unittest.TestCase):
    def test_native_quarantine_and_simulated_reboot_restore(self):
        repo = Path(__file__).resolve().parents[1]
        bins = Path(os.environ['CONDUCTOR_TEST_BIN_DIR'])
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary).resolve()
            state, project = root / 'state', root / 'project'
            project.mkdir()
            (project / 'README.md').write_text('Recovery fixture\n')
            for args in [['init', '-q'], ['add', '.'], ['-c', 'user.name=Fixture', '-c', 'user.email=fixture@example.invalid',
                          '-c', 'commit.gpgsign=false', '-c', 'core.hooksPath=/dev/null', 'commit', '-qm', 'fixture']]:
                subprocess.run(['git', '-C', str(project), *args], check=True, capture_output=True)
            with socket.socket() as available:
                available.bind(('127.0.0.1', 0)); port = available.getsockname()[1]
            command = [sys.executable, str(repo / 'tools/conductor'), '--state', str(state)]
            def call(*args, success=True):
                result = subprocess.run(command + list(args), capture_output=True, text=True, timeout=60)
                self.assertEqual(result.returncode == 0, success, result.stderr)
                return result
            epoch = json.loads(subprocess.check_output([str(bins / 'conductor-daemon'), '--host-epoch',
                                                        '--database', str(root / 'unused/db.sqlite3')], text=True))
            recovery_authority.validate_host(epoch)
            self.assertFalse((root / 'unused').exists())
            call('init', '--project', str(project), '--bin-dir', str(bins), '--port', str(port))
            process = orphan = None
            def start():
                child = subprocess.Popen(command + ['run'], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                try:
                    for _ in range(100):
                        self.assertIsNone(child.poll())
                        result = subprocess.run(command + ['status'], capture_output=True, text=True, timeout=5)
                        if result.returncode == 0:
                            health = json.loads(result.stdout)['health']
                            # The launcher's temporary bootstrap also reports ready.
                            # Wait for the final daemon with observation enabled.
                            if health['status'] == 'ready' and health['repository_watcher'] == 'running':
                                return child
                        time.sleep(.1)
                    self.fail('readiness timeout')
                except BaseException:
                    if child.poll() is None: child.send_signal(signal.SIGINT); child.wait(timeout=15)
                    raise
            def stop(child):
                child.send_signal(signal.SIGINT); self.assertEqual(child.wait(timeout=15), 0)
            try:
                process = start(); stop(process); process = None
                exported = root / 'snapshot'
                call('backup', str(exported))
                # A surviving synthetic writer remains alive when same-boot recovery
                # is refused. Preparation must not mistake daemon absence for a fence.
                orphan_file = state / 'artifacts/orphan-observation'
                orphan = subprocess.Popen([sys.executable, '-c',
                    'import pathlib,sys,time\np=pathlib.Path(sys.argv[1]);t=p.with_suffix(".pending")\nwhile True:\n t.write_text(str(time.monotonic_ns()));t.replace(p)\n time.sleep(.05)',
                    str(orphan_file)], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                for _ in range(100):
                    if orphan_file.exists(): break
                    time.sleep(.01)
                before = orphan_file.read_text()
                retained = root / 'retained'
                call('prepare-recovery', str(exported), '--retained', str(retained))
                failed = call('recover-state', success=False)
                self.assertIn('host restart required', failed.stderr)
                time.sleep(.1)
                self.assertIsNone(orphan.poll())
                self.assertNotEqual(orphan_file.read_text(), before)
                rejected = subprocess.run([str(bins / 'conductor-daemon'), '--database', str(state / 'state.sqlite3'),
                    '--socket', str(root / 'never-created/control.sock'), '--lock', str(root / 'never-created/runtime.lock')],
                    capture_output=True, text=True, timeout=15)
                self.assertNotEqual(rejected.returncode, 0)
                self.assertIn('quarantined', rejected.stderr)
                self.assertFalse((root / 'never-created').exists())
                call('cancel-recovery')
                orphan.terminate(); orphan.wait(timeout=10); orphan = None
                # The positive boundary below is injected only into this test's
                # Python call. Production has no boot override flag or environment.
                for failure in ('missing', 'corrupt'):
                    process = start(); stop(process); process = None
                    exported = root / ('snapshot-' + failure)
                    retained = root / ('retained-' + failure)
                    call('backup', str(exported))
                    if failure == 'missing':
                        state.rename(root / 'independent-retained-history')
                    else:
                        (state / 'state.sqlite3').write_bytes(b'retain corrupt database')
                    call('prepare-recovery', str(exported), '--retained', str(retained))
                    self.assertIn('host restart required', call('recover-state', success=False).stderr)
                    if failure == 'missing':
                        rejected = subprocess.run([str(bins / 'conductor-daemon'), '--database', str(state / 'state.sqlite3'),
                            '--socket', str(state / 'control.sock'), '--lock', str(state / 'runtime.lock')], capture_output=True, text=True, timeout=15)
                        self.assertNotEqual(rejected.returncode, 0)
                        self.assertFalse(state.exists())
                    synthetic = dict(epoch, boot_id='0' * 64 if epoch['boot_id'] != '0' * 64 else '1' * 64)
                    with mock.patch.object(disaster_recovery, 'host_epoch', return_value=synthetic):
                        self.assertTrue(disaster_recovery.finish(state)['recovered'])
                    if failure == 'corrupt':
                        self.assertEqual((retained / 'state.sqlite3').read_bytes(), b'retain corrupt database')
                    process = start()
                    self.assertTrue(json.loads(call('ctl', '--json', 'check-invariants').stdout)['result']['payload']['healthy'])
                    stop(process); process = None
            finally:
                if orphan is not None and orphan.poll() is None: orphan.terminate(); orphan.wait(timeout=10)
                if process is not None and process.poll() is None: stop(process)


if __name__ == '__main__':
    unittest.main()
