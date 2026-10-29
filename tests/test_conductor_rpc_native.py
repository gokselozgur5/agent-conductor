"""Actual daemon persistence through the packaged operator's RPC command."""
import json
import os
from pathlib import Path
import re
import signal
import sqlite3
import subprocess
import sys
import tempfile
import time
import unittest


class NativeRpcTests(unittest.TestCase):
    def test_registration_is_idempotent_and_survives_controller_restart(self):
        binary = Path(os.environ.get('CONDUCTOR_TEST_BIN_DIR', 'target/debug')) / 'conductor-daemon'
        if not binary.is_file():
            self.skipTest('build conductor-daemon or set CONDUCTOR_TEST_BIN_DIR')
        launcher = Path(__file__).resolve().parents[1] / 'tools/conductor'
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            token = root / 'token'
            token.write_text('synthetic-native-rpc-credential-00000000')
            token.chmod(0o600)
            request = dict(type='register_agent', actor_id='operator', idempotency_key='rpc-worker',
                           id='rpc-worker', adapter_kind='claude', protocol_fingerprint='v1',
                           capability_hash='full', access_mode='read_write')
            for boot in range(2):
                with (root / f'daemon-{boot}.log').open('w+') as log:
                    daemon = subprocess.Popen([str(binary.resolve()), '--database', str(root / 'db'),
                                               '--socket', str(root / 'ctl.sock'), '--lock', str(root / 'lock'),
                                               '--http', '127.0.0.1:0'], stdout=log, stderr=log,
                                              env={**os.environ, 'CONDUCTOR_OPERATOR_TOKEN': token.read_text()})
                    try:
                        address = self.wait_ready(daemon, log)
                        first = self.call(launcher, root, token, address, request)
                        second = self.call(launcher, root, token, address, request)
                        self.assertEqual(first['result'], second['result'])
                        self.assertEqual(first['result']['payload']['id'], 'rpc-worker')
                        inventory = self.call(launcher, root, token, address,
                                              dict(type='inventory', resource='agents', after=None))
                        self.assertEqual([x['id'] for x in inventory['result']['payload']['entries']], ['rpc-worker'])
                    finally:
                        if daemon.poll() is None:
                            daemon.send_signal(signal.SIGINT)
                        self.assertEqual(daemon.wait(timeout=15), 0)
            with sqlite3.connect(root / 'db') as database:
                self.assertEqual(database.execute('select count(*) from agents').fetchone()[0], 1)
                self.assertEqual(database.execute('pragma integrity_check').fetchone()[0], 'ok')

    def wait_ready(self, daemon, log):
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline:
            self.assertIsNone(daemon.poll(), 'daemon exited during startup')
            match = re.search(r'Operator board: http://(127\.0\.0\.1:\d+)', Path(log.name).read_text())
            if match:
                return match.group(1)
            time.sleep(.05)
        self.fail('daemon startup deadline')

    def call(self, launcher, root, token, address, payload):
        result = subprocess.run([sys.executable, '-E', '-s', str(launcher), '--state',
                                 str(root / 'unavailable-state'), 'rpc', '--address', address,
                                 '--token-file', str(token)], input=json.dumps(payload),
                                capture_output=True, text=True, timeout=15)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertNotIn(token.read_text(), result.stdout + result.stderr)
        return json.loads(result.stdout)
