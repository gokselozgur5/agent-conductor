"""Cross-account clients keep protocol identity and never retry a mutation."""
import http.server
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import threading
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'tools'))
from conductor_rpc import send

TOKEN = 'synthetic-rpc-credential-000000000000'


class Handler(http.server.BaseHTTPRequestHandler):
    def log_message(self, *_):
        pass

    def do_POST(self):
        body = json.loads(self.rfile.read(int(self.headers['Content-Length'])))
        self.server.calls.append((self.path, self.headers['Authorization'], body))
        result = dict(protocol=body['protocol'], version=1,
                      request_message_id=body['message_id'], correlation_id=body['correlation_id'],
                      result=dict(status='error', payload=dict(code='conflict', message='stale version')))
        if self.server.mismatch:
            result['correlation_id'] = 'unrelated-request'
        data = json.dumps(result).encode()
        self.send_response(200)
        self.send_header('Content-Length', str(len(data)))
        self.end_headers()
        self.wfile.write(data)


class RpcTests(unittest.TestCase):
    def setUp(self):
        self.server = http.server.ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        self.server.calls = []
        self.server.mismatch = False
        self.thread = threading.Thread(target=self.server.serve_forever)
        self.thread.start()
        self.address = '127.0.0.1:' + str(self.server.server_port)

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join()

    def test_launcher_needs_no_local_state_and_preserves_errors_without_retry(self):
        launcher = Path(__file__).resolve().parents[1] / 'tools/conductor'
        payload = dict(type='register_agent', actor_id='operator', idempotency_key='one',
                       id='account-worker', adapter_kind='claude', protocol_fingerprint='v1',
                       capability_hash='full', access_mode='read_write')
        with tempfile.TemporaryDirectory() as directory:
            token = Path(directory) / 'token'
            token.write_text(TOKEN)
            token.chmod(0o600)
            result = subprocess.run([sys.executable, '-E', '-s', str(launcher), '--state',
                                     directory + '/absent', 'rpc', '--address', self.address,
                                     '--token-file', str(token)], input=json.dumps(payload),
                                    capture_output=True, text=True, timeout=10)
        self.assertEqual(result.returncode, 3, result.stderr)
        self.assertEqual(json.loads(result.stdout)['result']['payload']['code'], 'conflict')
        self.assertNotIn(TOKEN, result.stdout + result.stderr)
        self.assertEqual(len(self.server.calls), 1)
        path, auth, request = self.server.calls[0]
        self.assertEqual((path, auth), ('/api/rpc', 'Bearer ' + TOKEN))
        self.assertEqual(request['payload'], payload)

    def test_mismatched_response_is_refused_without_retry(self):
        self.server.mismatch = True
        with self.assertRaisesRegex(ValueError, 'does not match'):
            send(self.address, TOKEN, dict(type='health'))
        self.assertEqual(len(self.server.calls), 1)

    def test_nonlocal_destination_and_header_injection_are_refused_before_send(self):
        for address, token in [('192.0.2.1:80', TOKEN), (self.address, TOKEN + '\r\nX: bad')]:
            with self.assertRaises(ValueError):
                send(address, token, dict(type='health'))
        self.assertEqual(self.server.calls, [])
