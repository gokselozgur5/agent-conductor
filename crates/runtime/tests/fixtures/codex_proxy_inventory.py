"""Pinned Codex discovery against a local refusing provider; no model backend."""
import hashlib
import http.server
import json
import os
from pathlib import Path
import subprocess
import sys
import threading
import time

binary, proxy, destination = map(Path, sys.argv[1:])
assert hashlib.sha256(binary.read_bytes()).hexdigest() == '8813367285524ac798dd96d1b071aea30d58708c7226fd236cd275cf1cfdee31'
destination.mkdir(mode=0o700)
home = destination / 'home'; home.mkdir(mode=0o700)
work = destination / 'work'; work.mkdir(mode=0o700)
captured = []
ready = threading.Event()
mode = os.environ.get("CONDUCTOR_PROXY_CALLS", "inventory")
execute_calls = mode != "inventory"
authorized = mode == "approved"
outputs = []
capability = json.loads(os.environ["CONDUCTOR_BROKER_HANDOFF"])["token"].encode()


class Handler(http.server.BaseHTTPRequestHandler):
    def log_message(self, *_):
        pass

    def do_POST(self):
        size = int(self.headers.get('Content-Length', '0'))
        if not 0 < size < 8 * 1024 * 1024:
            self.send_error(413)
            return
        raw = self.rfile.read(size)
        assert capability not in raw, 'Capability leaked into model request'
        body = json.loads(raw)
        captured.append(body.get('tools', []))
        if not execute_calls:
            ready.set()
            self.send_response(503)
            self.end_headers()
            self.wfile.write(b'Local fixture has no inference backend.')
            return
        step = len(captured) - 1
        last_step = 2 if authorized else 1
        assert step <= last_step, 'Unexpected extra model turn'
        if step:
            call_id = 'call_write' if step == 1 else 'call_read'
            matches = [item for item in body.get('input', [])
                       if item.get('type') == 'function_call_output' and item.get('call_id') == call_id]
            assert len(matches) == 1, 'Missing actual tool result'
            outputs.append(matches[0]['output'])
            encoded = json.dumps(matches[0]['output'])
            expected = ('written' if step == 1 else 'native codex proxy') if authorized else 'requires approval'
            assert expected in encoded, 'Unexpected tool result'
        if step < last_step:
            item = {'type': 'function_call', 'id': 'fc_' + str(step),
                    'call_id': 'call_write' if step == 0 else 'call_read',
                    'namespace': 'mcp__conductor_files',
                    'name': 'conductor_write_text' if step == 0 else 'conductor_read_text',
                    'arguments': json.dumps({'path': 'codex-answer', 'contents': 'native codex proxy\n',
                                             'expected_sha256': None} if step == 0 else {'path': 'codex-answer'}),
                    'status': 'completed'}
        else:
            item = {'type': 'message', 'id': 'msg_fixture', 'role': 'assistant', 'status': 'completed',
                    'content': [{'type': 'output_text', 'text': 'Fixture done.', 'annotations': []}]}
        response_id = 'resp_fixture_' + str(step)
        response = {'id': response_id, 'object': 'response', 'status': 'completed', 'output': [item],
                    'usage': {'input_tokens': 1, 'output_tokens': 1, 'total_tokens': 2,
                              'input_tokens_details': {'cached_tokens': 0},
                              'output_tokens_details': {'reasoning_tokens': 0}}}
        events = [
            {'type': 'response.created', 'response': {**response, 'status': 'in_progress', 'output': []}},
            {'type': 'response.output_item.added', 'response_id': response_id, 'output_index': 0, 'item': item},
            {'type': 'response.output_item.done', 'response_id': response_id, 'output_index': 0, 'item': item},
            {'type': 'response.completed', 'response': response},
        ]
        payload = b''.join(('data: ' + json.dumps({**event, 'sequence_number': index}) + '\n\n').encode()
                           for index, event in enumerate(events))
        self.send_response(200)
        self.send_header('Content-Type', 'text/event-stream')
        self.send_header('Content-Length', str(len(payload)))
        self.end_headers(); self.wfile.write(payload); self.wfile.flush()
        if step == last_step:
            ready.set()


def toml(value):
    if isinstance(value, dict):
        return '{' + ','.join(json.dumps(k) + '=' + toml(v) for k, v in value.items()) + '}'
    if isinstance(value, list):
        return '[' + ','.join(toml(v) for v in value) + ']'
    return json.dumps(value)


server = http.server.HTTPServer(('127.0.0.1', 0), Handler)
thread = threading.Thread(target=server.serve_forever, daemon=True); thread.start()
config = {
    'model_provider': 'fixture', 'model': 'gpt-5.3-codex',
    'model_providers.fixture': {'name': 'local-fixture',
        'base_url': f'http://127.0.0.1:{server.server_port}/v1', 'wire_api': 'responses',
        'env_key': 'CONDUCTOR_FIXTURE_KEY', 'request_max_retries': 0, 'stream_max_retries': 0},
}
policy = json.loads(os.environ['CONDUCTOR_CODEX_BROKER_OPTIONS'])
assert isinstance(policy, list) and len(policy) == 14
assert all(isinstance(value, str) for value in policy)
assert policy[::2] == ['-c'] * 7
args = [str(binary), '--no-daemon', '-a', 'never']
for key, value in config.items():
    args += ['-c', key + '=' + toml(value)]
args += policy
if not authorized:
    for name in ('conductor_read_text', 'conductor_write_text'):
        args += ['-c', f'mcp_servers.conductor_files.tools.{name}.approval_mode="prompt"']
args += ['exec', '--ignore-user-config', '--ignore-rules', '--ephemeral',
         '--skip-git-repo-check', '--json', '-C', str(work),
         'Synthetic local fixture. Execute the supplied file tools, then finish.']
environment = {'HOME': str(home), 'CODEX_HOME': str(home), 'PATH': '/usr/bin:/bin',
    'TMPDIR': os.environ['TMPDIR'], 'CONDUCTOR_FIXTURE_KEY': 'synthetic-local-only',
    'CONDUCTOR_BROKER_HANDOFF': os.environ['CONDUCTOR_BROKER_HANDOFF']}
child = None
try:
    with (destination / 'stdout.log').open('wb') as out, (destination / 'stderr.log').open('wb') as err:
        child = subprocess.Popen(args, env=environment, stdout=out, stderr=err)
        deadline = time.monotonic() + 35
        while not ready.wait(.1):
            if child.poll() is not None or time.monotonic() > deadline:
                break
        if execute_calls and ready.is_set():
            child.wait(timeout=10)
finally:
    if child is not None and child.poll() is None:
        child.terminate()
        try:
            child.wait(timeout=5)
        except subprocess.TimeoutExpired:
            child.kill(); child.wait(timeout=5)
    server.shutdown(); server.server_close(); thread.join(timeout=5)
assert captured, 'Pinned Codex did not reach local fixture with its tool inventory'
(destination / 'tools.json').write_text(json.dumps(captured[0], indent=2) + '\n')


def names(value):
    if isinstance(value, dict):
        if isinstance(value.get('name'), str):
            yield value['name']
        for item in value.values():
            yield from names(item)
    elif isinstance(value, list):
        for item in value:
            yield from names(item)


found = list(names(captured[0]))
assert not any(name in ('exec_command', 'write_stdin') for name in found)
for tool in ('conductor_read_text', 'conductor_write_text'):
    assert sum(name.endswith(tool) for name in found) == 1, 'Expected broker tool missing or duplicated'
for log in ('stdout.log', 'stderr.log', 'tools.json'):
    assert capability not in (destination / log).read_bytes(), 'Capability leaked into fixture log'
receipt = {'pinned_codex': True, 'tool_call_mode': mode, 'capability_absent_from_model_requests_and_logs': True, 'real_proxy_initialized': True, 'broker_tools_discovered': 2,
           'command_tools_absent': True, 'model_backend_invoked': False,
           'file_tools_called': authorized, 'unapproved_call_refused': mode == 'denied', 'codex_child_stopped': child.poll() is not None}
if execute_calls:
    assert len(outputs) == (2 if authorized else 1) and child.returncode == 0
    (destination / 'tool-outputs.json').write_text(json.dumps(outputs, indent=2) + '\n')
(destination / 'receipt.json').write_text(json.dumps(receipt, indent=2) + '\n')
print(json.dumps(receipt))
