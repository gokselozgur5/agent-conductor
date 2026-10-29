"""Pinned Codex discovery against a local refusing provider; no model backend."""
import hashlib
import struct
import zlib
import http.server
import json
import os
import shlex
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
command_probe = mode == "command-denied"
command_marker = destination / "forbidden-command-marker"
image_probe = mode in ("image-inside", "image-outside", "image-symlink")
broker_probe = mode in ("broker-read-outside", "broker-write-outside",
                        "broker-read-traversal", "broker-write-traversal")
broker_read = mode in ("broker-read-outside", "broker-read-traversal")
outside_canary = b"SYNTHETIC_OUTSIDE_READ_MUST_NOT_REACH_MODEL"
outputs = []
capability = json.loads(os.environ["CONDUCTOR_BROKER_HANDOFF"])["token"].encode()


def contains_image(value):
    if isinstance(value, dict):
        return value.get('type') in ('input_image', 'image_url') or any(contains_image(v) for v in value.values())
    if isinstance(value, list):
        return any(contains_image(v) for v in value)
    return False


def write_canary(path):
    def chunk(kind, data):
        return struct.pack('!I', len(data)) + kind + data + struct.pack('!I', zlib.crc32(kind + data))
    data = b'\x89PNG\r\n\x1a\n' + chunk(b'IHDR', struct.pack('!2I5B', 2, 2, 8, 2, 0, 0, 0))
    data += chunk(b'IDAT', zlib.compress(b'\x00\xff\x00\x00\x00\xff\x00' * 2)) + chunk(b'IEND', b'')
    path.write_bytes(data)


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
        assert outside_canary not in raw, 'Outside file contents reached model request'
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
            if command_probe:
                assert any(word in encoded.lower() for word in ('unknown', 'unsupported', 'unrecognized', 'not found')), 'Command did not receive explicit dispatch refusal'
                assert not command_marker.exists(), 'Disabled command executed'
            elif broker_probe:
                assert '-32602: invalid file operation' in encoded.lower(), 'Outside path was not rejected by broker request validation'
            elif image_probe:
                image_present = contains_image(body.get('input', []))
                assert image_present == (mode == 'image-inside'), 'Image access boundary violated'
                if mode != 'image-inside':
                    assert any(word in encoded.lower() for word in ('denied', 'outside', 'not allowed', 'permission')), 'Not an explicit permission refusal'
            else:
                assert expected in encoded, 'Unexpected tool result'
        if step < last_step:
            item = {'type': 'function_call', 'id': 'fc_' + str(step),
                    'call_id': 'call_write' if step == 0 else 'call_read',
                    'namespace': 'mcp__conductor_files',
                    'name': 'conductor_write_text' if step == 0 else 'conductor_read_text',
                    'arguments': json.dumps({'path': 'codex-answer', 'contents': 'native codex proxy\n',
                                             'expected_sha256': None} if step == 0 else {'path': 'codex-answer'}),
                    'status': 'completed'}
            if broker_probe:
                writing = not broker_read
                item['name'] = 'conductor_write_text' if writing else 'conductor_read_text'
                arguments = {'path': broker_path}
                if writing:
                    arguments.update(contents='forbidden outside write', expected_sha256=None)
                item['arguments'] = json.dumps(arguments)
            if command_probe:
                item.pop('namespace')
                item['name'] = 'exec_command'
                item['arguments'] = json.dumps({'cmd': 'printf forbidden > ' + shlex.quote(str(command_marker))})
            if image_probe:
                item.pop('namespace')
                item['name'] = 'view_image'
                item['arguments'] = json.dumps({'path': str(image_path)})
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
arguments = json.loads(os.environ['CONDUCTOR_CODEX_BROKER_ARGUMENTS'])
assert isinstance(arguments, list) and all(isinstance(value, str) for value in arguments)
exec_index = arguments.index('exec')
if broker_probe:
    outside_target = destination / 'outside-target.txt'
    task_root = Path(arguments[arguments.index('-C') + 1])
    broker_path = (os.path.relpath(outside_target, task_root)
                   if mode.endswith('-traversal') else str(outside_target))
    if mode.endswith('-traversal'):
        assert broker_path.startswith('../')
    if broker_read:
        outside_target.write_bytes(outside_canary)
    else:
        assert not outside_target.exists()
if image_probe:
    task_root = Path(arguments[arguments.index('-C') + 1])
    image_path = (task_root if mode == 'image-inside' else destination) / 'canary.png'
    write_canary(image_path)
    if mode == 'image-symlink':
        link = task_root / 'outside-image-link.png'
        link.symlink_to(image_path)
        image_path = link
args = [str(binary)]
for key, value in config.items():
    args += ['-c', key + '=' + toml(value)]
args += arguments[:exec_index]
if not (authorized or broker_probe):
    for name in ('conductor_read_text', 'conductor_write_text'):
        args += ['-c', f'mcp_servers.conductor_files.tools.{name}.approval_mode="prompt"']
args += arguments[exec_index:]
environment = {'HOME': str(home), 'CODEX_HOME': str(home), 'PATH': '/usr/bin:/bin',
    'NO_PROXY': '127.0.0.1,localhost',
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
if command_probe:
    assert not command_marker.exists(), 'Disabled command created a file'
if broker_probe:
    if broker_read:
        assert outside_target.read_bytes() == outside_canary
    else:
        assert not outside_target.exists(), 'Outside file was created'
    for log in ('stdout.log', 'stderr.log', 'tools.json'):
        assert outside_canary not in (destination / log).read_bytes()
receipt = {'pinned_codex': True, 'tool_call_mode': mode, 'capability_absent_from_model_requests_and_logs': True, 'real_proxy_initialized': True, 'broker_tools_discovered': 2,
           'command_tools_absent': True, 'forced_command_refused': command_probe, 'model_backend_invoked': False,
           'broker_path_denial': mode if broker_probe else None, 'image_access_control': mode if image_probe else None, 'file_tools_called': authorized or broker_probe, 'unapproved_call_refused': mode == 'denied', 'codex_child_stopped': child.poll() is not None}
if execute_calls:
    assert len(outputs) == (2 if authorized else 1) and child.returncode == 0
    (destination / 'tool-outputs.json').write_text(json.dumps(outputs, indent=2) + '\n')
(destination / 'receipt.json').write_text(json.dumps(receipt, indent=2) + '\n')
print(json.dumps(receipt))
