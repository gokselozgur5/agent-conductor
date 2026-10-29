"""Deterministic local Responses provider. Never launches a vendor process."""
import http.server
import json
from pathlib import Path
import sys
import subprocess
import threading
import time

home = Path(sys.argv[1])
home.mkdir(mode=0o700)
delay = int(sys.argv[3])
assert delay in (0, 35)


class Handler(http.server.BaseHTTPRequestHandler):
    step = 0

    def log_message(self, *_):
        pass

    def do_POST(self):
        size = int(self.headers.get('Content-Length', '0'))
        assert 0 < size < 8 * 1024 * 1024
        raw = self.rfile.read(size)
        step = Handler.step
        assert step <= 2
        (home / f'request-{step}.json').write_bytes(raw)
        if step == 1 and delay:
            started = time.monotonic()
            time.sleep(delay)
            (home / 'idle-delay.json').write_text(json.dumps({'elapsed_seconds': time.monotonic() - started}))
        body = json.loads(raw)
        if step:
            outputs = [v for v in body['input'] if v.get('type') == 'function_call_output'
                       and v.get('call_id') == f'call_{step - 1}']
            assert len(outputs) == 1
            assert ('written' if step == 1 else 'native owned codex') in json.dumps(outputs[0])
        if step < 2:
            item = {'type': 'function_call', 'id': f'fc_{step}', 'call_id': f'call_{step}',
                    'namespace': 'mcp__conductor_files',
                    'name': 'conductor_write_text' if step == 0 else 'conductor_read_text',
                    'arguments': json.dumps({'path': 'owned-answer', 'contents': 'native owned codex\n',
                        'expected_sha256': None} if step == 0 else {'path': 'owned-answer'}),
                    'status': 'completed'}
        else:
            item = {'type': 'message', 'id': 'msg_done', 'role': 'assistant', 'status': 'completed',
                    'content': [{'type': 'output_text', 'text': 'Fixture done.', 'annotations': []}]}
        response_id = f'resp_{step}'
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
        payload = b''.join(('data: ' + json.dumps({**event, 'sequence_number': i}) + '\n\n').encode()
                           for i, event in enumerate(events))
        Handler.step += 1
        self.send_response(200)
        self.send_header('Content-Type', 'text/event-stream')
        self.send_header('Content-Length', str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)
        self.wfile.flush()


server = http.server.HTTPServer(('127.0.0.1', 0), Handler)
# Test-only launcher preserves the process identity with exec. Production ignores
# user configuration, so inject only local provider options before trusted args.
configuration = {
    'model_provider': 'fixture', 'model': 'gpt-5.3-codex',
    'model_providers.fixture': {'name': 'local-fixture',
        'base_url': f'http://127.0.0.1:{server.server_port}/v1', 'wire_api': 'responses',
        'env_key': 'LANG', 'request_max_retries': 0, 'stream_max_retries': 0},
}

def toml(value):
    if isinstance(value, dict):
        return '{' + ','.join(json.dumps(k) + '=' + toml(v) for k, v in value.items()) + '}'
    return json.dumps(value)

options = [part for key, value in configuration.items() for part in ('-c', key + '=' + toml(value))]
binary = str(Path(sys.argv[2]).resolve())
launcher = home / 'launch-codex'
launcher.write_text('#!/usr/bin/python3\nimport os, sys\nopen(' + repr(str(home / 'vendor-pid')) + ', \"w\").write(str(os.getpid()))\nos.execve(' + repr(binary) + ', ['
                    + repr(binary) + '] + ' + repr(options) + ' + sys.argv[1:], os.environ)\n')
launcher.chmod(0o700)
def observe_group():
    pid_file = home / 'vendor-pid'
    while not pid_file.exists():
        time.sleep(.02)
    while not pid_file.read_text().strip():
        time.sleep(.02)
    group = int(pid_file.read_text())
    previous = None
    with (home / 'process-group.jsonl').open('w') as log:
        while True:
            rows = subprocess.check_output(['/bin/ps', '-axo', 'pid,ppid,pgid,stat,comm'], text=True)
            members = [line.split(None, 4) for line in rows.splitlines()[1:]
                       if len(line.split(None, 4)) == 5 and line.split(None, 4)[2] == str(group)]
            if members != previous:
                log.write(json.dumps({'time': time.monotonic(), 'group': group, 'members': members}) + '\n')
                log.flush()
                previous = members
            time.sleep(.02)

threading.Thread(target=observe_group, daemon=True).start()
print('ready', flush=True)
server.serve_forever()
