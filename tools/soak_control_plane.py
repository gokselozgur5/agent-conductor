#!/usr/bin/env python3
"""Isolated continuous control-plane exercise; never a full native/v1 acceptance."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import secrets
import signal
import socket
import sqlite3
import subprocess
import sys
import time
import urllib.request

SEVEN_DAYS = 7 * 24 * 3600
SCOPE = 'control_plane_only_no_native_workers_no_full_v1_acceptance'


class Failure(RuntimeError):
    """A fixed diagnostic code, never arbitrary daemon/vendor output."""


def require(condition, code):
    if not condition:
        raise Failure(code)


def digest(path):
    with path.open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest() if hasattr(hashlib, 'file_digest') else hashlib.sha256(stream.read()).hexdigest()


def atomic_json(path, value):
    temporary = path.with_suffix('.next')
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(descriptor, 'w', encoding='utf8') as stream:
        json.dump(value, stream, indent=2)
        stream.write('\n')
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temporary, path)
    directory = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(directory)
    finally:
        os.close(directory)


def validate_sample(sample, previous, limits):
    require(sample['healthy'], 'runtime_invariant_failure')
    require(sample['projection_matches'], 'board_projection_mismatch')
    require(sample['live_leases'] == 0, 'unexpected_live_lease')
    require(sample['executions'] == 0, 'unexpected_execution')
    require(sample['pending_deliveries'] == 0, 'delivery_queue_growth')
    require(sample['dispatch_count'] == 0, 'unexpected_dispatch')
    require(sample['rss_kib'] <= limits['rss_kib'], 'resident_memory_limit')
    require(sample['rss_kib'] <= limits['baseline_rss_kib'] + limits['growth_kib'], 'resident_memory_growth')
    require(sample['daemon_children'] == 0, 'unexpected_daemon_child')
    require(sample['state_bytes'] <= 2 * 1024**3, 'state_disk_limit')
    require(sample['log_bytes'] <= 16 * 1024**2, 'log_disk_limit')
    require(sample['stale_refused'], 'stale_epoch_mutation')
    require(sample['task_count'] == sample['cycle'] + 1, 'task_count_divergence')
    require(sample['event_count'] == sample['cycle'] + 1, 'event_count_divergence')
    if previous:
        require(sample['boot'] == previous['boot'], 'unexpected_daemon_restart')
        gap = sample['monotonic_s'] - previous['monotonic_s']
        wall_gap = sample['wall_s'] - previous['wall_s']
        require(0 < gap <= limits['max_gap_s'], 'observation_gap')
        require(abs(wall_gap - gap) <= 5, 'wall_clock_or_suspend_gap')


def completion_status(elapsed, duration):
    require(elapsed >= duration, 'incomplete_duration')
    return 'completed_control_plane_soak' if duration >= SEVEN_DAYS else 'completed_short_probe'


class Exercise:
    def __init__(self, args):
        self.args = args
        self.root = args.output.resolve()
        require(not self.root.exists(), 'output_already_exists')
        require(args.bin_dir.resolve().is_dir(), 'binary_directory_missing')
        require(args.socket_dir.resolve().is_dir(), 'socket_directory_missing')
        require(0 < args.duration_seconds <= SEVEN_DAYS, 'invalid_duration')
        require(.1 <= args.interval_seconds <= 300, 'invalid_interval')
        require(args.duration_seconds / args.interval_seconds <= 12000, 'excessive_cycle_count')
        require(1 <= args.max_rss_mib <= 4096, 'invalid_memory_limit')
        self.bins = {name: args.bin_dir.resolve() / name for name in ['conductor-daemon', 'conductorctl']}
        require(all(p.is_file() and os.access(p, os.X_OK) for p in self.bins.values()), 'binary_missing')
        self.hashes = {name: digest(path) for name, path in self.bins.items()}
        suffix = hashlib.sha256(str(self.root).encode()).hexdigest()[:16]
        self.sock = args.socket_dir.resolve() / ('soak-' + suffix + '.sock')
        require(len(os.fsencode(self.sock)) < 100 and not self.sock.exists(), 'socket_path_unavailable')
        self.root.mkdir(mode=0o700)
        self.token = secrets.token_hex(32)
        self.process = None
        self.log = None
        self.previous = None
        self.started = time.monotonic()
        self.started_wall = time.time()
        self.samples = 0
        self.limits = {'rss_kib': args.max_rss_mib * 1024, 'max_gap_s': args.interval_seconds * 3 + 10, 'growth_kib': 64 * 1024, 'baseline_rss_kib': 0}
        self.opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))

    def cli(self, *args, refuse=False):
        result = subprocess.run([str(self.bins['conductorctl']), '--socket', str(self.sock), '--json', *args], capture_output=True, timeout=10)
        if refuse:
            require(result.returncode != 0, 'expected_refusal_missing')
            body = json.loads(result.stdout)['result']
            require(body['status'] == 'error' and body['payload']['code'] == 'conflict' and 'attempt, worker, or epoch is stale' in body['payload']['message'], 'wrong_refusal')
            return
        require(result.returncode == 0, 'cli_request_failed')
        body = json.loads(result.stdout)
        require(body.get('result', {}).get('status') != 'error', 'cli_protocol_error')
        return body['result']['payload']

    def http(self, route):
        request = urllib.request.Request(self.url + route, headers={'Authorization': 'Bearer ' + self.token})
        with self.opener.open(request, timeout=10) as response:
            require(response.status == 200, 'http_request_failed')
            return json.load(response)

    def start(self):
        with socket.socket() as reservation:
            reservation.bind(('127.0.0.1', 0))
            port = reservation.getsockname()[1]
        self.url = 'http://127.0.0.1:' + str(port)
        self.log = (self.root / 'daemon.log').open('xb')
        env = {k: os.environ[k] for k in ['PATH', 'LANG', 'TMPDIR'] if k in os.environ}
        env['CONDUCTOR_OPERATOR_TOKEN'] = self.token
        self.process = subprocess.Popen([str(self.bins['conductor-daemon']), '--database', str(self.root / 'state.sqlite3'), '--socket', str(self.sock), '--lock', str(self.root / 'runtime.lock'), '--http', '127.0.0.1:' + str(port)], env=env, stdout=self.log, stderr=self.log, start_new_session=True)
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline:
            require(self.process.poll() is None, 'daemon_start_failed')
            try:
                if self.cli('health')['status'] == 'ready':
                    break
            except (Failure, OSError, ValueError):
                time.sleep(.05)
        else:
            raise Failure('daemon_readiness_deadline')
        self.cli('register-project', 'operator', 'project', 'soak-project', str(self.root))
        for name in ['producer', 'consumer', 'worker']:
            self.cli('register-agent', 'operator', name, name, 'synthetic', 'soak-v1', 'no-execution', 'read_write' if name == 'worker' else 'read_only')
        for agent in ['consumer', 'producer']:
            self.cli('subscribe-events', json.dumps(dict(schema_version=1, id='soak-' + agent, agent_id=agent, topic_filter='soak.tick', project_id='soak-project', path_prefix=None, echo_source=False)))
        atomic_json(self.root / 'run.json', dict(scope=SCOPE, full_v1_accepted=False, pid=self.process.pid, started_wall_s=time.time(), duration_s=self.args.duration_seconds, interval_s=self.args.interval_seconds, limits=self.limits, binary_sha256=self.hashes, binaries={k: str(v) for k, v in self.bins.items()}, socket=str(self.sock), url=self.url, script_sha256=digest(Path(__file__).resolve())))
        self.started = time.monotonic()
        self.started_wall = time.time()

    def task(self, name):
        return self.cli('get-task', name)['aggregate']

    def apply(self, name, kind, role='operator', refuse=False):
        task = self.task(name)
        command = dict(actor=dict(id='worker' if role == 'worker' else 'operator', role=role), expected_version=task['version'], correlation_id=name, idempotency_key=name + ':' + str(task['version']) + ':' + kind['type'], reason='Isolated control-plane endurance exercise', kind=kind)
        self.cli('apply', name, json.dumps(command), refuse=refuse)

    def lifecycle(self, cycle):
        name = 'soak-' + format(cycle, '08d')
        self.cli('create-task', 'operator', name, name, 'soak-project', 'Synthetic control-plane exercise; no executable worker')
        self.apply(name, {'type': 'mark_ready'})
        claim = dict(type='claim', attempt_id=name + ':1', worker='worker', lease_duration_ms=60000, worker_margin_ms=1000, recovery_grace_ms=1000)
        self.apply(name, claim, 'worker')
        self.apply(name, {'type': 'start', 'attempt_id': name + ':1', 'lease_epoch': 1}, 'worker')
        self.apply(name, {'type': 'pause'})
        self.apply(name, {'type': 'resume'})
        claim['attempt_id'] = name + ':2'
        self.apply(name, claim, 'worker')
        before = self.task(name)
        self.apply(name, {'type': 'start', 'attempt_id': name + ':1', 'lease_epoch': 1}, 'worker', refuse=True)
        require(self.task(name) == before, 'stale_epoch_changed_task')
        self.apply(name, {'type': 'cancel'})
        require(self.task(name)['state'] == 'cancelled', 'task_not_cancelled')
        return name

    def event(self, cycle):
        name = 'tick-' + str(cycle)
        event = dict(schema_version=1, id=name, idempotency_key=name, source_agent_id='producer', source_session_id='soak', correlation_id=name, causation_id=None, generation=0, topic='soak.tick', project_id='soak-project', task_id=None, attempt_id=None, lease_epoch=None, path_scope=None, payload={'cycle': cycle}, created_at_ms=int(time.time() * 1000))
        first = self.cli('publish-event', json.dumps(event))
        replay = self.cli('publish-event', json.dumps(event))
        require(not first['duplicate'] and first['fanout'] == 1 and replay['duplicate'], 'event_replay_divergence')
        delivery = self.cli('claim-event', 'consumer')['delivery']
        require(delivery is not None and delivery['event']['id'] == name, 'wrong_event_delivery')
        effect = dict(namespace='soak', key='last', expected_version=None if cycle == 0 else cycle - 1, value={'cycle': cycle})
        self.cli('ack-event', delivery['delivery_id'], 'consumer', delivery['claim_token'], json.dumps(effect))
        require(self.cli('claim-event', 'consumer')['delivery'] is None, 'duplicate_delivery')
        require(self.cli('claim-event', 'producer')['delivery'] is None, 'source_echo')

    def observe(self, cycle):
        require(self.process.poll() is None, 'daemon_exited')
        invariant = self.cli('check-invariants')
        http = self.http('/api/tasks')
        # Traverse every page: no independently stored board status is trusted.
        rows = []
        while True:
            rows.extend(http['tasks'])
            if http['next_after'] is None:
                break
            http = self.http('/api/tasks?after=' + http['next_after'])
        with sqlite3.connect('file:' + str(self.root / 'state.sqlite3') + '?mode=ro', uri=True) as db:
            expected = [(r[0], json.loads(r[1])) for r in db.execute('select id,aggregate_json from tasks order by id')]
            count = lambda sql: db.execute(sql).fetchone()[0]
            sample = dict(cycle=cycle, wall_s=time.time(), monotonic_s=time.monotonic(), healthy=invariant['healthy'], projection_matches=[(r['aggregate']['id'], r['aggregate']) for r in rows] == expected, live_leases=count("select count(*) from leases where status in ('active','recovery_pending')"), executions=count('select count(*) from executions'), pending_deliveries=count("select count(*) from event_deliveries where status!='acked'"), dispatch_count=count('select count(*) from workflow_dispatch'), task_count=count('select count(*) from tasks'), event_count=count('select count(*) from semantic_events'), stale_refused=True)
        allowed = {'state.sqlite3', 'state.sqlite3-wal', 'state.sqlite3-shm', 'runtime.lock', 'daemon.log', 'run.json', 'status.json', 'samples.jsonl'}
        files = list(self.root.iterdir())
        require(all(p.name in allowed and p.is_file() and not p.is_symlink() for p in files), 'unexpected_state_file')
        sample['state_bytes'] = sum(p.stat().st_size for p in files)
        sample['log_bytes'] = (self.root / 'daemon.log').stat().st_size
        sample['boot'] = self.cli('health')['boot_generation']
        stats = subprocess.run(['ps', '-p', str(self.process.pid), '-o', 'rss='], capture_output=True, text=True, timeout=5)
        require(stats.returncode == 0 and stats.stdout.strip().isdigit(), 'process_observation_failed')
        sample['rss_kib'] = int(stats.stdout.strip())
        inventory = subprocess.run(['ps', '-axo', 'ppid='], capture_output=True, text=True, timeout=5)
        require(inventory.returncode == 0, 'process_inventory_failed')
        sample['daemon_children'] = sum(int(value.strip()) == self.process.pid for value in inventory.stdout.splitlines() if value.strip())
        if self.samples == 0:
            self.limits['baseline_rss_kib'] = sample['rss_kib']
        if self.previous is None:
            require(sample['monotonic_s'] - self.started <= self.limits['max_gap_s'], 'initial_observation_gap')
            require(abs((sample['wall_s'] - self.started_wall) - (sample['monotonic_s'] - self.started)) <= 5, 'initial_clock_or_suspend_gap')
        validate_sample(sample, self.previous, self.limits)
        self.previous = sample
        with (self.root / 'samples.jsonl').open('a', encoding='utf8') as stream:
            stream.write(json.dumps(sample) + '\n')
            stream.flush()
            os.fsync(stream.fileno())
        self.samples += 1
        atomic_json(self.root / 'status.json', dict(status='running', scope=SCOPE, full_v1_accepted=False, cycles=self.samples, elapsed_s=time.monotonic() - self.started, limits=self.limits, latest=sample))

    def run(self):
        status = 'failed'
        error = None
        try:
            self.start()
            while time.monotonic() - self.started < self.args.duration_seconds:
                cycle_start = time.monotonic()
                self.lifecycle(self.samples)
                self.event(self.samples)
                self.observe(self.samples)
                time.sleep(max(0, min(self.args.interval_seconds - (time.monotonic() - cycle_start), self.args.duration_seconds - (time.monotonic() - self.started))))
            require(self.samples > 0, 'no_observations')
            tail_gap = time.monotonic() - self.previous['monotonic_s']
            require(tail_gap <= self.limits['max_gap_s'], 'final_observation_gap')
            require(abs((time.time() - self.previous['wall_s']) - tail_gap) <= 5, 'final_clock_or_suspend_gap')
            require(all(digest(p) == self.hashes[n] for n, p in self.bins.items()), 'binary_changed')
            require(self.process.poll() is None, 'daemon_exited')
            status = completion_status(time.monotonic() - self.started, self.args.duration_seconds)
        except (Exception, KeyboardInterrupt) as caught:
            error = str(caught) if isinstance(caught, Failure) else type(caught).__name__
        finally:
            if self.process is not None and self.process.poll() is None:
                self.process.send_signal(signal.SIGINT)
                try:
                    require(self.process.wait(timeout=15) == 0, 'daemon_stop_failed')
                except (Failure, subprocess.TimeoutExpired):
                    status, error = 'failed', 'daemon_stop_failed'
            if self.log:
                self.log.close()
            atomic_json(self.root / 'status.json', dict(status=status, scope=SCOPE, full_v1_accepted=False, error=error, cycles=self.samples, elapsed_s=time.monotonic() - self.started, binary_sha256=self.hashes, limits=self.limits))
        require(status != 'failed', error or 'exercise_failed')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--bin-dir', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--socket-dir', type=Path, required=True)
    parser.add_argument('--duration-seconds', type=float, default=SEVEN_DAYS)
    parser.add_argument('--interval-seconds', type=float, default=60)
    parser.add_argument('--max-rss-mib', type=int, default=512)
    args = parser.parse_args()
    def interrupted(_signal, _frame):
        raise Failure('monitor_interrupted')
    signal.signal(signal.SIGTERM, interrupted)
    os.umask(0o077)
    try:
        Exercise(args).run()
    except (Failure, OSError) as error:
        print(str(error) if isinstance(error, Failure) else type(error).__name__, file=sys.stderr)
        return 1
    return 0


if __name__ == '__main__':
    sys.exit(main())
