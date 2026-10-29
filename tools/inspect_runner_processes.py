#!/usr/bin/env python3
"""Read-only, racy process/SQLite correlation; never authority to signal a PID."""
import argparse
import json
import os
from pathlib import Path
import re
import selectors
import sqlite3
import subprocess
import sys
import time
import uuid

from process_birth import observe as process_birth
from auxiliary_process_inventory import parse_tag as parse_auxiliary_tag, inventory as auxiliary_inventory

TAG = re.compile(r'--conductor-owner=v1:([0-9a-f]{64}):([0-9a-f]{64}):([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})\Z')


def ps(arguments, timeout=5):
    """Bound both bytes and time; never save or return raw command lines externally."""
    process = subprocess.Popen(['/bin/ps', *arguments], stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
    output = bytearray()
    deadline = time.monotonic() + timeout
    try:
        with selectors.DefaultSelector() as selector:
            selector.register(process.stdout, selectors.EVENT_READ)
            while True:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise ValueError('process_observation_timeout')
                if not selector.select(remaining):
                    raise ValueError('process_observation_timeout')
                chunk = os.read(process.stdout.fileno(), 65536)
                if not chunk:
                    break
                output.extend(chunk)
                if len(output) > 4 * 1024 * 1024:
                    raise ValueError('process_observation_too_large')
        code = process.wait(timeout=max(.01, deadline - time.monotonic()))
        return code, output.decode('utf8', errors='replace')
    finally:
        if process.poll() is None:
            process.kill()  # Only this tool's own ps child, never a listed process.
            process.wait()
        process.stdout.close()


def parse_tag(command):
    candidates = [word for word in command.split() if word.startswith('--conductor-owner=')]
    if len(candidates) != 1:
        return None
    match = TAG.fullmatch(candidates[0])
    return match.groups() if match else None


def pid_field(text):
    if not text.isascii() or not text.isdigit() or len(text) > 10:
        return None
    value = int(text)
    return value if 0 < value <= 2**31 - 1 else None


def runner_pids(inventory):
    result = set()
    for line in inventory.splitlines():
        fields = line.strip().split(None, 1)
        if len(fields) == 2 and Path(fields[1]).name == 'conductor-runner':
            pid = pid_field(fields[0])
            if pid is not None:
                result.add(pid)
    if len(result) > 128:
        raise ValueError('too_many_runner_observations')
    return sorted(result)


def discover_runner_pids(names, arguments):
    # Host argv is only a bounded candidate hint. Never trust its line structure,
    # tag or process name as a binding; each PID gets an independent fresh read.
    result = set(runner_pids(names))
    for line in arguments.splitlines():
        fields = line.strip().split(None, 1)
        if len(fields) == 2:
            pid = pid_field(fields[0])
            if pid is not None and (parse_tag(fields[1]) is not None or parse_auxiliary_tag(fields[1]) is not None):
                result.add(pid)
                if len(result) > 128:
                    raise ValueError('too_many_runner_observations')
    return sorted(result)


def bounded_column(name, limit):
    # Only fixed local column names reach this helper; malformed stored values
    # become a small invalid sentinel before being materialized in Python.
    return f"CASE WHEN {name} IS NULL THEN NULL WHEN typeof({name})='text' AND length({name})<={limit} THEN {name} ELSE '' END"


def expected_binding(expected, observed):
    if expected is None:
        return 'legacy_unavailable'
    try:
        parsed = uuid.UUID(expected)
        if parsed.int == 0 or str(parsed) != expected:
            return 'invalid_recorded_identity'
    except (ValueError, TypeError, AttributeError):
        return 'invalid_recorded_identity'
    return 'matching_observation' if expected == observed else 'identity_mismatch'


def correlate(db, authority, observations):
    result = []
    columns = {row[1] for row in db.execute('pragma table_info(executions)')}
    process_column = bounded_column('process_json', 1024) if 'process_json' in columns else 'NULL'
    expected_column = bounded_column('expected_wrapper_identity', 36) if 'expected_wrapper_identity' in columns else 'NULL'
    for pid, tag in observations:
        if tag[0] != authority:
            continue
        rows = db.execute('select task_id,attempt_id,lease_epoch,state,wrapper_identity,' + process_column + ',' + expected_column + ' from executions where packet_sha256=? limit 2', (tag[1],)).fetchall()
        item = dict(pid=pid, packet_sha256=tag[1], wrapper_identity=tag[2])
        if not rows:
            item['classification'] = 'no_matching_record'
        elif len(rows) != 1:
            item['classification'] = 'ambiguous_records'
        else:
            task, attempt, epoch, state, wrapper = rows[0][:5]
            item['_recorded_process'] = rows[0][5]
            binding = expected_binding(rows[0][6], tag[2])
            item['expected_wrapper_binding'] = binding
            item.update(task_id=task, attempt_id=attempt, lease_epoch=str(epoch), execution_state=state)
            if binding == 'invalid_recorded_identity':
                item['classification'] = 'invalid_expected_wrapper_identity'
            elif binding == 'identity_mismatch':
                item['classification'] = 'expected_wrapper_identity_mismatch'
            elif wrapper is None and binding == 'matching_observation' and state != 'prepared':
                item['classification'] = 'fenced_record_process_observed' if state == 'fenced' else 'unacknowledged_expected_wrapper'
            elif wrapper is None and state == 'prepared':
                item['classification'] = 'prepared_not_acknowledged'
            elif wrapper != tag[2]:
                item['classification'] = 'wrapper_identity_mismatch'
            elif state == 'fenced':
                item['classification'] = 'fenced_record_process_observed'
            else:
                item['classification'] = 'matching_record'
        result.append(item)
    return result


def process_binding(raw, pid, start, host):
    if raw is None:
        return 'legacy_unavailable'
    try:
        if not isinstance(raw, str) or len(raw) > 1024:
            return 'invalid_recorded_process'
        value = json.loads(raw)
        if not isinstance(value, dict) or set(value) != {'platform', 'host_id', 'boot_id', 'pid', 'start_marker'}:
            return 'invalid_recorded_process'
        if type(value['pid']) is not int or not 0 < value['pid'] <= 2**31 - 1 or value['platform'] not in ('linux', 'macos'):
            return 'invalid_recorded_process'
        if any(not isinstance(value[k], str) or not re.fullmatch('[0-9a-f]{64}', value[k]) for k in ('host_id', 'boot_id')):
            return 'invalid_recorded_process'
        if value['pid'] != pid:
            return 'pid_mismatch'
        if value['start_marker'] != start:
            return 'kernel_marker_mismatch'
        if host is None:
            return 'host_boot_unchecked'
        if any(value[k] != host[k] for k in ('platform', 'host_id', 'boot_id')):
            return 'host_boot_mismatch'
        return 'matching_observation'
    except (ValueError, TypeError, KeyError):
        return 'invalid_recorded_process'


def recorded_observations(db, authority, host, deadline):
    """Observe stored PIDs separately; never manufacture a missing observed tag."""
    if host is None:
        return dict(status='host_reader_required', observations=[])
    columns = {row[1] for row in db.execute('pragma table_info(executions)')}
    if 'process_json' not in columns:
        return dict(status='legacy_unavailable', observations=[])
    rows = db.execute('select ' + bounded_column('process_json', 1024) + ',' +
                      bounded_column('packet_sha256', 64) + ',' +
                      bounded_column('wrapper_identity', 36) +
                      ' from executions where process_json is not null limit 4097').fetchall()
    if len(rows) > 4096:
        raise ValueError('too_many_recorded_processes')
    candidates = []
    invalid = 0
    foreign = 0
    for raw, packet, wrapper in rows:
        try:
            value = json.loads(raw)
            pid, start = value['pid'], value['start_marker']
            binding = process_binding(raw, pid, start, host)
            if binding == 'host_boot_mismatch':
                foreign += 1
                continue
            pattern = r'linux-start-ticks:[0-9]{1,20}' if value['platform'] == 'linux' else r'macos-start-us:[0-9]{1,20}:[0-9]{1,6}'
            if binding != 'matching_observation' or not isinstance(start, str) or not re.fullmatch(pattern, start):
                raise ValueError('invalid')
            if not isinstance(packet, str) or not re.fullmatch('[0-9a-f]{64}', packet) or expected_binding(wrapper, wrapper) != 'matching_observation':
                raise ValueError('invalid')
        except (ValueError, TypeError, KeyError):
            invalid += 1
            continue
        candidates.append((pid, start, packet, wrapper))
    if len(candidates) > 128:
        raise ValueError('too_many_recorded_candidates')
    result = []
    for pid, start, packet, wrapper in candidates:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise ValueError('inventory_deadline')
        item = dict(recorded_pid=pid, recorded_packet_sha256=packet,
                    recorded_wrapper_identity=wrapper)
        before = process_birth(pid)
        if before is None:
            item['classification'] = 'recorded_process_unavailable'
        elif before != start:
            item['classification'] = 'recorded_pid_reused_or_marker_mismatch'
        else:
            code, command = ps(['-ww', '-p', str(pid), '-o', 'args='], min(5, remaining))
            after = process_birth(pid)
            if after != before:
                item['classification'] = 'recorded_process_changed_during_read'
            elif code:
                item['classification'] = 'recorded_arguments_unavailable'
            else:
                tag = parse_tag(command)
                item['classification'] = ('recorded_process_tag_absent_or_invalid' if tag is None else
                                          'recorded_process_tag_matches' if tag == (authority, packet, wrapper) else
                                          'recorded_process_tag_mismatch')
                item['process_start_observation'] = before
                item['start_observation_unchanged'] = True
        result.append(item)
    if time.monotonic() > deadline:
        raise ValueError('inventory_deadline')
    return dict(status='observed', invalid_records=invalid, other_host_boot_records=foreign,
                observations=result)


def inspect(database, host=None):
    database = database.resolve(strict=True)
    deadline = time.monotonic() + 10
    code, inventory = ps(['-axo', 'pid=,comm='])
    if code:
        raise ValueError('process_inventory_failed')
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise ValueError('inventory_deadline')
    code, argument_hints = ps(['-ww', '-axo', 'pid=,args='], min(5, remaining))
    if code:
        raise ValueError('process_discovery_failed')
    named_candidates = set(runner_pids(inventory))
    candidates = discover_runner_pids(inventory, argument_hints)
    del inventory, argument_hints  # Never retain raw host command lines in reports.
    observations = []
    auxiliary = []
    unscoped = 0
    unconfirmed = 0
    vanished = 0
    changed = 0
    births = {}
    for pid in candidates:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise ValueError('inventory_deadline')
        before = process_birth(pid)
        if before is None:
            vanished += 1
            continue
        code, command = ps(['-ww', '-p', str(pid), '-o', 'args='], min(5, remaining))
        if code:
            vanished += 1
            continue
        after = process_birth(pid)
        if after != before:
            changed += 1
            continue
        tag = parse_tag(command)
        if tag is None:
            auxiliary_tag = parse_auxiliary_tag(command)
            if auxiliary_tag is not None:
                auxiliary.append((pid, auxiliary_tag, before))
                continue
            if pid in named_candidates:
                unscoped += 1
            else:
                unconfirmed += 1
        else:
            observations.append((pid, tag))
            births[pid] = before
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise ValueError('inventory_deadline')
    with sqlite3.connect(database.as_uri() + '?mode=ro', uri=True, timeout=remaining) as db:
        db.set_progress_handler(lambda: int(time.monotonic() > deadline), 10000)
        db.execute('begin')
        row = db.execute('select a.authority_id,r.boot_generation from recovery_authority a join runtime_metadata r on a.singleton=r.singleton where a.singleton=1').fetchone()
        if not row or not isinstance(row[0], str) or not re.fullmatch('[0-9a-f]{64}', row[0]) or not isinstance(row[1], int) or row[1] <= 0:
            raise ValueError('unbound_runtime_scope')
        matches = correlate(db, row[0], observations)
        recorded = recorded_observations(db, row[0], host, deadline)
        auxiliary_report = auxiliary_inventory(db, row[0], host, auxiliary, deadline,
                                               birth=process_birth, ps=ps, binding=process_binding)
    if time.monotonic() > deadline:
        raise ValueError('inventory_deadline')
    stable = []
    for item in matches:
        if process_birth(item['pid']) != births[item['pid']]:
            changed += 1
            continue
        item['process_start_observation'] = births[item['pid']]
        item['start_observation_unchanged'] = True
        item['recorded_process_binding'] = process_binding(item.pop('_recorded_process', None), item['pid'], births[item['pid']], host)
        stable.append(item)
    if time.monotonic() > deadline:
        raise ValueError('inventory_deadline')
    return dict(version=3, discovery='bounded_host_argv_candidates', authority_id=row[0], boot_generation=str(row[1]), observations=stable,
                recorded_process_inventory=recorded, auxiliary_process_inventory=auxiliary_report,
                changed_process_observations=changed,
                unconfirmed_candidate_observations=unconfirmed,
                unscoped_runners_on_host=unscoped, unavailable_process_observations=vanished,
                process_liveness_proven=False, fencing_proven=False,
                trust='Forgeable command-line metadata and a later SQLite snapshot. Kernel start markers were equal across reads, but timestamps have finite resolution. Races, exec changes and PID reuse are still possible. Expected wrapper identity is durable intent, not proof of ACK or worker release. Reobserve contradictions; never signal a PID or grant lifecycle authority from this report.')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('database', type=Path)
    parser.add_argument('--host-reader', type=Path, help='Explicit trusted conductor-daemon executable for native host/boot comparison')
    args = parser.parse_args()
    try:
        host = None
        if args.host_reader is not None:
            from disaster_recovery import host_epoch
            host = host_epoch(args.host_reader.resolve(strict=True))
        print(json.dumps(inspect(args.database, host), indent=2))
    except (OSError, ValueError, sqlite3.Error, subprocess.SubprocessError) as error:
        # Only fixed local diagnostics; SQLite and process text can contain private data.
        message = str(error) if type(error) is ValueError else type(error).__name__
        print(json.dumps({'error': message}), file=sys.stderr)
        return 1
    return 0


if __name__ == '__main__':
    sys.exit(main())
