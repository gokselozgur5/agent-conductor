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

from process_birth import observe as process_birth

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


def runner_pids(inventory):
    result = []
    for line in inventory.splitlines():
        fields = line.strip().split(None, 1)
        if len(fields) == 2 and fields[0].isdigit() and Path(fields[1]).name == 'conductor-runner':
            result.append(int(fields[0]))
    if len(result) > 128:
        raise ValueError('too_many_runner_observations')
    return result


def correlate(db, authority, observations):
    result = []
    for pid, tag in observations:
        if tag[0] != authority:
            continue
        rows = db.execute('select task_id,attempt_id,lease_epoch,state,wrapper_identity from executions where packet_sha256=? limit 2', (tag[1],)).fetchall()
        item = dict(pid=pid, packet_sha256=tag[1], wrapper_identity=tag[2])
        if not rows:
            item['classification'] = 'no_matching_record'
        elif len(rows) != 1:
            item['classification'] = 'ambiguous_records'
        else:
            task, attempt, epoch, state, wrapper = rows[0]
            item.update(task_id=task, attempt_id=attempt, lease_epoch=str(epoch), execution_state=state)
            if wrapper is None and state == 'prepared':
                item['classification'] = 'prepared_not_acknowledged'
            elif wrapper != tag[2]:
                item['classification'] = 'wrapper_identity_mismatch'
            elif state == 'fenced':
                item['classification'] = 'fenced_record_process_observed'
            else:
                item['classification'] = 'matching_record'
        result.append(item)
    return result


def inspect(database):
    database = database.resolve(strict=True)
    deadline = time.monotonic() + 10
    code, inventory = ps(['-axo', 'pid=,comm='])
    if code:
        raise ValueError('process_inventory_failed')
    observations = []
    unscoped = 0
    vanished = 0
    changed = 0
    births = {}
    for pid in runner_pids(inventory):
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
            unscoped += 1
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
    if time.monotonic() > deadline:
        raise ValueError('inventory_deadline')
    stable = []
    for item in matches:
        if process_birth(item['pid']) != births[item['pid']]:
            changed += 1
            continue
        item['process_start_observation'] = births[item['pid']]
        item['start_observation_unchanged'] = True
        stable.append(item)
    if time.monotonic() > deadline:
        raise ValueError('inventory_deadline')
    return dict(version=2, authority_id=row[0], boot_generation=str(row[1]), observations=stable,
                changed_process_observations=changed,
                unscoped_runners_on_host=unscoped, unavailable_process_observations=vanished,
                process_liveness_proven=False, fencing_proven=False,
                trust='Forgeable command-line metadata and a later SQLite snapshot. Kernel start markers were equal across reads, but timestamps have finite resolution. Races, exec changes and PID reuse are still possible. Reobserve contradictions; never signal a PID or grant lifecycle authority from this report.')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('database', type=Path)
    args = parser.parse_args()
    try:
        print(json.dumps(inspect(args.database), indent=2))
    except (OSError, ValueError, sqlite3.Error, subprocess.SubprocessError) as error:
        # Only fixed local diagnostics; SQLite and process text can contain private data.
        message = str(error) if type(error) is ValueError else type(error).__name__
        print(json.dumps({'error': message}), file=sys.stderr)
        return 1
    return 0


if __name__ == '__main__':
    sys.exit(main())
