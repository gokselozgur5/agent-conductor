#!/usr/bin/env python3
"""Read-only sampled native acceptance guard; never process/fencing authority."""
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import stat
import time

from inspect_runner_processes import inspect


class Violation(RuntimeError):
    """Fixed diagnostic code; no private process arguments or file contents."""


def require(condition, code):
    if not condition:
        raise Violation(code)


def board_projection(database, read_page, deadline):
    """Compare all pages only when no SQLite writer committed during observation.

    read_page(after) must use its own bounded transport timeout and return the
    operator /api/tasks envelope. No aggregate contents leave this function.
    """
    with sqlite3.connect(Path(database).as_uri()+'?mode=ro', uri=True, timeout=2) as db:
        db.set_progress_handler(lambda: int(time.monotonic() > deadline), 1000)
        version = db.execute('pragma data_version').fetchone()[0]
        expected = {}
        total = 0
        for task_id, raw in db.execute("select id,case when typeof(aggregate_json)='text' and length(aggregate_json)<=1048576 then aggregate_json else null end from tasks order by id limit 10001"):
            require(len(expected) < 10000 and isinstance(raw, str), 'board_database_limit')
            total += len(raw.encode())
            require(total <= 64*1024**2, 'board_database_limit')
            expected[task_id] = json.loads(raw)
        observed = {}
        after = None
        seen = set()
        total = 0
        for _ in range(10001):
            require(time.monotonic() <= deadline, 'board_observation_deadline')
            page = read_page(after)
            require(isinstance(page, dict) and isinstance(page.get('tasks'), list), 'board_page_invalid')
            for row in page['tasks']:
                require(isinstance(row, dict) and isinstance(row.get('aggregate'), dict), 'board_row_invalid')
                task = row['aggregate']
                key = task.get('id')
                require(isinstance(key, str) and key not in observed, 'board_duplicate_or_invalid_task')
                total += len(json.dumps(task).encode())
                require(len(observed) < 10000 and total <= 64*1024**2, 'board_response_limit')
                observed[key] = task
            require('next_after' in page, 'board_cursor_missing')
            after = page['next_after']
            if after is None:
                break
            require(isinstance(after, str) and after not in seen and page['tasks'], 'board_cursor_invalid')
            seen.add(after)
        else:
            raise Violation('board_page_limit')
        require(time.monotonic() <= deadline, 'board_observation_deadline')
        if db.execute('pragma data_version').fetchone()[0] != version:
            return 'concurrent_change'
        require(observed == expected, 'board_projection_mismatch')
        return 'matching'


def protected_digest(root, deadline):
    """Bounded regular-file tree observation, including metadata names but not contents."""
    root = Path(root)
    require(root.is_absolute() and root.resolve(strict=True) == root and root.is_dir(), 'protected_root_invalid')
    digest = hashlib.sha256()
    entries = 0
    total = 0
    for directory, dirs, files in os.walk(root, followlinks=False):
        dirs.sort()
        files.sort()
        for name in sorted(dirs + files):
            require(time.monotonic() <= deadline, 'surface_observation_deadline')
            entries += 1
            require(entries <= 4096, 'surface_entry_limit')
            path = Path(directory) / name
            before = path.lstat()
            require(stat.S_ISDIR(before.st_mode) or stat.S_ISREG(before.st_mode), 'surface_special_file')
            digest.update(os.fsencode(str(path.relative_to(root))) + b'\0')
            digest.update(str(stat.S_IMODE(before.st_mode)).encode() + b'\0')
            if stat.S_ISREG(before.st_mode):
                descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
                with os.fdopen(descriptor, 'rb') as stream:
                    opened = os.fstat(stream.fileno())
                    require((opened.st_dev, opened.st_ino) == (before.st_dev, before.st_ino) and stat.S_ISREG(opened.st_mode), 'surface_changed_during_read')
                    while True:
                        chunk = stream.read(65536)
                        if not chunk:
                            break
                        total += len(chunk)
                        require(total <= 64 * 1024**2, 'surface_byte_limit')
                        require(time.monotonic() <= deadline, 'surface_observation_deadline')
                        digest.update(chunk)
                    after = os.fstat(stream.fileno())
                    require((after.st_size, after.st_mtime_ns, after.st_ctime_ns) == (opened.st_size, opened.st_mtime_ns, opened.st_ctime_ns), 'surface_changed_during_read')
                current = path.lstat()
                require((current.st_dev, current.st_ino) == (before.st_dev, before.st_ino), 'surface_changed_during_read')
            digest.update(b'\0')
    return digest.hexdigest()


class NativeObservationGuard:
    """Fail an acceptance run on sampled contradictions, never repair live state.

    The caller must stop the run if sample() raises and preserve its failure.
    Protected roots must be explicitly selected immutable surfaces, not worker roots.
    """
    def __init__(self, database, host, protected_roots, *, grace_seconds=10, max_gap_seconds=15, board_reader=None):
        require(host is not None, 'trusted_host_required')
        require(0 <= grace_seconds <= 60 and 0 < max_gap_seconds <= 60, 'invalid_monitor_limits')
        require(0 < len(protected_roots) <= 8, 'protected_roots_required')
        self.database = Path(database).resolve(strict=True)
        self.host = host
        self.board_reader = board_reader
        self.grace = grace_seconds
        self.max_gap = max_gap_seconds
        self.roots = tuple(Path(p) for p in protected_roots)
        self.baseline = [protected_digest(p, time.monotonic()+5) for p in self.roots]
        self.suspects = {}
        self.last = time.monotonic()
        self.last_wall = time.time()
        self.boot = None
        self.authority = None
        self.samples = 0
        self.failed = False
        self.uncertain_since = None

    def sample(self, invariant):
        require(not self.failed, 'native_monitor_already_failed')
        try:
            return self._sample(invariant)
        except Exception:
            self.failed = True
            raise

    def _sample(self, invariant):
        now = time.monotonic()
        wall = time.time()
        require(0 <= now-self.last <= self.max_gap, 'native_observation_gap')
        require(abs((wall-self.last_wall)-(now-self.last)) <= 5, 'native_clock_or_suspend_gap')
        require(invariant.get('healthy') is True, 'runtime_invariant_failure')
        with sqlite3.connect(self.database.as_uri()+'?mode=ro', uri=True, timeout=2) as db:
            db.set_progress_handler(lambda: int(time.monotonic()-now > 5), 1000)
            duplicate = db.execute("select 1 from leases where status in ('active','recovery_pending') group by task_id having count(*)>1 limit 1").fetchone()
            require(duplicate is None, 'duplicate_live_lease')
        board = board_projection(self.database, self.board_reader, now+5) if self.board_reader is not None else 'not_checked'
        report = inspect(self.database, self.host)
        require(self.boot is None or report['boot_generation'] == self.boot, 'native_daemon_restart')
        require(self.authority is None or report['authority_id'] == self.authority, 'native_authority_changed')
        uncertain = board == 'concurrent_change' or report['changed_process_observations'] != 0 or report['unavailable_process_observations'] != 0
        self.uncertain_since = (now if self.uncertain_since is None else self.uncertain_since) if uncertain else None
        require(self.uncertain_since is None or now-self.uncertain_since < self.grace, 'native_inventory_uncertain')
        records = report['recorded_process_inventory']
        require(records['status'] == 'observed' and records['invalid_records'] == 0, 'native_recorded_inventory_unavailable')
        suspects = set()
        for item in report['observations']:
            if item['classification'] != 'matching_record' or item['recorded_process_binding'] != 'matching_observation':
                suspects.add(('tag', item['pid'], item['process_start_observation']))
        for item in records['observations']:
            if item['classification'] not in ('recorded_process_unavailable', 'recorded_pid_reused_or_marker_mismatch', 'recorded_process_tag_matches'):
                suspects.add(('record', item['recorded_pid'], item['recorded_packet_sha256']))
        self.suspects = {key: self.suspects.get(key, now) for key in suspects}
        require(all(now-first < self.grace for first in self.suspects.values()), 'persistent_process_contradiction')
        for path, baseline in zip(self.roots, self.baseline):
            require(protected_digest(path, time.monotonic()+5) == baseline, 'protected_surface_changed')
        require(time.monotonic()-now <= self.max_gap, 'native_sample_duration')
        self.last, self.last_wall = now, wall
        self.boot, self.authority = report['boot_generation'], report['authority_id']
        self.samples += 1
        return dict(sample=self.samples, boot=self.boot, tagged_observations=len(report['observations']),
                    recorded_observations=len(records['observations']), pending_contradictions=len(suspects),
                    protected_surfaces=len(self.roots), board_projection=board, inventory_uncertain=uncertain,
                    healthy=not suspects and not uncertain, full_v1_accepted=False)
