#!/usr/bin/env python3
"""Bounded unattended native observation loop, without workload/lifecycle authority."""
import hashlib
import json
import math
import os
from pathlib import Path
import time

from native_observation_guard import Violation, require
from soak_control_plane import atomic_json

SEVEN_DAYS = 604800


def pin(path):
    digest = hashlib.sha256()
    total = 0
    with Path(path).open('rb') as stream:
        while True:
            chunk = stream.read(65536)
            if not chunk:
                break
            total += len(chunk)
            require(total <= 512*1024**2, 'pin_file_limit')
            digest.update(chunk)
    return digest.hexdigest()


def run_native_monitor(guard, invariant_reader, output, *, duration_seconds,
                       interval_seconds, pinned_files, resources=None):
    """Observe an already-owned test runtime; caller owns workload and cleanup.

    Reader transport must have a finite timeout. Existing output is never reused.
    A healthy terminal sample and actual tagged/recorded observations are required.
    No full-v1 or write-capability acceptance is granted, even after seven days.
    """
    require(all(type(v) in (int, float) and math.isfinite(v) for v in (duration_seconds, interval_seconds)), 'invalid_monitor_duration')
    require(.1 <= duration_seconds <= SEVEN_DAYS and .1 <= interval_seconds <= 60, 'invalid_monitor_duration')
    require(interval_seconds < guard.max_gap, 'invalid_monitor_cadence')
    require(math.ceil(duration_seconds/interval_seconds)+2 <= 150000, 'monitor_sample_limit')
    require(guard.board_reader is not None, 'board_reader_required')
    require(duration_seconds < SEVEN_DAYS or resources is not None, 'soak_resources_required')
    require(resources is None or resources.database == guard.database, 'resource_database_mismatch')
    require(0 < len(pinned_files) <= 8, 'runtime_pins_required')
    sources = [Path(__file__), Path(__file__).with_name('native_observation_guard.py'),
               Path(__file__).with_name('inspect_runner_processes.py'),
               Path(__file__).with_name('process_birth.py'),
               Path(__file__).with_name('native_resource_guard.py'),
               Path(__file__).with_name('verification_surface.py'),
               Path(__file__).with_name('soak_control_plane.py')]
    files = {str(Path(p).resolve(strict=True)): pin(p) for p in [*pinned_files, *sources]}
    output = Path(output)
    output.mkdir(mode=0o700)  # Refuse an existing directory; never merge runs.
    started = time.monotonic()
    wall = time.time()
    samples = 0
    tagged = 0
    recorded = 0
    unhealthy = 0
    written = 0
    last = None
    status = 'running'
    error = None
    record = dict(scope='sampled_native_process_board_and_protected_tree',
                  full_v1_accepted=False, duration_seconds=duration_seconds,
                  interval_seconds=interval_seconds, started_wall_s=wall, pinned_sha256=files,
                  resource_limits=None if resources is None else dict(resources.limits),
                  observed_daemon=None if resources is None else dict(pid=resources.pid, start_marker=resources.birth))
    atomic_json(output/'run.json', record)
    try:
        descriptor = os.open(output/'samples.jsonl', os.O_WRONLY|os.O_CREAT|os.O_EXCL, 0o600)
        with os.fdopen(descriptor, 'w') as stream:
            while True:
                sample_started = time.monotonic()
                require(resources is None or dict(resources.limits) == record['resource_limits'], 'resource_limits_changed')
                last = guard.sample(invariant_reader())
                last = dict(last, resources=None if resources is None else resources.sample(), elapsed_s=time.monotonic()-started)
                samples += 1
                require(samples <= 150000, 'monitor_sample_limit')
                tagged = max(tagged, last['tagged_observations'])
                recorded = max(recorded, last['recorded_observations'])
                unhealthy += not last['healthy']
                line = json.dumps(last)+'\n'
                written += len(line.encode())
                require(written <= 128*1024**2, 'monitor_evidence_limit')
                stream.write(line)
                stream.flush()
                os.fsync(stream.fileno())
                atomic_json(output/'status.json', dict(status='running', samples=samples,
                            elapsed_s=last['elapsed_s'], latest=last, full_v1_accepted=False))
                if time.monotonic()-started >= duration_seconds:
                    break
                time.sleep(max(0, min(interval_seconds-(time.monotonic()-sample_started), duration_seconds-(time.monotonic()-started))))
        require(last['healthy'] and last['board_projection']=='matching', 'uncertain_terminal_sample')
        require(tagged > 0 and recorded > 0, 'native_workload_not_observed')
        require(all(pin(path)==digest for path,digest in files.items()), 'monitor_pin_changed')
        require(time.monotonic()-started >= duration_seconds, 'monitor_duration_incomplete')
        tail = time.monotonic()-guard.last
        require(tail <= guard.max_gap and abs((time.time()-guard.last_wall)-tail) <= 5, 'native_terminal_observation_gap')
        status = 'completed_sampled_native_soak' if duration_seconds == SEVEN_DAYS else 'completed_short_native_probe'
    except (Exception, KeyboardInterrupt) as caught:
        status = 'failed'
        error = str(caught) if isinstance(caught, Violation) else type(caught).__name__
    finally:
        receipt = dict(record, status=status, error=error, samples=samples,
                       elapsed_s=time.monotonic()-started, unhealthy_samples=unhealthy,
                       maximum_tagged=tagged, maximum_recorded=recorded)
        atomic_json(output/'status.json', receipt)
    require(status != 'failed', error or 'native_monitor_failed')
    return receipt
