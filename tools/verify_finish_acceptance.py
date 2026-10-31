#!/usr/bin/env python3
"""Read-only, fail-closed verification of final delivery evidence bindings."""
import argparse
import hashlib
import json
import math
from pathlib import Path
import re
import subprocess
import sys

SCHEMA = 'conductor.finish-acceptance.v1'
RECEIPT_SCHEMA = 'conductor.finish-evidence.v1'
GATES = {*(f'P{i}' for i in range(1, 7)), *(f'FR-{i}' for i in range(1, 14)),
         *(f'NFR-{i}' for i in range(1, 6))}
FULL_GATES = {'P5', 'P6', 'NFR-1'}
SEVEN_DAYS = 7 * 24 * 60 * 60
SOAK_INVARIANTS = {'leases', 'projections', 'tagged_orphans', 'stale_epochs', 'unfenced_surfaces'}
SHA256 = re.compile(r'[0-9a-f]{64}\Z')


def require(condition, message):
    if not condition:
        raise ValueError(message)


def parse_json(text):
    def unique(pairs):
        result = {}
        for key, value in pairs:
            require(key not in result, 'duplicate JSON field')
            result[key] = value
        return result
    return json.loads(text, object_pairs_hook=unique,
                      parse_constant=lambda _: (_ for _ in ()).throw(ValueError('nonfinite JSON')))


def read_bounded(path, maximum):
    with path.open('rb') as stream:
        data = stream.read(maximum + 1)
    require(len(data) <= maximum, 'evidence exceeds size bound')
    return data


def read_json(path):
    return parse_json(read_bounded(path, 4 * 1024 * 1024))


def evidence_path(root, reference):
    require(isinstance(reference, dict), 'file reference must be an object')
    name, expected = reference.get('path'), reference.get('sha256')
    require(isinstance(name, str) and name, 'file path missing')
    relative = Path(name)
    require(not relative.is_absolute() and '..' not in relative.parts,
            'file path must stay under evidence root')
    require(isinstance(expected, str) and SHA256.fullmatch(expected), 'invalid file digest')
    target = root.joinpath(relative)
    require(not any(p.is_symlink() for p in [target, *target.parents] if p != root.parent),
            'linked evidence is refused')
    require(target.is_file() and target.resolve().is_relative_to(root), 'evidence file missing or escaped')
    return target, expected


def bound_bytes(root, reference, maximum):
    # Hash and parse the same bounded bytes; never reopen a validated pathname.
    target, expected = evidence_path(root, reference)
    data = read_bounded(target, maximum)
    require(hashlib.sha256(data).hexdigest() == expected, 'file digest mismatch')
    return data


def bound_file(root, reference):
    target, expected = evidence_path(root, reference)
    digest = hashlib.sha256()
    with target.open('rb') as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b''):
            digest.update(chunk)
    require(digest.hexdigest() == expected, 'file digest mismatch')
    return target


def number(value):
    return type(value) in (int, float) and math.isfinite(value)


def verify_soak(receipt, attachments):
    metrics = receipt.get('metrics', {})
    require(isinstance(metrics, dict), 'metrics must be an object')
    require(metrics.get('scope') == 'integrated_native', 'control-plane-only soak is insufficient')
    require(metrics.get('uninterrupted') is True, 'soak was interrupted')
    require(number(metrics.get('requested_duration_s')) and metrics['requested_duration_s'] >= SEVEN_DAYS,
            'soak requested duration is shorter than seven days')
    samples = attachments.get('soak_samples')
    require(isinstance(samples, bytes) and len(samples) <= 32 * 1024 * 1024,
            'bounded native soak samples required')
    rss_limit, queue_limit = metrics.get('rss_limit_bytes'), metrics.get('queue_limit')
    require(number(rss_limit) and rss_limit > 0 and number(queue_limit) and queue_limit >= 0,
            'soak resource limits missing')
    previous, first, boot, count = None, None, None, 0
    for line in samples.splitlines():
        sample = parse_json(line)
        require(isinstance(sample, dict), 'invalid soak sample')
        elapsed = sample.get('elapsed_monotonic_s')
        require(number(elapsed) and elapsed >= 0, 'invalid soak sample time')
        if previous is None:
            require(elapsed <= 1, 'soak initial observation missing')
            first = elapsed
            boot = sample.get('boot_generation')
            require(type(boot) is int and boot > 0, 'soak boot identity missing')
        else:
            require(0 < elapsed - previous <= 190, 'soak observation gap or reversed time')
        require(sample.get('boot_generation') == boot, 'soak restarted')
        require(sample.get('invariants_passed') is True and sample.get('native_monitoring') is True,
                'native invariant observation missing or failed')
        violations = sample.get('invariant_violations')
        require(isinstance(violations, dict) and set(violations) == SOAK_INVARIANTS
                and all(type(value) is int and value == 0 for value in violations.values()),
                'explicit native invariant coverage missing or violated')
        require(number(sample.get('rss_bytes')) and 0 <= sample['rss_bytes'] <= rss_limit,
                'soak memory bound failed')
        require(type(sample.get('pending_queue')) is int and 0 <= sample['pending_queue'] <= queue_limit,
                'soak queue bound failed')
        previous, count = elapsed, count + 1
    require(count > 1 and previous - first >= SEVEN_DAYS, 'seven days of continuous samples required')


def verify_metrics(gate, receipt, attachments):
    metrics = receipt.get('metrics', {})
    require(isinstance(metrics, dict), 'metrics must be an object')
    if gate == 'NFR-1':
        verify_soak(receipt, attachments)
    if gate == 'NFR-2':
        for key, threshold in [('projects', 100), ('tasks', 10_000), ('events', 100_000)]:
            require(type(metrics.get(key)) is int and metrics[key] >= threshold, 'performance load is incomplete')
        for key, threshold in [('board_p95_ms', 100), ('wake_p95_ms', 500)]:
            require(number(metrics.get(key)) and 0 <= metrics[key] < threshold, 'performance target failed')
        require(number(metrics.get('idle_cpu_percent')) and metrics['idle_cpu_percent'] >= 0,
                'idle CPU measurement missing')
    if gate == 'NFR-3':
        require(set(metrics.get('accepted_hosts', [])) >= {'macos-arm64', 'linux-arm64', 'linux-amd64'},
                'supported-host acceptance incomplete')
    if gate == 'P3':
        accounts = metrics.get('worker_uids', [])
        require(isinstance(accounts, list) and all(type(uid) is int and uid >= 0 for uid in accounts)
                and len(set(accounts)) >= 2, 'distinct worker accounts not demonstrated')
        require(set(metrics.get('vendors', [])) >= {'codex', 'claude'}, 'both native vendors required')


def verify(matrix_path, evidence_root, source_root):
    errors, accepted = [], []
    try:
        matrix = read_json(matrix_path)
        require(isinstance(matrix, dict), 'matrix must be an object')
        require(matrix.get('schema') == SCHEMA, 'unsupported matrix schema')
        rows = matrix.get('gates', [])
        require(isinstance(rows, list) and all(isinstance(row, dict) for row in rows), 'invalid gate list')
        ids = [row.get('id') for row in rows]
        require(len(ids) == len(set(ids)) and set(ids) == GATES, 'required gate inventory mismatch')
        candidate = matrix.get('candidate')
        require(isinstance(candidate, dict), 'final candidate is not pinned')
        source = candidate.get('source_commit')
        require(isinstance(source, str) and re.fullmatch(r'(?:[0-9a-f]{40}|[0-9a-f]{64})', source), 'invalid source commit')
        actual = subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=source_root, text=True).strip()
        require(actual == source, 'candidate source differs from checkout HEAD')
        committed = parse_json(subprocess.check_output(
            ['git', 'show', 'HEAD:docs/archive/finish_acceptance.json'], cwd=source_root, text=True))
        require(isinstance(committed, dict) and isinstance(committed.get('gates'), list),
                'committed acceptance contract missing')
        contract = {row['id']: row for row in committed['gates']}
        require(set(contract) == GATES, 'committed gate inventory mismatch')
        dirty = subprocess.check_output(['git', '--no-optional-locks', 'status', '--porcelain', '--untracked-files=all'],
                                        cwd=source_root, text=True).splitlines()
        require(all(line[3:] == 'docs/archive/finish_acceptance.json' for line in dirty),
                'source checkout has uncommitted or untracked changes')
        artifacts = candidate.get('artifacts', {})
        require(isinstance(artifacts, dict) and artifacts, 'candidate artifacts missing')
        root = evidence_root.resolve(strict=True)
        for reference in artifacts.values():
            bound_file(root, reference)
        artifact_hashes = {name: reference['sha256'] for name, reference in artifacts.items()}
    except (ValueError, OSError, KeyError, TypeError, RecursionError, subprocess.CalledProcessError) as error:
        return {'accepted': False, 'accepted_gates': [], 'errors': [str(error)]}
    for row in rows:
        gate = row['id']
        try:
            require(row.get('required_scope') == ('full' if gate in FULL_GATES else 'native'),
                    'required gate scope mismatch')
            checks = row.get('required_checks')
            require(checks == contract[gate].get('required_checks'), 'required checks differ from committed contract')
            require(isinstance(checks, list) and checks and all(isinstance(c, str) and c for c in checks)
                    and len(set(checks)) == len(checks), 'required checks missing or duplicated')
            require(row.get('status') == 'accepted', 'gate is incomplete')
            references = row.get('evidence')
            require(isinstance(references, list) and references, 'receipt missing')
            covered = set()
            for reference in references:
                receipt = parse_json(bound_bytes(root, reference, 4 * 1024 * 1024))
                require(isinstance(receipt, dict), 'receipt must be an object')
                require(receipt.get('schema') == RECEIPT_SCHEMA, 'unsupported receipt schema')
                require(receipt.get('gate_id') == gate, 'receipt belongs to another gate')
                require(receipt.get('source_commit') == source and receipt.get('artifact_sha256') == artifact_hashes,
                        'receipt source or artifact binding mismatch')
                scope = receipt.get('scope')
                require(scope in ({'full'} if gate in FULL_GATES else {'native', 'full'}),
                        'fixture or insufficient evidence scope')
                require(receipt.get('passed') is True, 'receipt did not pass')
                attachments = receipt.get('attachments', {})
                require(isinstance(attachments, dict) and attachments, 'underlying evidence missing')
                verified = {name: bound_bytes(root, ref, 32 * 1024 * 1024)
                            if name == 'soak_samples' else bound_file(root, ref)
                            for name, ref in attachments.items()}
                results = receipt.get('checks', {})
                require(isinstance(results, dict) and all(value is True for value in results.values()),
                        'receipt contains failed, skipped or unknown checks')
                covered.update(results)
                verify_metrics(gate, receipt, verified)
            require(set(checks) <= covered, 'required check evidence incomplete')
            accepted.append(gate)
        except (ValueError, OSError, KeyError, TypeError, RecursionError) as error:
            errors.append(f'{gate}: {error}')
    return {'accepted': not errors, 'scope': 'evidence_contract_consistency_only', 'source_commit': source, 'accepted_gates': accepted, 'errors': errors}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--matrix', type=Path, default=Path(__file__).resolve().parents[1] / 'docs/archive/finish_acceptance.json')
    parser.add_argument('--evidence-root', type=Path, required=True)
    parser.add_argument('--source-root', type=Path, default=Path(__file__).resolve().parents[1])
    args = parser.parse_args()
    result = verify(args.matrix, args.evidence_root, args.source_root)
    print(json.dumps(result, indent=2))
    return 0 if result['accepted'] else 1


if __name__ == '__main__':
    sys.exit(main())
