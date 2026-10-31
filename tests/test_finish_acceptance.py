"""Synthetic evidence tests validator mechanics, never product acceptance."""
import copy
import hashlib
import importlib.util
import json
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

REPO = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location('finish_acceptance', REPO / 'tools/verify_finish_acceptance.py')
verify_finish = importlib.util.module_from_spec(spec)
spec.loader.exec_module(verify_finish)


class FinishAcceptanceTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name).resolve()
        self.source = self.root / 'source'
        self.source.mkdir()
        self.evidence = self.root / 'evidence'
        self.evidence.mkdir()
        self.matrix_path = self.root / 'matrix.json'
        self.git('init', '-q')
        self.git('config', 'user.name', 'Acceptance fixture')
        self.git('config', 'user.email', 'fixture@example.invalid')
        (self.source / 'source.txt').write_text('synthetic source')
        (self.source / 'docs/archive').mkdir(parents=True)
        (self.source / 'docs/archive/finish_acceptance.json').write_bytes((REPO / 'docs/archive/finish_acceptance.json').read_bytes())
        self.git('add', 'source.txt', 'docs/archive/finish_acceptance.json')
        self.git('commit', '-qm', 'Synthetic acceptance fixture')
        self.commit = self.git('rev-parse', 'HEAD').strip()
        self.matrix = json.loads((REPO / 'docs/archive/finish_acceptance.json').read_text())
        artifact = self.write('runtime.bin', b'synthetic runtime artifact')
        recording = self.write('recording.txt', b'synthetic test recording, not production evidence')
        samples = []
        for elapsed in range(0, verify_finish.SEVEN_DAYS + 1, 60):
            samples.append(json.dumps({'elapsed_monotonic_s': elapsed, 'boot_generation': 1,
                                       'invariants_passed': True, 'native_monitoring': True,
                                       'invariant_violations': dict.fromkeys(verify_finish.SOAK_INVARIANTS, 0),
                                       'rss_bytes': 1024, 'pending_queue': 0}))
        soak = self.write('samples.jsonl', ('\n'.join(samples) + '\n').encode())
        self.matrix['candidate'] = {'source_commit': self.commit, 'artifacts': {'runtime': artifact}}
        self.receipts = {}
        for row in self.matrix['gates']:
            receipt = {'schema': verify_finish.RECEIPT_SCHEMA, 'gate_id': row['id'],
                       'source_commit': self.commit, 'artifact_sha256': {'runtime': artifact['sha256']},
                       'scope': row['required_scope'], 'passed': True,
                       'checks': dict.fromkeys(row['required_checks'], True),
                       'attachments': {'recording': recording}, 'metrics': {}}
            if row['id'] == 'NFR-1':
                receipt['attachments']['soak_samples'] = soak
                receipt['metrics'] = {'scope': 'integrated_native', 'uninterrupted': True,
                                      'requested_duration_s': verify_finish.SEVEN_DAYS,
                                      'rss_limit_bytes': 2048, 'queue_limit': 1}
            if row['id'] == 'NFR-2':
                receipt['metrics'] = {'projects': 100, 'tasks': 10000, 'events': 100000,
                                      'board_p95_ms': 99, 'wake_p95_ms': 499, 'idle_cpu_percent': 0.5}
            if row['id'] == 'NFR-3':
                receipt['metrics'] = {'accepted_hosts': ['macos-arm64', 'linux-amd64', 'linux-arm64']}
            if row['id'] == 'P3':
                receipt['metrics'] = {'worker_uids': [501, 502], 'vendors': ['codex', 'claude']}
            self.receipts[row['id']] = receipt
            row['status'] = 'accepted'
        self.save()

    def git(self, *args):
        return subprocess.check_output(['git', *args], cwd=self.source, text=True,
                                       stderr=subprocess.DEVNULL)

    def write(self, name, data):
        path = self.evidence / name
        path.write_bytes(data)
        return {'path': name, 'sha256': hashlib.sha256(data).hexdigest()}

    def save(self):
        for row in self.matrix['gates']:
            row['evidence'] = [self.write(row['id'] + '.json', json.dumps(self.receipts[row['id']]).encode())]
        self.matrix_path.write_text(json.dumps(self.matrix))

    def check(self):
        return verify_finish.verify(self.matrix_path, self.evidence, self.source)

    def assertRefused(self, fragment):
        result = self.check()
        self.assertFalse(result['accepted'])
        self.assertIn(fragment, '\n'.join(result['errors']))

    def test_synthetic_complete_bindings_pass_without_mutating_source_or_evidence(self):
        before = {p: p.read_bytes() for p in self.evidence.iterdir()}
        result = self.check()
        self.assertTrue(result['accepted'], result)
        self.assertEqual(set(result['accepted_gates']), verify_finish.GATES)
        self.assertEqual({p: p.read_bytes() for p in self.evidence.iterdir()}, before)
        self.assertEqual(self.git('status', '--porcelain'), '')

    def test_checked_in_matrix_is_intentionally_incomplete(self):
        self.matrix_path.write_bytes((REPO / 'docs/archive/finish_acceptance.json').read_bytes())
        self.assertRefused('final candidate is not pinned')

    def test_gate_inventory_cannot_be_shortened(self):
        self.matrix['gates'].pop()
        self.save()
        self.assertRefused('gate inventory mismatch')

    def test_uncommitted_matrix_cannot_weaken_required_checks(self):
        self.matrix['gates'][0]['required_checks'] = ['default_coordinator_delegates']
        self.save()
        self.assertRefused('required checks differ from committed contract')

    def test_pending_missing_or_failed_evidence_cannot_close_gate(self):
        row = self.matrix['gates'][0]
        row['status'] = 'pending'
        self.save()
        self.assertRefused('P1: gate is incomplete')
        row['status'] = 'accepted'
        self.receipts['P1']['checks'].pop(row['required_checks'][0])
        self.save()
        self.assertRefused('P1: required check evidence incomplete')
        self.receipts['P1']['checks'][row['required_checks'][0]] = False
        self.save()
        self.assertRefused('failed, skipped or unknown')
        (self.evidence / 'P1.json').unlink()
        self.assertRefused('evidence file missing')

    def test_source_and_receipt_identity_must_match(self):
        self.receipts['P1']['source_commit'] = 'a' * 40
        self.save()
        self.assertRefused('receipt source or artifact binding mismatch')
        self.matrix['candidate']['source_commit'] = 'a' * 40
        self.save()
        self.assertRefused('differs from checkout HEAD')

    def test_dirty_source_and_untracked_code_are_refused(self):
        (self.source / 'source.txt').write_text('changed source')
        self.assertRefused('uncommitted or untracked')
        self.git('checkout', '--', 'source.txt')
        (self.source / 'new.py').write_text('untracked implementation')
        self.assertRefused('uncommitted or untracked')

    def test_artifact_receipt_and_recording_tampering_are_refused(self):
        for name in ['runtime.bin', 'P1.json', 'recording.txt']:
            with self.subTest(name=name):
                path = self.evidence / name
                original = path.read_bytes()
                path.write_bytes(original + b'changed')
                self.assertRefused('digest mismatch')
                path.write_bytes(original)
        self.receipts['P1']['artifact_sha256']['runtime'] = 'a' * 64
        self.save()
        self.assertRefused('artifact binding mismatch')

    def test_fixture_scope_never_becomes_native_or_full_acceptance(self):
        self.receipts['P1']['scope'] = 'fixture'
        self.receipts['NFR-1']['scope'] = 'native'
        self.save()
        self.assertRefused('P1: fixture or insufficient')
        self.assertRefused('NFR-1: fixture or insufficient')

    def test_soak_rejects_control_plane_short_request_and_observation_gap(self):
        receipt = self.receipts['NFR-1']
        original = copy.deepcopy(receipt)
        for key, value, failure in [('scope', 'control_plane', 'control-plane-only'),
                                    ('requested_duration_s', 60, 'requested duration'),
                                    ('uninterrupted', False, 'interrupted')]:
            receipt['metrics'] = dict(original['metrics'], **{key: value})
            self.save()
            self.assertRefused(failure)
        receipt['metrics'] = original['metrics']
        lines = (self.evidence / 'samples.jsonl').read_text().splitlines()
        late_start = lines.copy()
        initial = json.loads(late_start[0])
        initial['elapsed_monotonic_s'] = 1
        late_start[0] = json.dumps(initial)
        for changed, failure in [(lines[:2], 'continuous samples'),
                                 (late_start, 'continuous samples'),
                                 ([lines[0], lines[-1]], 'observation gap')]:
            receipt['attachments']['soak_samples'] = self.write('bad_samples.jsonl', '\n'.join(changed).encode())
            self.save()
            self.assertRefused(failure)

    def test_soak_rejects_reboot_missing_native_monitor_and_resource_violation(self):
        original = (self.evidence / 'samples.jsonl').read_text().splitlines()
        for key, value, failure in [('boot_generation', 2, 'restarted'),
                                    ('native_monitoring', False, 'native invariant'),
                                    ('rss_bytes', 9999, 'memory bound'),
                                    ('pending_queue', 2, 'queue bound'),
                                    ('invariant_violations', {}, 'explicit native invariant coverage'),
                                    ('invariant_violations', dict.fromkeys(verify_finish.SOAK_INVARIANTS, 1), 'explicit native invariant coverage')]:
            lines = original.copy()
            sample = json.loads(lines[1])
            sample[key] = value
            lines[1] = json.dumps(sample)
            self.receipts['NFR-1']['attachments']['soak_samples'] = self.write('bad_samples.jsonl', '\n'.join(lines).encode())
            self.save()
            self.assertRefused(failure)

    def test_soak_jsonl_rejects_duplicate_fields_and_nonfinite_values(self):
        for body in ['{"elapsed_monotonic_s":0,"elapsed_monotonic_s":0}',
                     '{"elapsed_monotonic_s":NaN}']:
            self.receipts['NFR-1']['attachments']['soak_samples'] = self.write('bad_samples.jsonl', body.encode())
            self.save()
            self.assertRefused('JSON')

    def mutate_after_digest(self, path, replacement, refusal):
        expected = hashlib.sha256(path.read_bytes()).hexdigest()
        real_sha256 = hashlib.sha256
        mutated = []

        class MutatingDigest:
            def __init__(self, data=b''):
                self.digest = real_sha256(data)

            def update(self, data):
                self.digest.update(data)

            def hexdigest(self):
                result = self.digest.hexdigest()
                if result == expected and not mutated:
                    path.write_bytes(replacement)
                    mutated.append(True)
                return result

        with patch.object(verify_finish.hashlib, 'sha256', MutatingDigest):
            self.assertRefused(refusal)
        self.assertEqual(mutated, [True], 'control must replace evidence after its digest is calculated')
        self.assertEqual(path.read_bytes(), replacement)

    def test_receipt_mutation_after_hash_cannot_replace_checked_bytes(self):
        self.receipts['P1']['passed'] = False
        self.save()
        replacement = json.dumps(dict(self.receipts['P1'], passed=True)).encode()
        self.mutate_after_digest(self.evidence / 'P1.json', replacement, 'receipt did not pass')

    def test_soak_mutation_after_hash_cannot_replace_checked_bytes(self):
        path = self.evidence / 'samples.jsonl'
        valid = path.read_bytes()
        lines = valid.decode().splitlines()
        sample = json.loads(lines[1])
        sample['invariants_passed'] = False
        lines[1] = json.dumps(sample)
        self.receipts['NFR-1']['attachments']['soak_samples'] = self.write('samples.jsonl', '\n'.join(lines).encode())
        self.save()
        self.mutate_after_digest(path, valid, 'native invariant observation missing or failed')

    def test_measured_gates_refuse_thresholds_missing_hosts_and_same_account(self):
        self.receipts['NFR-2']['metrics']['board_p95_ms'] = 100
        self.receipts['NFR-3']['metrics']['accepted_hosts'].remove('linux-arm64')
        self.receipts['P3']['metrics']['worker_uids'] = [501, 501]
        self.save()
        for failure in ['performance target failed', 'supported-host acceptance incomplete', 'distinct worker accounts']:
            self.assertRefused(failure)

    def test_path_traversal_symlinks_and_malformed_documents_fail_closed(self):
        self.receipts['P1']['attachments']['recording']['path'] = '../outside'
        self.save()
        self.assertRefused('stay under evidence root')
        self.receipts['P1']['attachments']['recording']['path'] = 'linked'
        (self.evidence / 'linked').symlink_to(self.evidence / 'recording.txt')
        self.save()
        self.assertRefused('linked evidence')
        for body in ['[]', '{"schema":1,"schema":2}', '{"schema":NaN}']:
            self.matrix_path.write_text(body)
            self.assertFalse(self.check()['accepted'])


if __name__ == '__main__':
    unittest.main()
