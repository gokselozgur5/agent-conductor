import importlib.util
from pathlib import Path
import sqlite3
import unittest
import sys
import subprocess
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'tools'))
import process_birth
spec = importlib.util.spec_from_file_location('runner_inventory', ROOT / 'tools/inspect_runner_processes.py')
inventory = importlib.util.module_from_spec(spec)
spec.loader.exec_module(inventory)
SCOPE = 'a' * 64
PACKET = 'b' * 64
WRAPPER = '12345678-1234-1234-1234-123456789abc'
TAG = '--conductor-owner=v1:' + ':'.join([SCOPE, PACKET, WRAPPER])


class ProcessInventory(unittest.TestCase):
    def test_only_canonical_single_tag_is_recognized(self):
        self.assertEqual(inventory.parse_tag('/path/conductor-runner ' + TAG), (SCOPE, PACKET, WRAPPER))
        for text in [TAG + ' ' + TAG, TAG.upper(), TAG + ':extra', TAG.replace('v1:', 'v2:'), '--other=value']:
            with self.subTest(text=text):
                self.assertIsNone(inventory.parse_tag(text))

    def test_persisted_binding_requires_pid_marker_and_native_boot(self):
        import json
        host = dict(platform='linux', host_id='a' * 64, boot_id='b' * 64)
        value = dict(host, pid=42, start_marker='linux-start-ticks:123')
        check = lambda raw, pid=42, start='linux-start-ticks:123', current=host: inventory.process_binding(raw, pid, start, current)
        raw = json.dumps(value)
        self.assertEqual(check(raw), 'matching_observation')
        self.assertEqual(check(raw, current=None), 'host_boot_unchecked')
        self.assertEqual(check(raw, pid=43), 'pid_mismatch')
        self.assertEqual(check(raw, start='linux-start-ticks:124'), 'kernel_marker_mismatch')
        self.assertEqual(check(raw, current=dict(host, boot_id='c' * 64)), 'host_boot_mismatch')
        self.assertEqual(check(None), 'legacy_unavailable')
        for bad in ['{}', '[]', 'x' * 1025, json.dumps(dict(value, pid=True))]:
            self.assertEqual(check(bad), 'invalid_recorded_process')

    def test_runner_names_handle_spaces_and_bound_candidates(self):
        self.assertEqual(inventory.runner_pids('  12 /path with spaces/conductor-runner\n13 unrelated\n14 conductor-runner\n'), [12, 14])
        with self.assertRaisesRegex(ValueError, 'too_many_runner_observations'):
            inventory.runner_pids('\n'.join(str(i) + ' conductor-runner' for i in range(1, 130)))

    def test_discovery_finds_renamed_tagged_candidates_and_deduplicates_pids(self):
        names = '12 /path with spaces/conductor-runner\n13 unrelated\n'
        arguments = '12 named ' + TAG + '\n14 /renamed binary ' + TAG + '\n15 untagged\n'
        self.assertEqual(inventory.discover_runner_pids(names, arguments), [12, 14])
        invalid = '\n'.join(pid + ' renamed ' + TAG for pid in ['0', '-1', '2147483648', '①', '999999999999999999999'])
        self.assertEqual(inventory.discover_runner_pids('', invalid), [])
        self.assertEqual(inventory.runner_pids('0 conductor-runner\n① conductor-runner'), [])
        with self.assertRaisesRegex(ValueError, 'too_many_runner_observations'):
            inventory.discover_runner_pids('', '\n'.join(str(pid) + ' renamed ' + TAG for pid in range(1, 130)))
        with self.assertRaisesRegex(ValueError, 'too_many_runner_observations'):
            inventory.discover_runner_pids('1 conductor-runner', '\n'.join(str(pid) + ' renamed ' + TAG for pid in range(2, 130)))

    def test_correspondence_is_scoped_and_never_issues_mutations(self):
        db = sqlite3.connect(':memory:')
        db.execute('create table executions(task_id,attempt_id,lease_epoch,state,wrapper_identity,packet_sha256)')
        observations = [(12, (SCOPE, PACKET, WRAPPER)), (13, ('c' * 64, PACKET, WRAPPER))]
        query = lambda: inventory.correlate(db, SCOPE, observations)
        self.assertEqual(query(), [dict(pid=12, packet_sha256=PACKET, wrapper_identity=WRAPPER, classification='no_matching_record')])
        db.execute('insert into executions values (?,?,?,?,?,?)', ('task', 'attempt', 1, 'prepared', None, PACKET))
        self.assertEqual(query()[0]['classification'], 'prepared_not_acknowledged')
        db.execute("update executions set state='started',wrapper_identity=?", (WRAPPER,))
        before = db.total_changes
        self.assertEqual(query()[0]['classification'], 'matching_record')
        self.assertEqual(db.total_changes, before)
        db.execute("update executions set state='fenced'")
        self.assertEqual(query()[0]['classification'], 'fenced_record_process_observed')
        db.execute("update executions set wrapper_identity='other'")
        self.assertEqual(query()[0]['classification'], 'wrapper_identity_mismatch')
        db.execute('insert into executions select * from executions')
        self.assertEqual(query()[0]['classification'], 'ambiguous_records')
        db.close()


    def test_expected_identity_is_canonical_and_never_inferred_for_legacy(self):
        self.assertEqual(inventory.expected_binding(None, WRAPPER), 'legacy_unavailable')
        self.assertEqual(inventory.expected_binding(WRAPPER, WRAPPER), 'matching_observation')
        self.assertEqual(inventory.expected_binding('22345678-1234-1234-1234-123456789abc', WRAPPER), 'identity_mismatch')
        for invalid in ['', True, 42, b'raw', WRAPPER.upper(), WRAPPER.replace('-', ''), '00000000-0000-0000-0000-000000000000']:
            with self.subTest(invalid=invalid):
                self.assertEqual(inventory.expected_binding(invalid, WRAPPER), 'invalid_recorded_identity')

    def test_expected_identity_distinguishes_preack_blocked_fenced_and_contradictory_ack(self):
        db = sqlite3.connect(':memory:')
        db.execute('create table executions(task_id,attempt_id,lease_epoch,state,wrapper_identity,packet_sha256,expected_wrapper_identity)')
        other = '22345678-1234-1234-1234-123456789abc'
        db.execute('insert into executions values(?,?,?,?,?,?,?)', ('task', 'attempt', 1, 'prepared', None, PACKET, WRAPPER))
        observed = [(12, (SCOPE, PACKET, WRAPPER)), (13, (SCOPE, PACKET, other))]
        for state, classification in [('prepared', 'prepared_not_acknowledged'), ('blocked', 'unacknowledged_expected_wrapper'), ('fenced', 'fenced_record_process_observed')]:
            db.execute('update executions set state=?', (state,))
            before = db.total_changes
            result = inventory.correlate(db, SCOPE, observed)
            self.assertEqual(db.total_changes, before)
            self.assertEqual(result[0]['classification'], classification)
            self.assertEqual(result[0]['expected_wrapper_binding'], 'matching_observation')
            self.assertEqual(result[1]['classification'], 'expected_wrapper_identity_mismatch')
        db.execute("update executions set state='started',wrapper_identity=?", (other,))
        self.assertEqual(inventory.correlate(db, SCOPE, observed)[0]['classification'], 'wrapper_identity_mismatch')
        db.execute('update executions set wrapper_identity=?', (WRAPPER,))
        self.assertEqual(inventory.correlate(db, SCOPE, observed)[0]['classification'], 'matching_record')
        db.execute("update executions set wrapper_identity=NULL,state='prepared',expected_wrapper_identity=NULL")
        result = inventory.correlate(db, SCOPE, observed)[0]
        self.assertEqual(result['classification'], 'prepared_not_acknowledged')
        self.assertEqual(result['expected_wrapper_binding'], 'legacy_unavailable')
        db.close()

    def test_corrupt_or_oversized_bindings_are_bounded_without_exposing_stored_text(self):
        import json
        db = sqlite3.connect(':memory:')
        db.execute('create table executions(task_id,attempt_id,lease_epoch,state,wrapper_identity,packet_sha256,process_json,expected_wrapper_identity)')
        secret = 'PRIVATE_STORED_TEXT' * 10000
        db.execute('insert into executions values(?,?,?,?,?,?,?,?)', ('task', 'attempt', 1, 'prepared', None, PACKET, secret, secret))
        observed = [(12, (SCOPE, PACKET, WRAPPER))]
        for expected in [secret, 42, b'raw', '', '00000000-0000-0000-0000-000000000000']:
            db.execute('update executions set expected_wrapper_identity=?', (expected,))
            result = inventory.correlate(db, SCOPE, observed)[0]
            self.assertEqual(result['classification'], 'invalid_expected_wrapper_identity')
            self.assertEqual(result['_recorded_process'], '')
            self.assertNotIn('PRIVATE_STORED_TEXT', json.dumps(result))
        db.close()


class BirthObservation(unittest.TestCase):
    def test_linux_parser_handles_parentheses_and_refuses_bad_fields(self):
        tail = [b'S'] + [b'0'] * 18 + [b'12345']
        raw = b'42 (worker ) name) ' + b' '.join(tail)
        self.assertEqual(process_birth.linux_start(raw, 42), 'linux-start-ticks:12345')
        for bad in [b'', raw.replace(b'42 (', b'41 ('), raw.replace(b'12345', b'-1'),
                    raw.replace(b'12345', b'18446744073709551616'), b'42 (x) S']:
            self.assertIsNone(process_birth.linux_start(bad, 42))

    def test_native_child_start_is_stable_then_unavailable_after_reap(self):
        child = subprocess.Popen([sys.executable, '-c', 'import sys;sys.stdin.read()'], stdin=subprocess.PIPE)
        try:
            start = process_birth.observe(child.pid)
            self.assertIsNotNone(start)
            self.assertEqual(process_birth.observe(child.pid), start)
        finally:
            child.stdin.close()
            child.wait(timeout=5)
        self.assertIsNone(process_birth.observe(child.pid))
        for bad in [0, -1, True, 2**32]:
            self.assertIsNone(process_birth.observe(bad))

    def test_argument_snapshot_is_only_a_hint_and_private_text_is_not_reported(self):
        import tempfile
        import json
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'db'
            with sqlite3.connect(path) as db:
                db.executescript('create table recovery_authority(singleton,authority_id); create table runtime_metadata(singleton,boot_generation); create table executions(task_id,attempt_id,lease_epoch,state,wrapper_identity,packet_sha256);')
                db.execute('insert into recovery_authority values(1,?)', (SCOPE,))
                db.execute('insert into runtime_metadata values(1,1)')
            # A newline in someone else's argv can invent a candidate line. The
            # direct PID read must independently confirm the tag, or discard it.
            hints = '7 untrusted PRIVATE_HINT\n42 renamed ' + TAG
            for command, expected in [('unrelated PRIVATE_ARGV', 0), ('renamed ' + TAG, 1)]:
                with mock.patch.object(inventory, 'ps', side_effect=[(0, ''), (0, hints), (0, command)]), mock.patch.object(inventory, 'process_birth', return_value='stable'):
                    report = inventory.inspect(path)
                self.assertEqual(len(report['observations']), expected)
                self.assertEqual(report['unconfirmed_candidate_observations'], 1 - expected)
                self.assertEqual(report['unscoped_runners_on_host'], 0)
                self.assertNotIn('PRIVATE_', json.dumps(report))
                self.assertFalse(report['fencing_proven'])
            with mock.patch.object(inventory, 'ps', side_effect=[(0, ''), (1, '')]):
                with self.assertRaisesRegex(ValueError, 'process_discovery_failed'):
                    inventory.inspect(path)

    def test_birth_change_during_argv_read_is_discarded(self):
        import tempfile
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'db'
            with sqlite3.connect(path) as db:
                db.execute('create table recovery_authority(singleton,authority_id)')
                db.execute('create table runtime_metadata(singleton,boot_generation)')
                db.execute('insert into recovery_authority values(1,?)', (SCOPE,))
                db.execute('insert into runtime_metadata values(1,1)')
                db.execute('create table executions(task_id,attempt_id,lease_epoch,state,wrapper_identity,packet_sha256)')
            for starts, expected, changed in [(['old', 'new'], 0, 1),
                                              (['old', 'old', 'new'], 0, 1),
                                              (['old', 'old', 'old'], 1, 0),
                                              ([None], 0, 0)]:
                with mock.patch.object(inventory, 'ps', side_effect=[(0, '42 conductor-runner'), (0, '42 renamed ' + TAG), (0, TAG)]), mock.patch.object(inventory, 'process_birth', side_effect=starts):
                    result = inventory.inspect(path)
                self.assertEqual(len(result['observations']), expected)
                self.assertEqual(result['changed_process_observations'], changed)
                self.assertFalse(result['fencing_proven'])
                if expected:
                    self.assertEqual(result['observations'][0]['process_start_observation'], 'old')


if __name__ == '__main__':
    unittest.main()
