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

    def test_runner_names_handle_spaces_without_reading_other_arguments(self):
        self.assertEqual(inventory.runner_pids('  12 /path with spaces/conductor-runner\n13 unrelated\n14 conductor-runner\n'), [12, 14])
        with self.assertRaisesRegex(ValueError, 'too_many_runner_observations'):
            inventory.runner_pids('\n'.join(str(i) + ' conductor-runner' for i in range(129)))

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
                with mock.patch.object(inventory, 'ps', side_effect=[(0, '42 conductor-runner'), (0, TAG)]), mock.patch.object(inventory, 'process_birth', side_effect=starts):
                    result = inventory.inspect(path)
                self.assertEqual(len(result['observations']), expected)
                self.assertEqual(result['changed_process_observations'], changed)
                self.assertFalse(result['fencing_proven'])
                if expected:
                    self.assertEqual(result['observations'][0]['process_start_observation'], 'old')


if __name__ == '__main__':
    unittest.main()
