import importlib.util
from pathlib import Path
import sqlite3
import unittest

ROOT = Path(__file__).resolve().parents[1]
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


if __name__ == '__main__':
    unittest.main()
