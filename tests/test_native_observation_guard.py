import json
from pathlib import Path
import sqlite3
import sys
import tempfile
import time
import unittest
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'tools'))
import native_observation_guard as guard


class NativeGuardTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.surface = self.root/'protected'
        self.surface.mkdir()
        (self.surface/'file').write_text('original')
        self.db = self.root/'db'
        with sqlite3.connect(self.db) as db:
            db.execute('create table leases(task_id,status)')
        self.host = dict(platform='linux', host_id='a'*64, boot_id='b'*64)
        self.report = dict(boot_generation='1', authority_id='a'*64, changed_process_observations=0,
                           unavailable_process_observations=0, observations=[],
                           recorded_process_inventory=dict(status='observed', invalid_records=0, observations=[]),
                           auxiliary_process_inventory=dict(status='observed', invalid_records=0, observations=[]))
        self.patch = mock.patch.object(guard, 'inspect', side_effect=lambda *args: self.report)
        self.patch.start()

    def tearDown(self):
        self.patch.stop()
        self.temp.cleanup()

    def monitor(self, **kwargs):
        return guard.NativeObservationGuard(self.db, self.host, [self.surface], **kwargs)

    def test_clean_native_sample_and_write_violation_latches(self):
        monitor = self.monitor()
        self.assertTrue(monitor.sample({'healthy': True})['healthy'])
        (self.surface/'file').write_text('UNAUTHORIZED_PRIVATE_DATA')
        with self.assertRaisesRegex(guard.Violation, '^protected_surface_changed$'):
            monitor.sample({'healthy': True})
        (self.surface/'file').write_text('original')
        with self.assertRaisesRegex(guard.Violation, '^native_monitor_already_failed$'):
            monitor.sample({'healthy': True})

    def test_process_grace_clears_but_persistent_orphan_fails(self):
        monitor = self.monitor(grace_seconds=1)
        self.report['observations'] = [dict(pid=42, process_start_observation='start', classification='no_matching_record', recorded_process_binding='legacy_unavailable')]
        self.assertEqual(monitor.sample({'healthy': True})['pending_contradictions'], 1)
        self.report['observations'] = []
        self.assertEqual(monitor.sample({'healthy': True})['pending_contradictions'], 0)
        self.report['observations'] = [dict(pid=42, process_start_observation='start', classification='no_matching_record', recorded_process_binding='legacy_unavailable')]
        monitor.sample({'healthy': True})
        monitor.suspects = {key: time.monotonic()-2 for key in monitor.suspects}
        with self.assertRaisesRegex(guard.Violation, 'persistent_process_contradiction'):
            monitor.sample({'healthy': True})

    def test_recorded_tag_loss_and_uncertain_inventory_fail(self):
        self.report['recorded_process_inventory']['observations'] = [dict(recorded_pid=42, recorded_packet_sha256='b'*64, classification='recorded_process_tag_absent_or_invalid')]
        with self.assertRaisesRegex(guard.Violation, 'persistent_process_contradiction'):
            self.monitor(grace_seconds=0).sample({'healthy': True})
        self.report['recorded_process_inventory']['observations'] = []
        self.report['changed_process_observations'] = 1
        with self.assertRaisesRegex(guard.Violation, 'native_inventory_uncertain'):
            self.monitor(grace_seconds=0).sample({'healthy': True})

    def test_uncertainty_is_unhealthy_and_cannot_persist_past_grace(self):
        monitor = self.monitor(grace_seconds=1)
        self.report['unavailable_process_observations'] = 1
        result = monitor.sample({'healthy': True})
        self.assertFalse(result['healthy'])
        self.assertTrue(result['inventory_uncertain'])
        self.report['unavailable_process_observations'] = 0
        self.assertTrue(monitor.sample({'healthy': True})['healthy'])
        self.report['unavailable_process_observations'] = 1
        monitor.sample({'healthy': True})
        monitor.uncertain_since -= 2
        with self.assertRaisesRegex(guard.Violation, 'native_inventory_uncertain'):
            monitor.sample({'healthy': True})

    def test_auxiliary_mismatch_grace_and_missing_coverage_fail_closed(self):
        monitor = self.monitor(grace_seconds=1)
        auxiliary = self.report['auxiliary_process_inventory']
        auxiliary['observations'] = [dict(invocation_id='invocation', pid=42, classification='awaiting_ack')]
        self.assertFalse(monitor.sample({'healthy': True})['healthy'])
        auxiliary['observations'][0]['classification'] = 'matching_record'
        self.assertTrue(monitor.sample({'healthy': True})['healthy'])
        auxiliary['observations'][0]['classification'] = 'no_matching_record'
        monitor.sample({'healthy': True})
        monitor.suspects = {key: time.monotonic()-2 for key in monitor.suspects}
        with self.assertRaisesRegex(guard.Violation, 'persistent_process_contradiction'):
            monitor.sample({'healthy': True})
        auxiliary['invalid_records'] = 1
        with self.assertRaisesRegex(guard.Violation, 'native_auxiliary_inventory_unavailable'):
            self.monitor().sample({'healthy': True})
        del self.report['auxiliary_process_inventory']
        with self.assertRaisesRegex(guard.Violation, 'native_auxiliary_inventory_unavailable'):
            self.monitor().sample({'healthy': True})

    def test_duplicate_lease_runtime_failure_and_observation_gap(self):
        with self.assertRaisesRegex(guard.Violation, 'runtime_invariant_failure'):
            self.monitor().sample({'healthy': False})
        monitor = self.monitor()
        monitor.last -= 61
        with self.assertRaisesRegex(guard.Violation, 'native_observation_gap'):
            monitor.sample({'healthy': True})
        with sqlite3.connect(self.db) as db:
            db.executemany('insert into leases values(?,?)', [('task','active'),('task','recovery_pending')])
        with self.assertRaisesRegex(guard.Violation, 'duplicate_live_lease'):
            self.monitor().sample({'healthy': True})

    def test_bound_verification_surface_cannot_be_mutated_or_rebound(self):
        import hashlib
        root=self.root/'snapshot';root.mkdir();(root/'tree').mkdir()
        file=root/'tree/value';file.write_bytes(b'one')
        raw=json.dumps(dict(version=1,entries=[dict(path='value',sha256=hashlib.sha256(b'one').hexdigest(),bytes=3,executable=False)]),separators=(',',':')).encode()
        (root/'manifest.json').write_bytes(raw)
        source='snapshot:'+hashlib.sha256(raw).hexdigest()
        monitor=self.monitor();monitor.bind_verification_surface(root,source)
        self.assertEqual(monitor.sample({'healthy':True})['verification_surfaces'],1)
        file.write_bytes(b'two')
        with self.assertRaisesRegex(guard.Violation,'verification_file_changed'):
            monitor.sample({'healthy':True})
        self.assertTrue(monitor.failed)
        file.write_bytes(b'one')
        monitor=self.monitor();monitor.bind_verification_surface(root,source)
        with self.assertRaisesRegex(guard.Violation,'verification_binding_changed'):
            monitor.bind_verification_surface(root,'snapshot:'+'a'*64)
        self.assertTrue(monitor.failed)

    def test_surface_symlink_and_added_file_refuse_without_contents(self):
        monitor = self.monitor()
        (self.surface/'added').write_text('PRIVATE')
        with self.assertRaisesRegex(guard.Violation, '^protected_surface_changed$') as caught:
            monitor.sample({'healthy': True})
        self.assertNotIn('PRIVATE', str(caught.exception))
        (self.surface/'link').symlink_to(self.root/'db')
        with self.assertRaisesRegex(guard.Violation, 'surface_special_file'):
            self.monitor()
        self.assertNotIn('PRIVATE', json.dumps(self.report))

class ProtectedTreeEncodingTests(unittest.TestCase):
    def test_file_content_cannot_impersonate_another_tree_entry(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            left, right = root/'left', root/'right'
            left.mkdir(); right.mkdir()
            # The old unframed stream encodes these different trees identically.
            (left/'a').write_bytes(b'x' + b'\0b\0' + b'420\0y')
            (right/'a').write_bytes(b'x')
            (right/'b').write_bytes(b'y')
            for path in [left/'a', right/'a', right/'b']:
                path.chmod(0o644)
            self.assertNotEqual(guard.protected_digest(left,time.monotonic()+5), guard.protected_digest(right,time.monotonic()+5))

    def test_empty_file_and_empty_directory_are_distinct_at_equal_mode(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            left, right = root/'left', root/'right'
            left.mkdir(); right.mkdir()
            (left/'entry').write_bytes(b'')
            (right/'entry').mkdir()
            (left/'entry').chmod(0o755); (right/'entry').chmod(0o755)
            self.assertNotEqual(guard.protected_digest(left,time.monotonic()+5), guard.protected_digest(right,time.monotonic()+5))


class BoardProjectionTests(unittest.TestCase):
    def test_pages_match_but_missing_duplicate_and_changed_rows_refuse(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory)/'db'
            tasks = [dict(id=str(i), version=1) for i in range(3)]
            with sqlite3.connect(path) as db:
                db.execute('create table tasks(id,aggregate_json)')
                db.executemany('insert into tasks values(?,?)', [(x['id'],json.dumps(x)) for x in tasks])
            def read(after):
                index = 0 if after is None else int(after)
                return dict(tasks=[dict(aggregate=tasks[index])], next_after=str(index+1) if index<2 else None)
            self.assertEqual(guard.board_projection(path,read,time.monotonic()+5),'matching')
            for rows in [tasks[:2], [tasks[0],tasks[0]], [dict(id='0',version=2),*tasks[1:]]]:
                with self.assertRaises(guard.Violation):
                    guard.board_projection(path,lambda _:dict(tasks=[dict(aggregate=x) for x in rows],next_after=None),time.monotonic()+5)

    def test_concurrent_commit_is_unknown_even_if_values_are_restored(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory)/'db'
            with sqlite3.connect(path) as db:
                db.execute('create table tasks(id,aggregate_json)')
                db.execute('insert into tasks values(?,?)',('a','{"id":"a","version":1}'))
            def read(_):
                with sqlite3.connect(path) as db:
                    db.execute('update tasks set aggregate_json=?', ('{"id":"a","version":2}',))
                with sqlite3.connect(path) as db:
                    db.execute('update tasks set aggregate_json=?', ('{"id":"a","version":1}',))
                return dict(tasks=[],next_after=None)
            self.assertEqual(guard.board_projection(path,read,time.monotonic()+5),'concurrent_change')

    def test_oversized_database_and_cyclic_cursor_refuse(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory)/'db'
            with sqlite3.connect(path) as db:
                db.execute('create table tasks(id,aggregate_json)')
                db.execute('insert into tasks values(?,?)',('a','x'*1048577))
            with self.assertRaisesRegex(guard.Violation,'board_database_limit'):
                guard.board_projection(path,lambda _:None,time.monotonic()+5)
            with sqlite3.connect(path) as db:db.execute('delete from tasks')
            with self.assertRaisesRegex(guard.Violation,'board_cursor_invalid'):
                guard.board_projection(path,lambda _:dict(tasks=[],next_after='a'),time.monotonic()+5)


if __name__ == '__main__':
    unittest.main()
