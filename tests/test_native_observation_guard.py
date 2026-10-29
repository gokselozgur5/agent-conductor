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
                           recorded_process_inventory=dict(status='observed', invalid_records=0, observations=[]))
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


if __name__ == '__main__':
    unittest.main()
