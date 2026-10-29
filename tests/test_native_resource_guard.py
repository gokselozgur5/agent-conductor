from pathlib import Path
import sqlite3
import sys
import tempfile
import unittest
from unittest import mock
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'tools'))
import native_resource_guard as resources
from native_observation_guard import Violation


class ResourceTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.root=Path(self.temp.name);self.db=self.root/'db'
        with sqlite3.connect(self.db) as db:
            db.executescript('create table event_deliveries(status);create table workflow_dispatch(status);')
        self.birth=mock.patch.object(resources,'observe',return_value='stable');self.birth.start()
        self.ps=mock.patch.object(resources,'ps',return_value=(0,'100'));self.ps.start()

    def tearDown(self):
        self.ps.stop();self.birth.stop();self.temp.cleanup()

    def test_rss_absolute_and_growth_limits_latch(self):
        monitor=resources.NativeResourceGuard(self.db,42,growth_kib=10)
        self.assertEqual(monitor.sample()['rss_kib'],100)
        with mock.patch.object(resources,'ps',return_value=(0,'111')):
            with self.assertRaisesRegex(Violation,'resource_rss_growth'):monitor.sample()
        with self.assertRaisesRegex(Violation,'resource_monitor_already_failed'):monitor.sample()
        with self.assertRaisesRegex(Violation,'resource_rss_limit'):
            resources.NativeResourceGuard(self.db,42,max_rss_kib=99).sample()

    def test_active_queues_are_bounded_but_terminal_history_is_not_backlog(self):
        with sqlite3.connect(self.db) as db:
            db.executemany('insert into event_deliveries values(?)', [('acked',),('stale',),('dead',),('pending',),('leased',)])
            db.executemany('insert into workflow_dispatch values(?)',[('finished',),('queued',),('needs_recovery',)])
        sample=resources.NativeResourceGuard(self.db,42).sample()
        self.assertEqual(sample['pending_deliveries'],2);self.assertEqual(sample['unfinished_dispatch'],2)
        with self.assertRaisesRegex(Violation,'resource_delivery_limit'):
            resources.NativeResourceGuard(self.db,42,max_pending_deliveries=1).sample()
        with self.assertRaisesRegex(Violation,'resource_dispatch_limit'):
            resources.NativeResourceGuard(self.db,42,max_unfinished_dispatch=1).sample()

    def test_disk_special_files_and_process_identity_refuse(self):
        with self.assertRaisesRegex(Violation,'resource_state_limit'):
            resources.NativeResourceGuard(self.db,42,max_state_bytes=1).sample()
        link=self.root/'log';link.symlink_to(self.db)
        with self.assertRaisesRegex(Violation,'resource_special_file'):
            resources.NativeResourceGuard(self.db,42,[link]).sample()
        monitor=resources.NativeResourceGuard(self.db,42)
        with mock.patch.object(resources,'observe',side_effect=['stable','different']):
            with self.assertRaisesRegex(Violation,'resource_process_identity_changed'):monitor.sample()

    def test_unavailable_or_malformed_rss_refuses(self):
        for result in [(1,''),(0,'PRIVATE'),(0,'-1'),(0,'①')]:
            monitor=resources.NativeResourceGuard(self.db,42)
            with mock.patch.object(resources,'ps',return_value=result):
                with self.assertRaisesRegex(Violation,'resource_rss_unavailable'):monitor.sample()


if __name__=='__main__':unittest.main()
