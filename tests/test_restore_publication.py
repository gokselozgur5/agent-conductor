"""Abrupt process-exit controls for the real publication/repair filesystem steps."""
import fcntl
import json
import os
from pathlib import Path
import sqlite3
import unittest
from unittest import mock
import test_operator_backup as fixtures
from test_operator_backup import backup, operator_restore
import restore_publication


class PublicationTests(unittest.TestCase):
    setUp = fixtures.BackupTests.setUp
    tearDown = fixtures.BackupTests.tearDown

    def run_crashing_child(self, action):
        child = os.fork()
        if child == 0:
            try:
                action()
            except BaseException:
                import traceback
                traceback.print_exc()
            os._exit(80)
        _, status = os.waitpid(child, 0)
        self.assertEqual(os.waitstatus_to_exitcode(status), 79)

    def crash_restore(self, boundary):
        backup.backup(self.state, self.target)
        (self.state / 'artifacts/result').write_text('later original work')
        with sqlite3.connect(self.state / 'state.sqlite3') as db:
            db.execute('update runtime_metadata set boot_generation=19')
        retained = self.root / 'retained'
        def crash():
            rename = os.rename
            def interrupt(source, target):
                if boundary == 'before_move' and Path(source) == self.state:
                    os._exit(79)
                result = rename(source, target)
                if (boundary == 'after_move' and Path(target) == retained
                        or boundary == 'after_publish' and Path(target) == self.state):
                    os._exit(79)
                return result
            with mock.patch.object(operator_restore.os, 'rename', side_effect=interrupt):
                operator_restore.restore(self.state, self.target, retained)
        self.run_crashing_child(crash)
        return retained

    def test_abrupt_exit_before_directory_move_rolls_back(self):
        retained = self.crash_restore('before_move')
        self.check_rollback(retained)

    def test_abrupt_exit_in_missing_root_gap_rolls_back(self):
        retained = self.crash_restore('after_move')
        self.assertFalse(self.state.exists())
        self.check_rollback(retained)

    def check_rollback(self, retained):
        result = restore_publication.repair(self.state, retained)
        self.assertEqual(result['outcome'], 'original_state_restored')
        self.assertFalse(result['dispatch_started'])
        self.assertEqual((self.state / 'artifacts/result').read_text(), 'later original work')
        self.assertEqual(operator_restore.generation(self.state / 'state.sqlite3'), 19)
        self.assertEqual((self.state / 'runtime.lock').stat().st_nlink, 1)
        self.assertFalse((self.state / restore_publication.MARKER).exists())
        stage = Path(result['preserved_directory'])
        self.assertEqual((stage / 'artifacts/result').read_text(), 'preserve me')
        self.assertTrue((stage / restore_publication.MARKER).exists())
        self.assertFalse((stage / 'runtime.lock').exists())

    def test_abrupt_exit_after_publication_preserves_restored_state(self):
        retained = self.crash_restore('after_publish')
        result = restore_publication.repair(self.state, retained)
        self.assertEqual(result['outcome'], 'published_restore_retained')
        self.assertEqual((self.state / 'artifacts/result').read_text(), 'preserve me')
        self.assertEqual((retained / 'artifacts/result').read_text(), 'later original work')
        self.assertEqual((self.state / 'runtime.lock').stat().st_nlink, 1)
        self.assertEqual(restore_publication.repair(self.state, retained), result)

    def test_repair_can_resume_after_its_own_rename(self):
        retained = self.crash_restore('after_move')
        def crash():
            rename = os.rename
            def interrupt(source, target):
                result = rename(source, target)
                if Path(target) == self.state:
                    os._exit(79)
                return result
            with mock.patch.object(restore_publication.os, 'rename', side_effect=interrupt):
                restore_publication.repair(self.state, retained)
        self.run_crashing_child(crash)
        self.check_rollback(retained)

    def test_live_lock_and_active_database_refuse_without_repair(self):
        retained = self.crash_restore('after_move')
        with (retained / 'runtime.lock').open('rb') as lock:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            with self.assertRaises(BlockingIOError):
                restore_publication.repair(self.state, retained)
        with sqlite3.connect(retained / 'state.sqlite3') as db:
            db.execute("insert into leases values('active')")
        with self.assertRaisesRegex(ValueError, 'lease'):
            restore_publication.repair(self.state, retained)
        self.assertFalse(self.state.exists())
        self.assertEqual((retained / 'runtime.lock').stat().st_nlink, 2)

    def test_replaced_directory_and_lock_are_refused(self):
        retained = self.crash_restore('after_move')
        record = json.loads((retained / restore_publication.MARKER).read_text())
        stage = Path(record['staged'])
        saved = stage.with_name(stage.name + '-saved')
        stage.rename(saved)
        stage.mkdir(mode=0o700)
        with self.assertRaisesRegex(ValueError, 'replaced'):
            restore_publication.repair(self.state, retained)
        stage.rmdir()
        saved.rename(stage)
        (stage / 'runtime.lock').unlink()
        (stage / 'runtime.lock').touch(mode=0o600)
        with self.assertRaisesRegex(ValueError, 'links disagree'):
            restore_publication.repair(self.state, retained)
        self.assertFalse(self.state.exists())

    def test_legacy_wrong_paths_and_unexpected_links_are_refused(self):
        retained = self.crash_restore('after_move')
        marker = retained / restore_publication.MARKER
        original = marker.read_bytes()
        for field, value in [('version', 1), ('restored_to', str(self.root / 'other')),
                             ('retained', str(self.root / 'other')),
                             ('staged', str(self.root / 'outside'))]:
            with self.subTest(field=field):
                record = json.loads(original)
                record[field] = value
                marker.write_text(json.dumps(record))
                with self.assertRaises(ValueError):
                    restore_publication.repair(self.state, retained)
                self.assertFalse(self.state.exists())
                self.assertEqual((retained / 'runtime.lock').stat().st_nlink, 2)
        marker.write_bytes(original)
        os.link(retained / 'runtime.lock', self.root / 'unexpected-link')
        with self.assertRaisesRegex(ValueError, 'link count'):
            restore_publication.repair(self.state, retained)

    def test_occupied_destination_and_corrupt_original_refuse(self):
        retained = self.crash_restore('after_move')
        self.state.symlink_to(self.root / 'missing')
        with self.assertRaises(ValueError):
            restore_publication.repair(self.state, retained)
        self.state.unlink()
        (retained / 'state.sqlite3').write_bytes(b'corrupt original database')
        with self.assertRaises(sqlite3.DatabaseError):
            restore_publication.repair(self.state, retained)
        self.assertFalse(self.state.exists())
        self.assertEqual((retained / 'runtime.lock').stat().st_nlink, 2)

