"""Simulated boot transitions are unit evidence, never physical reboot evidence."""
import json
import os
from pathlib import Path
import sqlite3
import unittest
from unittest import mock
import test_operator_backup as fixtures
from test_operator_backup import backup
import disaster_recovery as recovery
import recovery_authority as authority


class DisasterRecoveryTests(unittest.TestCase):
    def setUp(self):
        fixtures.BackupTests.setUp(self)
        self.host = {'platform': 'macos', 'host_id': 'a' * 64, 'boot_id': 'b' * 64}
        self.rebooted = dict(self.host, boot_id='d' * 64)
        self.record = {'version': 2, 'database': str(self.state / 'state.sqlite3'),
                       'authority_id': 'c' * 64, 'generation_high_water': 5, 'host': self.host}
        self.anchor = authority.root_for(self.state)
        self.anchor.mkdir(mode=0o700)
        (self.anchor / 'ownership.lock').touch(mode=0o600)
        (self.anchor / 'authority.json').write_text(json.dumps(self.record))
        (self.anchor / 'authority.json').chmod(0o600)
        with sqlite3.connect(self.state / 'state.sqlite3') as db:
            db.execute('update recovery_authority set authority_id=?', (self.record['authority_id'],))
        backup.backup(self.state, self.target)
        self.retained = self.root / 'retained'
        self.reader = mock.patch.object(recovery, 'host_epoch', return_value=self.host)
        self.reader_mock = self.reader.start()
        self.addCleanup(self.reader.stop)

    tearDown = fixtures.BackupTests.tearDown

    def prepare(self):
        return recovery.prepare(self.state, self.target, self.retained)

    def reboot(self):
        self.reader_mock.return_value = self.rebooted

    def test_same_boot_is_refused_and_preparation_is_idempotent(self):
        before = backup.inventory(self.state, backup.SKIP)
        first = self.prepare()
        self.assertTrue(first['host_restart_required'])
        self.assertEqual(self.prepare(), first)
        with self.assertRaisesRegex(ValueError, 'host restart required'):
            recovery.finish(self.state)
        with self.assertRaisesRegex(ValueError, 'quarantined'):
            backup.backup(self.state, self.root / 'blocked-backup')
        self.assertEqual(backup.inventory(self.state, backup.SKIP), before)
        self.assertFalse(self.retained.exists())

    def test_corrupt_current_database_is_retained_after_simulated_reboot(self):
        (self.state / 'state.sqlite3').write_bytes(b'preserve corrupt current bytes')
        self.prepare(); self.reboot()
        result = recovery.finish(self.state)
        self.assertTrue(result['recovered'])
        self.assertFalse(result['dispatch_started'])
        self.assertEqual((self.retained / 'state.sqlite3').read_bytes(), b'preserve corrupt current bytes')
        self.assertEqual((self.state / 'artifacts/result').read_text(), 'preserve me')
        self.assertNotEqual((self.state / 'board-token').read_text(), 'fixture-only')
        self.assertIsNone(authority.read(self.state)['recovery'])
        self.assertTrue(recovery.finish(self.state)['already_completed'])

    def test_missing_original_state_recovers_from_snapshot(self):
        self.state.rename(self.root / 'independent-retained-history')
        self.prepare(); self.reboot()
        result = recovery.finish(self.state)
        self.assertIsNone(result['retained'])
        self.assertEqual((self.state / 'artifacts/result').read_text(), 'preserve me')
        self.assertEqual(authority.binding(self.state), self.record['authority_id'])

    def test_other_host_and_tampered_snapshot_remain_quarantined(self):
        self.prepare()
        self.reader_mock.return_value = dict(self.rebooted, host_id='e' * 64)
        with self.assertRaisesRegex(ValueError, 'another host'):
            recovery.finish(self.state)
        self.reboot()
        (self.target / 'state/artifacts/result').write_text('changed')
        with self.assertRaises(ValueError):
            recovery.finish(self.state)
        self.assertIsNotNone(authority.read(self.state)['recovery'])
        self.assertFalse(self.retained.exists())

    def test_failed_directory_publication_resumes_with_original_bytes(self):
        (self.state / 'artifacts/result').write_text('later original')
        self.prepare(); self.reboot()
        rename = os.rename
        def fail(source, destination):
            if Path(destination) == self.state:
                raise OSError('injected publish failure')
            return rename(source, destination)
        with mock.patch.object(recovery.os, 'rename', side_effect=fail):
            with self.assertRaises(OSError):
                recovery.finish(self.state)
        self.assertFalse(self.state.exists())
        self.assertEqual((self.retained / 'artifacts/result').read_text(), 'later original')
        result = recovery.finish(self.state)
        self.assertTrue(result['recovered'])
        self.assertEqual((self.state / 'artifacts/result').read_text(), 'preserve me')

    def test_candidate_tampering_and_cancel_after_staging_are_refused(self):
        self.prepare(); self.reboot()
        with mock.patch.object(recovery, 'publish', side_effect=OSError('stop before publish')):
            with self.assertRaises(OSError): recovery.finish(self.state)
        ticket = authority.read(self.state)['recovery']
        (Path(ticket['stage']) / 'artifacts/result').write_text('changed')
        with self.assertRaisesRegex(ValueError, 'candidate changed'):
            recovery.finish(self.state)
        with self.assertRaisesRegex(ValueError, 'staged'):
            recovery.cancel(self.state)
        self.assertFalse(self.retained.exists())

    def test_state_local_binary_prerequisites_survive_publication_gap(self):
        local = self.state / 'bin'
        self.bins.rename(local)
        (self.state / 'operator.json').write_text(json.dumps({'bin_dir': str(local)}))
        self.target = self.root / 'local-binary-snapshot'
        backup.backup(self.state, self.target)
        self.prepare(); self.reboot()
        rename = os.rename
        def fail(source, destination):
            if Path(destination) == self.state:
                raise OSError('injected gap')
            return rename(source, destination)
        with mock.patch.object(recovery.os, 'rename', side_effect=fail):
            with self.assertRaises(OSError): recovery.finish(self.state)
        self.assertFalse(self.state.exists())
        self.assertTrue(recovery.finish(self.state)['recovered'])
        self.assertEqual((local / 'conductor-daemon').read_text(), 'fixture binary')

    def test_cancel_does_not_grant_a_recovery_or_change_original(self):
        before = backup.inventory(self.state, backup.SKIP)
        self.prepare()
        self.assertTrue(recovery.cancel(self.state)['cancelled'])
        self.assertEqual(backup.inventory(self.state, backup.SKIP), before)
        with self.assertRaisesRegex(ValueError, 'no prepared recovery'):
            recovery.finish(self.state)
        self.assertTrue(backup.verify(self.target, external=True)['verified'])

    def test_existing_archive_guard_is_preserved_without_rewriting_history(self):
        marker = self.state / 'ARCHIVED_STATE.json'
        marker.write_text(json.dumps({'version': 2, 'dispatch_forbidden': True, 'earlier': 'preserve'}))
        marker.chmod(0o600)
        before = marker.read_bytes()
        (self.state / 'state.sqlite3').write_bytes(b'corrupt archived database')
        self.prepare(); self.reboot()
        recovery.finish(self.state)
        self.assertEqual((self.retained / marker.name).read_bytes(), before)

    def test_incomplete_candidate_is_preserved_and_never_published(self):
        self.prepare(); self.reboot()
        old_stage = Path(authority.read(self.state)['recovery']['stage'])
        def fail(*args):
            (old_stage / 'partial-copy').write_text('keep diagnostic bytes')
            raise OSError('interrupted copying')
        with mock.patch.object(recovery.publication, 'populate_stage', side_effect=fail):
            with self.assertRaises(OSError): recovery.finish(self.state)
        self.assertEqual(authority.read(self.state)['recovery']['phase'], 'prepared')
        recovery.finish(self.state)
        self.assertEqual((old_stage / 'partial-copy').read_text(), 'keep diagnostic bytes')
        self.assertFalse((self.state / 'partial-copy').exists())

    def test_exhausted_generation_does_not_prepare_an_unstartable_recovery(self):
        record = authority.read(self.state)
        record['generation_high_water'] = 9223372036854775806
        authority.persist(self.state, record)
        with self.assertRaisesRegex(ValueError, 'cannot safely advance'):
            self.prepare()
        self.assertIsNone(authority.read(self.state).get('recovery'))
        self.assertFalse(self.retained.exists())

    def test_abrupt_exit_after_publication_can_finish_without_republishing(self):
        self.prepare(); self.reboot()
        child = os.fork()
        if child == 0:
            try:
                rename = os.rename
                def interrupt(source, destination):
                    result = rename(source, destination)
                    if Path(destination) == self.state:
                        os._exit(79)
                    return result
                with mock.patch.object(recovery.os, 'rename', side_effect=interrupt):
                    recovery.finish(self.state)
            except BaseException:
                import traceback
                traceback.print_exc()
            os._exit(80)
        _, status = os.waitpid(child, 0)
        self.assertEqual(os.waitstatus_to_exitcode(status), 79)
        inode = self.state.stat().st_ino
        self.assertIsNotNone(authority.read(self.state)['recovery'])
        self.assertTrue(recovery.finish(self.state)['recovered'])
        self.assertEqual(self.state.stat().st_ino, inode)
