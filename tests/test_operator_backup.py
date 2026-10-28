"""Cold backup refusal and consistency controls; never touch the operator state."""
import fcntl
import importlib.util
import json
import hashlib
import os
from pathlib import Path
import sqlite3
import tempfile
import unittest

spec = importlib.util.spec_from_file_location('operator_backup', Path(__file__).resolve().parents[1] / 'tools/operator_backup.py')
backup = importlib.util.module_from_spec(spec)
spec.loader.exec_module(backup)


class BackupTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name).resolve()
        self.state = self.root / 'operator'
        self.state.mkdir(mode=0o700)
        (self.state / 'runtime.lock').touch(mode=0o600)
        (self.state / 'artifacts').mkdir(mode=0o700)
        (self.state / 'artifacts/result').write_text('preserve me')
        (self.state / 'plans').mkdir(mode=0o700)
        (self.state / 'operator.json').write_text('{}')
        (self.state / 'operator.json').chmod(0o600)
        (self.state / 'board-token').write_text('fixture-only')
        (self.state / 'board-token').chmod(0o600)
        with sqlite3.connect(self.state / 'state.sqlite3') as db:
            db.executescript('''
              CREATE TABLE _sqlx_migrations(version INTEGER,checksum BLOB,success INTEGER);
              CREATE TABLE tasks(state TEXT);
              CREATE TABLE executions(state TEXT,packet_json TEXT);
              CREATE TABLE workflow_dispatch(status TEXT,plan_json TEXT);
            ''')
            db.executemany('insert into _sqlx_migrations values(?,?,1)', [(int(p.name.split('_')[0]),hashlib.sha384(p.read_bytes()).digest()) for p in sorted(backup.MIGRATIONS.glob('*.sql'))])
        self.target = self.root / 'backup'

    def tearDown(self):
        self.temp.cleanup()

    def test_snapshot_preserves_bytes_modes_and_database(self):
        before = backup.inventory(self.state, backup.SKIP)
        backup.backup(self.state, self.target)
        manifest = json.loads((self.target / 'manifest.json').read_text())
        self.assertEqual(manifest['entries'], backup.inventory(self.target / 'state'))
        self.assertEqual(before, backup.inventory(self.state, backup.SKIP))
        self.assertEqual((self.target / 'state/artifacts/result').read_text(), 'preserve me')
        self.assertEqual(self.target.stat().st_mode & 0o777, 0o700)
        with self.assertRaises(FileExistsError):
            backup.backup(self.state, self.target)

    def test_wal_export_verifies_and_tampering_is_refused(self):
        with sqlite3.connect(self.state / 'state.sqlite3') as db:
            db.execute('pragma journal_mode=wal')
            db.execute("insert into tasks values('completed')")
            db.commit()
            backup.backup(self.state, self.target)
        self.assertTrue(backup.verify(self.target)['verified'])
        self.assertFalse((self.target / 'state/state.sqlite3-wal').exists())
        self.assertFalse((self.target / 'state/state.sqlite3-shm').exists())
        target = self.target / 'state/artifacts/result'
        target.write_text('changed')
        with self.assertRaises(ValueError):
            backup.verify(self.target)

    def test_live_lock_refused_without_output(self):
        with (self.state / 'runtime.lock').open('rb') as lock:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            with self.assertRaises(BlockingIOError):
                backup.backup(self.state, self.target)
        self.assertFalse(self.target.exists())

    def test_unfenced_and_active_work_refused(self):
        for table, sql in [('executions', "insert into executions values('exited','{}')"),
                           ('tasks', "insert into tasks values('running')"),
                           ('workflow_dispatch', "insert into workflow_dispatch values('running','{}')")]:
            with sqlite3.connect(self.state / 'state.sqlite3') as db:
                db.execute(sql)
            with self.assertRaises(ValueError):
                backup.backup(self.state, self.target)
            self.assertFalse(self.target.exists())
            with sqlite3.connect(self.state / 'state.sqlite3') as db:
                db.execute('delete from ' + table)

    def test_links_and_external_artifacts_refused(self):
        link = self.state / 'escape'
        link.symlink_to(self.root)
        with self.assertRaises(ValueError):
            backup.backup(self.state, self.target)
        link.unlink()
        with sqlite3.connect(self.state / 'state.sqlite3') as db:
            db.execute('insert into workflow_dispatch values(?,?)', ('queued', json.dumps({'artifacts': str(self.root / 'elsewhere')})))
        with self.assertRaises(ValueError):
            backup.backup(self.state, self.target)
        self.assertFalse(self.target.exists())

    def test_unsupported_schema_and_nested_destination_refused(self):
        with self.assertRaises(ValueError):
            backup.backup(self.state, self.state / 'backup')
        with sqlite3.connect(self.state / 'state.sqlite3') as db:
            db.execute('update _sqlx_migrations set version=999')
        with self.assertRaises(ValueError):
            backup.backup(self.state, self.target)
        self.assertFalse(self.target.exists())


if __name__ == '__main__':
    unittest.main()
