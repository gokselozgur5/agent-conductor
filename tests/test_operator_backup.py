"""Cold backup refusal and consistency controls; never touch the operator state."""
import fcntl
import importlib.util
import json
import hashlib
import os
from pathlib import Path
import sqlite3
import tempfile
import sys
import unittest
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'tools'))
import operator_restore
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
        self.bins = self.root / 'bins'
        self.bins.mkdir()
        for name in ['conductor-daemon', 'conductorctl', 'conductor-runner']:
            (self.bins / name).write_text('fixture binary')
        (self.state / 'operator.json').write_text(json.dumps({'bin_dir': str(self.bins)}))
        (self.state / 'operator.json').chmod(0o600)
        (self.state / 'board-token').write_text('fixture-only')
        (self.state / 'board-token').chmod(0o600)
        with sqlite3.connect(self.state / 'state.sqlite3') as db:
            db.executescript('''
              CREATE TABLE _sqlx_migrations(version INTEGER,checksum BLOB,success INTEGER);
              CREATE TABLE tasks(state TEXT,id TEXT);
              CREATE TABLE projects(canonical_path TEXT,filesystem_identity TEXT,git_common_dir_identity TEXT);
              CREATE TABLE executions(state TEXT,packet_json TEXT);
              CREATE TABLE conversation_turns(state TEXT);
              CREATE TABLE auxiliary_invocations(state TEXT);
              CREATE TABLE file_brokers(state TEXT);
              CREATE TABLE file_broker_launches(state TEXT);
              CREATE TABLE file_operations(completed_at_ms INTEGER);
              CREATE TABLE legacy_auxiliary_ownership(task_id TEXT);
              CREATE TABLE leases(status TEXT);
              CREATE TABLE effect_intents(status TEXT);
              CREATE TABLE recovery_authority(singleton INTEGER, authority_id TEXT);
              INSERT INTO recovery_authority VALUES(1,NULL);
              CREATE TABLE runtime_metadata(boot_generation INTEGER);
              INSERT INTO runtime_metadata VALUES(5);
              CREATE TABLE workflow_dispatch(status TEXT,plan_json TEXT,task_id TEXT);
            ''')
            db.executemany('insert into _sqlx_migrations values(?,?,1)', [(int(p.name.split('_')[0]),hashlib.sha384(p.read_bytes()).digest()) for p in sorted(backup.MIGRATIONS.glob('*.sql'))])
        self.target = self.root / 'backup'

    def tearDown(self):
        self.temp.cleanup()

    def test_snapshot_preserves_bytes_modes_and_database(self):
        before = backup.inventory(self.state, backup.SKIP)
        result = backup.backup(self.state, self.target)
        self.assertEqual(result['schema'], 37)
        manifest = json.loads((self.target / 'manifest.json').read_text())
        self.assertEqual(manifest['entries'], backup.inventory(self.target / 'state'))
        self.assertEqual(before, backup.inventory(self.state, backup.SKIP))
        self.assertEqual((self.target / 'state/artifacts/result').read_text(), 'preserve me')
        self.assertEqual(self.target.stat().st_mode & 0o777, 0o700)
        with self.assertRaises(FileExistsError):
            backup.backup(self.state, self.target)

    def test_pending_auxiliary_refuses_backup_even_without_live_writer(self):
        with sqlite3.connect(self.state / 'state.sqlite3') as db:
            db.execute("insert into auxiliary_invocations values('pending')")
        with self.assertRaisesRegex(ValueError, 'unresolved auxiliary'):
            backup.backup(self.state, self.target)
        self.assertFalse(self.target.exists())

    def test_native_chat_reply_is_not_a_write_fence(self):
        for state in ['launch_intent', 'unknown', 'failed', 'replied']:
            with self.subTest(state=state):
                with sqlite3.connect(self.state / 'state.sqlite3') as db:
                    db.execute('insert into conversation_turns values(?)', (state,))
                before = backup.inventory(self.state, backup.SKIP)
                with self.assertRaisesRegex(ValueError, 'conversation execution has no verified write fence'):
                    backup.backup(self.state, self.target)
                self.assertEqual(before, backup.inventory(self.state, backup.SKIP))
                self.assertFalse(self.target.exists())
                with sqlite3.connect(self.state / 'state.sqlite3') as db:
                    db.execute('delete from conversation_turns')

    def test_legacy_auxiliary_uncertainty_refuses_cold_backup(self):
        with sqlite3.connect(self.state / 'state.sqlite3') as db:
            db.execute("insert into legacy_auxiliary_ownership values('legacy-task')")
        with self.assertRaisesRegex(ValueError, 'legacy auxiliary ownership unresolved'):
            backup.backup(self.state, self.target)
        self.assertFalse(self.target.exists())

    def test_open_file_broker_refuses_backup_until_closed(self):
        with sqlite3.connect(self.state / 'state.sqlite3') as db:
            db.execute("insert into file_brokers values('open')")
        with self.assertRaisesRegex(ValueError, 'file broker ownership unresolved'):
            backup.backup(self.state, self.target)
        self.assertFalse(self.target.exists())
        with sqlite3.connect(self.state / 'state.sqlite3') as db:
            db.execute("update file_brokers set state='closed'")
        backup.backup(self.state, self.target)
        self.assertTrue(backup.verify(self.target)['verified'])

    def test_prepared_broker_launch_refuses_backup_before_registration(self):
        with sqlite3.connect(self.state / 'state.sqlite3') as db:
            db.execute("insert into file_broker_launches values('open')")
        with self.assertRaisesRegex(ValueError, 'file broker ownership unresolved'):
            backup.backup(self.state, self.target)
        self.assertFalse(self.target.exists())
        with sqlite3.connect(self.state / 'state.sqlite3') as db:
            db.execute("update file_broker_launches set state='closed'")
        backup.backup(self.state, self.target)
        self.assertTrue(backup.verify(self.target)['verified'])

    def test_closed_broker_with_unknown_operation_still_refuses_backup(self):
        with sqlite3.connect(self.state / 'state.sqlite3') as db:
            db.execute("insert into file_brokers values('closed')")
            db.execute("insert into file_operations values(NULL)")
        with self.assertRaisesRegex(ValueError, 'file broker ownership unresolved'):
            backup.backup(self.state, self.target)
        self.assertFalse(self.target.exists())

    def test_wal_export_verifies_and_tampering_is_refused(self):
        with sqlite3.connect(self.state / 'state.sqlite3') as db:
            db.execute('pragma journal_mode=wal')
            db.execute("insert into tasks(state) values('completed')")
            db.commit()
            backup.backup(self.state, self.target)
        self.assertTrue(backup.verify(self.target)['verified'])
        self.assertFalse((self.target / 'state/state.sqlite3-wal').exists())
        self.assertFalse((self.target / 'state/state.sqlite3-shm').exists())
        target = self.target / 'state/artifacts/result'
        target.write_text('changed')
        with self.assertRaises(ValueError):
            backup.verify(self.target)

    @unittest.skipIf(os.getuid() == 0, 'root bypasses directory read permissions')
    def test_unreadable_directory_is_not_silently_omitted(self):
        protected = self.state / 'artifacts'
        protected.chmod(0)
        try:
            with self.assertRaises(PermissionError):
                backup.backup(self.state, self.target)
            self.assertFalse(self.target.exists())
        finally:
            protected.chmod(0o700)

    def test_external_changes_are_separate_from_snapshot_corruption(self):
        backup.backup(self.state, self.target)
        self.assertTrue(backup.verify(self.target, external=True)['external_verified'])
        (self.bins / 'conductor-runner').write_text('changed binary')
        self.assertTrue(backup.verify(self.target)['verified'])
        with self.assertRaises(ValueError):
            backup.verify(self.target, external=True)

    def test_replaced_project_identity_is_refused(self):
        project = self.root / 'project'
        project.mkdir()
        import recovery_dependencies
        with sqlite3.connect(self.state / 'state.sqlite3') as db:
            db.execute('insert into projects values(?,?,NULL)', (str(project), recovery_dependencies.identity(project)))
        backup.backup(self.state, self.target)
        project.rename(self.root / 'retained-project')
        project.mkdir()
        with self.assertRaises(ValueError):
            backup.verify(self.target, external=True)

    def test_restore_retains_current_state_and_generation_floor(self):
        backup.backup(self.state, self.target)
        (self.state / 'artifacts/result').write_text('later work')
        with sqlite3.connect(self.state / 'state.sqlite3') as db:
            db.execute('update runtime_metadata set boot_generation=11')
            db.execute("insert into tasks values('paused','later-task')")
        retained = self.root / 'retained'
        old_token = (self.state / 'board-token').read_bytes()
        rename = os.rename
        lock_checks = []
        def observe_switch(source, target):
            result = rename(source, target)
            if Path(target) == self.state:
                with (self.state / 'runtime.lock').open('rb') as concurrent:
                    with self.assertRaises(BlockingIOError):
                        fcntl.flock(concurrent, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    lock_checks.append(True)
            return result
        with mock.patch.object(operator_restore.os, 'rename', side_effect=observe_switch):
            result = operator_restore.restore(self.state, self.target, retained)
        self.assertEqual(lock_checks, [True])
        self.assertEqual(result['generation_floor'], 11)
        self.assertEqual((self.state / 'artifacts/result').read_text(), 'preserve me')
        self.assertEqual((retained / 'artifacts/result').read_text(), 'later work')
        self.assertNotEqual((self.state / 'board-token').read_bytes(), old_token)
        self.assertEqual((self.state / 'runtime.lock').stat().st_nlink, 1)
        self.assertTrue((retained / 'ARCHIVED_STATE.json').exists())
        with sqlite3.connect(self.state / 'state.sqlite3') as db:
            self.assertEqual(db.execute('select count(*) from tasks').fetchone(), (0,))
            self.assertEqual(db.execute('select boot_generation from runtime_metadata').fetchone(), (11,))
        with sqlite3.connect(retained / 'state.sqlite3') as db:
            self.assertEqual(db.execute('select count(*) from tasks').fetchone(), (1,))

    def test_failed_publication_restores_original_lock_and_state(self):
        backup.backup(self.state, self.target)
        (self.state / 'artifacts/result').write_text('retain on failure')
        rename = os.rename
        def injected(source, target):
            if '.restore-' in Path(source).name:
                raise OSError('injected publish failure')
            return rename(source, target)
        with mock.patch.object(operator_restore.os, 'rename', side_effect=injected):
            with self.assertRaises(OSError):
                operator_restore.restore(self.state, self.target, self.root / 'retained')
        self.assertEqual((self.state / 'artifacts/result').read_text(), 'retain on failure')
        self.assertEqual((self.state / 'runtime.lock').stat().st_nlink, 1)
        self.assertFalse((self.state / 'ARCHIVED_STATE.json').exists())

    def test_restore_refuses_live_owner_and_changed_dependencies(self):
        backup.backup(self.state, self.target)
        with (self.state / 'runtime.lock').open('rb') as lock:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            with self.assertRaises(BlockingIOError):
                operator_restore.restore(self.state, self.target, self.root / 'retained')
        (self.bins / 'conductor-daemon').write_text('replaced')
        with self.assertRaises(ValueError):
            operator_restore.restore(self.state, self.target, self.root / 'retained')
        self.assertFalse((self.root / 'retained').exists())

    def test_restore_refuses_missing_current_database_and_legacy_snapshot(self):
        with self.assertRaises(ValueError):
            operator_restore.restore(self.state, self.root, self.root / 'retained')
        backup.backup(self.state, self.target)
        (self.state / 'state.sqlite3').rename(self.state / 'state.retained')
        with self.assertRaises(FileNotFoundError):
            operator_restore.restore(self.state, self.target, self.root / 'retained')
        self.assertFalse((self.root / 'retained').exists())
        (self.state / 'state.retained').rename(self.state / 'state.sqlite3')
        manifest_path = self.target / 'manifest.json'
        manifest = json.loads(manifest_path.read_text())
        manifest['version'] = 1
        manifest.pop('dependencies')
        manifest_path.write_text(json.dumps(manifest))
        self.assertTrue(backup.verify(self.target)['verified'])
        with self.assertRaises(ValueError):
            operator_restore.restore(self.state, self.target, self.root / 'retained')

    def test_missing_dependency_entry_cannot_bypass_external_verification(self):
        backup.backup(self.state, self.target)
        path = self.target / 'manifest.json'
        value = json.loads(path.read_text())
        value['dependencies'].pop()
        path.write_text(json.dumps(value))
        with self.assertRaises(ValueError):
            backup.verify(self.target, external=True)

    def test_restore_preserves_excluded_state_local_executables(self):
        local = self.state / 'bin'
        self.bins.rename(local)
        (self.state / 'operator.json').write_text(json.dumps({'bin_dir': str(local)}))
        backup.backup(self.state, self.target)
        self.assertFalse((self.target / 'state/bin').exists())
        operator_restore.restore(self.state, self.target, self.root / 'retained')
        for name in ['conductor-daemon', 'conductorctl', 'conductor-runner']:
            self.assertEqual((local / name).read_text(), 'fixture binary')
        self.assertTrue(backup.verify(self.target, external=True)['external_verified'])

    def test_post_publication_failure_reports_that_state_was_restored(self):
        backup.backup(self.state, self.target)
        retained = self.root / 'retained'
        unlink = Path.unlink
        def fail_archive_lock(path, *args, **kwargs):
            if path == retained / 'runtime.lock':
                raise OSError('injected archive housekeeping failure')
            return unlink(path, *args, **kwargs)
        with mock.patch.object(Path, 'unlink', fail_archive_lock):
            with self.assertRaisesRegex(ValueError, 'was published'):
                operator_restore.restore(self.state, self.target, retained)
        self.assertTrue((self.state / 'operator.json').exists())
        self.assertTrue((retained / 'ARCHIVED_STATE.json').exists())
        self.assertEqual((self.state / 'runtime.lock').stat().st_nlink, 2)

    def test_live_lock_refused_without_output(self):
        with (self.state / 'runtime.lock').open('rb') as lock:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            with self.assertRaises(BlockingIOError):
                backup.backup(self.state, self.target)
        self.assertFalse(self.target.exists())

    def test_unfenced_and_active_work_refused(self):
        for table, sql in [('executions', "insert into executions values('exited','{}')"),
                           ('tasks', "insert into tasks(state) values('running')"),
                           ('leases', "insert into leases values('active')"),
                           ('effect_intents', "insert into effect_intents values('pending')"),
                           ('workflow_dispatch', "insert into workflow_dispatch(status,plan_json) values('running','{}')")]:
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
            db.execute('insert into workflow_dispatch(status,plan_json) values(?,?)', ('queued', json.dumps({'artifacts': str(self.root / 'elsewhere')})))
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



@unittest.skipUnless(os.environ.get('CONDUCTOR_TEST_BIN_DIR'), 'requires built Rust operator binaries')
class OperatorRecoveryIntegration(unittest.TestCase):
    def test_real_operator_restore_and_token_rotation(self):
        import signal
        import socket
        import subprocess
        import time
        import urllib.request
        import urllib.error
        repo = Path(__file__).resolve().parents[1]
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary).resolve()
            project = root / 'project'
            project.mkdir()
            (project / 'README.md').write_text('Recovery fixture\n')
            for args in [['init', '-q'], ['add', '.'], ['-c', 'user.name=Fixture', '-c', 'user.email=fixture@example.invalid', '-c', 'commit.gpgsign=false', '-c', 'core.hooksPath=/dev/null', 'commit', '-qm', 'fixture']]:
                subprocess.run(['git', '-C', str(project), *args], check=True, capture_output=True)
            with socket.socket() as available:
                available.bind(('127.0.0.1', 0))
                port = available.getsockname()[1]
            state, exported, retained = root / 'state', root / 'snapshot', root / 'retained'
            command = [sys.executable, str(repo / 'tools/conductor'), '--state', str(state)]
            def call(*args, success=True):
                result = subprocess.run(command + list(args), capture_output=True, text=True, timeout=60)
                self.assertEqual(result.returncode == 0, success, result.stderr)
                return result.stdout
            call('init', '--project', str(project), '--bin-dir', os.environ['CONDUCTOR_TEST_BIN_DIR'], '--port', str(port))
            process = None
            def start():
                child = subprocess.Popen(command + ['run'], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                deadline = time.monotonic() + 15
                try:
                    while time.monotonic() < deadline:
                        self.assertIsNone(child.poll())
                        result = subprocess.run(command + ['status'], capture_output=True, text=True, timeout=5)
                        if result.returncode == 0 and json.loads(result.stdout)['health']['repository_watcher'] == 'running':
                            return child
                        time.sleep(.1)
                    self.fail('operator readiness deadline')
                except BaseException:
                    if child.poll() is None:
                        child.send_signal(signal.SIGINT)
                        child.wait(timeout=15)
                    raise
            def stop(child):
                child.send_signal(signal.SIGINT)
                self.assertEqual(child.wait(timeout=15), 0)
            try:
                process = start()
                call('backup', str(exported), success=False)
                self.assertFalse(exported.exists())
                stop(process); process = None
                call('backup', str(exported))
                call('verify-backup', str(exported), '--external')
                process = start()
                stop(process); process = None
                floor = operator_restore.generation(state / 'state.sqlite3')
                old_token = (state / 'board-token').read_text().strip()
                (state / 'operator.json').rename(state / 'operator.retained')
                receipt = json.loads(call('restore', str(exported), '--retain-current', str(retained)))
                self.assertEqual(receipt['generation_floor'], floor)
                self.assertTrue((retained / 'operator.retained').exists())
                process = start()
                health = json.loads(call('status'))['health']
                self.assertGreater(health['boot_generation'], floor)
                opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
                url = f'http://127.0.0.1:{port}/api/health'
                with self.assertRaises(urllib.error.HTTPError) as rejected:
                    opener.open(urllib.request.Request(url, headers={'Authorization': 'Bearer ' + old_token}), timeout=5)
                self.assertEqual(rejected.exception.code, 401)
                new_token = (state / 'board-token').read_text().strip()
                with opener.open(urllib.request.Request(url, headers={'Authorization': 'Bearer ' + new_token}), timeout=5) as response:
                    self.assertEqual(json.load(response)['status'], 'ready')
                self.assertTrue(json.loads(call('ctl', '--json', 'check-invariants'))['result']['payload']['healthy'])
                stop(process); process = None
                # Exercise the repair CLI against a real migrated database, then
                # reopen the actual daemon after both possible directory outcomes.
                for boundary in ('after_move', 'after_publish'):
                    exported = root / ('snapshot-' + boundary)
                    retained = root / ('retained-' + boundary)
                    call('backup', str(exported))
                    script = '''
import os, sys
from pathlib import Path
sys.path.insert(0, sys.argv[1])
import operator_restore
state, source, retained = map(Path, sys.argv[2:5])
rename = os.rename
def interrupt(source, target):
    result = rename(source, target)
    if Path(target) == (retained if sys.argv[5] == 'after_move' else state):
        os._exit(79)
    return result
operator_restore.os.rename = interrupt
operator_restore.restore(state, source, retained)
'''
                    interrupted = subprocess.run([sys.executable, '-c', script, str(repo / 'tools'),
                                                  str(state), str(exported), str(retained), boundary],
                                                 capture_output=True, timeout=60)
                    self.assertEqual(interrupted.returncode, 79, interrupted.stderr)
                    if boundary == 'after_move':
                        self.assertFalse(state.exists())
                    repair = json.loads(call('repair-restore', '--retained', str(retained)))
                    self.assertFalse(repair['dispatch_started'])
                    self.assertEqual(repair['outcome'], 'original_state_restored' if boundary == 'after_move'
                                     else 'published_restore_retained')
                    process = start()
                    self.assertTrue(json.loads(call('ctl', '--json', 'check-invariants'))['result']['payload']['healthy'])
                    if boundary == 'after_publish':
                        call('repair-restore', '--retained', str(retained), success=False)
                    stop(process); process = None
                import recovery_authority
                authority_root = recovery_authority.root_for(state)
                authority_record = authority_root / 'authority.json'
                high_water = json.loads(authority_record.read_text())['generation_high_water']
                with sqlite3.connect(state / 'state.sqlite3') as db:
                    db.execute('update runtime_metadata set boot_generation=1')
                process = start()
                self.assertGreater(json.loads(call('status'))['health']['boot_generation'], high_water)
                stop(process); process = None
                generation_before_refusal = operator_restore.generation(state / 'state.sqlite3')
                saved_record = authority_root / 'record-retained.json'
                authority_record.rename(saved_record)
                call('run', success=False)
                saved_record.rename(authority_record)
                with (authority_root / 'ownership.lock').open('rb') as lock:
                    fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    call('run', success=False)
                    call('backup', str(root / 'refused-backup'), success=False)
                self.assertEqual(operator_restore.generation(state / 'state.sqlite3'), generation_before_refusal)
                archived = subprocess.run([
                    str(Path(os.environ['CONDUCTOR_TEST_BIN_DIR']) / 'conductor-daemon'),
                    '--database', str(retained / 'state.sqlite3'),
                    '--socket', str(retained / 'control.sock'), '--lock', str(retained / 'runtime.lock'),
                ], capture_output=True, text=True, timeout=15)
                self.assertNotEqual(archived.returncode, 0)
                self.assertIn('authority binding', archived.stderr)
                (state / 'state.sqlite3').rename(state / 'database-retained')
                call('run', success=False)
                self.assertFalse((state / 'state.sqlite3').exists())
                (state / 'database-retained').rename(state / 'state.sqlite3')
            finally:
                if process is not None and process.poll() is None:
                    stop(process)


if __name__ == '__main__':
    unittest.main()
