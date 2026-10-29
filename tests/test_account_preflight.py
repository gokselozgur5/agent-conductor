"""Account setup inspection neither launches vendors nor consumes socket clients."""
import copy
import hashlib
import os
from pathlib import Path
import pwd
import socket
import sys
import tempfile
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'tools'))
from account_preflight import check  # noqa: E402


class AccountPreflightTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()
        user = pwd.getpwuid(os.getuid())
        executable = Path(sys.executable).resolve()
        pin = dict(kind='codex', version='fixture', executable=str(executable),
                   executable_sha256=hashlib.sha256(executable.read_bytes()).hexdigest(),
                   conformance_sha256='a' * 64)
        self.template = dict(worker=pin, verifier=copy.deepcopy(pin),
                             full_capability=dict(account=dict(name=user.pw_name,
                                 uid=user.pw_uid, gid=user.pw_gid, home=user.pw_dir),
                                 path='/usr/bin:/bin'),
                             account_worker=dict(socket=str(self.root / 'worker.sock'),
                                                 artifacts=str(self.root)))

    def test_absent_receiver_is_worker_setup_only(self):
        before = self.root.stat()
        result = check(self.template, 'worker')
        self.assertEqual(result['endpoint'], 'absent')
        self.assertEqual(list(self.root.iterdir()), [])
        self.assertEqual(self.root.stat().st_mode, before.st_mode)
        with self.assertRaisesRegex(ValueError, 'absent'):
            check(self.template, 'coordinator')

    def test_existing_socket_is_not_connected_or_certified_live(self):
        with socket.socket(socket.AF_UNIX) as listener:
            listener.bind(self.template['account_worker']['socket'])
            listener.listen(1)
            listener.settimeout(.05)
            for role in ('worker', 'coordinator'):
                result = check(self.template, role)
                self.assertEqual(result['endpoint'], 'present_unverified')
                self.assertFalse(result['connection_attempted'])
                self.assertEqual(result['vendor_login'], 'not_checked')
            with self.assertRaises(TimeoutError):
                listener.accept()

    def test_stale_identity_pin_and_relative_path_are_rejected(self):
        for key, value in [('home', '/stale-home'), ('uid', os.getuid() + 1)]:
            candidate = copy.deepcopy(self.template)
            candidate['full_capability']['account'][key] = value
            with self.assertRaisesRegex(ValueError, 'metadata'):
                check(candidate, 'worker')
        self.template['worker']['executable_sha256'] = '0' * 64
        with self.assertRaisesRegex(ValueError, 'binary changed'):
            check(self.template, 'worker')
        self.template['full_capability']['path'] = '/usr/bin:'
        with self.assertRaisesRegex(ValueError, 'PATH'):
            check(self.template, 'worker')

    def test_file_and_symlink_do_not_count_as_receiver(self):
        endpoint = Path(self.template['account_worker']['socket'])
        endpoint.write_text('do not overwrite')
        with self.assertRaisesRegex(ValueError, 'not a socket'):
            check(self.template, 'worker')
        self.assertEqual(endpoint.read_text(), 'do not overwrite')
        link = self.root / 'alias.sock'
        link.symlink_to(endpoint)
        self.template['account_worker']['socket'] = str(link)
        with self.assertRaisesRegex(ValueError, 'not a socket'):
            check(self.template, 'worker')

    def test_coordinator_does_not_open_foreign_worker_binary(self):
        self.template['worker']['executable'] = '/unreadable/foreign-account/vendor'
        with socket.socket(socket.AF_UNIX) as listener:
            listener.bind(self.template['account_worker']['socket'])
            self.assertEqual(check(self.template, 'coordinator')['local_checks'], 'passed')
