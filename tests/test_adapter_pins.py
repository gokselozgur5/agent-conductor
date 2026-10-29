"""Remote pin handling must not skip independent local verifier validation."""
import hashlib
from pathlib import Path
import sys
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'tools'))
from adapter_pins import accepted_pins


class AdapterPinTests(unittest.TestCase):
    def setUp(self):
        executable = Path(sys.executable).resolve()
        self.pin = dict(kind='codex', version='fixture', executable=str(executable),
                        executable_sha256=hashlib.sha256(executable.read_bytes()).hexdigest(),
                        conformance_sha256='a' * 64)

    def test_remote_selection_still_checks_verifier_bytes(self):
        template = dict(worker=dict(self.pin, executable='/private-worker/vendor'),
                        verifier=dict(self.pin, executable_sha256='0' * 64),
                        account_worker={}, full_capability={})
        with self.assertRaisesRegex(ValueError, 'binary changed'):
            accepted_pins(template)

    def test_remote_selection_requires_profile_and_valid_pin(self):
        template = dict(worker=self.pin, verifier=self.pin, account_worker={})
        with self.assertRaisesRegex(ValueError, 'requires full capability'):
            accepted_pins(template)
        template['full_capability'] = {}
        for path, digest in [('relative/vendor', 'a' * 64),
                             ('/private/../vendor', 'a' * 64), ('/vendor', 'bad')]:
            template['worker'] = dict(self.pin, executable=path, executable_sha256=digest)
            with self.subTest(path=path, digest=digest), self.assertRaises(ValueError):
                accepted_pins(template)

    def test_local_worker_still_requires_existing_binary(self):
        with self.assertRaises(FileNotFoundError):
            accepted_pins(dict(worker=dict(self.pin, executable='/missing/vendor'),
                               verifier=self.pin))
