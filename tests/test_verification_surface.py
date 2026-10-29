import hashlib
import json
from pathlib import Path
import sys
import tempfile
import time
import unittest
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'tools'))
from native_observation_guard import Violation
from verification_surface import verify_snapshot


class VerificationSurfaceTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
        self.root=Path(self.temp.name);(self.root/'tree').mkdir()
        self.file=self.root/'tree/answer';self.file.write_bytes(b'42\n')
        self.manifest=dict(version=1,entries=[dict(path='answer',sha256=hashlib.sha256(b'42\n').hexdigest(),bytes=3,executable=False),dict(path='deleted',sha256=None,bytes=0,executable=False)])
        self.publish()

    def publish(self):
        raw=json.dumps(self.manifest,separators=(',',':')).encode()
        (self.root/'manifest.json').write_bytes(raw)
        self.source='snapshot:'+hashlib.sha256(raw).hexdigest()

    def check(self):return verify_snapshot(self.root,self.source,time.monotonic()+5)

    def test_source_binding_refuses_tampering_before_first_observation(self):
        self.assertEqual(self.check(),self.check())
        self.file.write_bytes(b'43\n')
        with self.assertRaisesRegex(Violation,'verification_file_changed'):self.check()
        self.file.write_bytes(b'42\n')
        (self.root/'manifest.json').write_bytes(b'{}')
        with self.assertRaisesRegex(Violation,'verification_manifest_mismatch'):self.check()

    def test_extra_deleted_and_executable_changes_are_detected(self):
        (self.root/'tree/extra').write_text('extra')
        with self.assertRaisesRegex(Violation,'verification_inventory_changed'):self.check()
        (self.root/'tree/extra').unlink()
        (self.root/'tree/deleted').write_text('restored')
        with self.assertRaisesRegex(Violation,'verification_deletion_changed'):self.check()
        (self.root/'tree/deleted').unlink();self.file.chmod(0o755)
        with self.assertRaisesRegex(Violation,'verification_file_metadata_changed'):self.check()

    def test_links_and_escaping_manifest_paths_refuse(self):
        self.file.unlink();self.file.symlink_to(self.root/'manifest.json')
        with self.assertRaisesRegex(Violation,'verification_link_or_escape'):self.check()
        self.file.unlink();self.file.write_bytes(b'42\n')
        self.manifest['entries'][0]['path']='../outside';self.publish()
        with self.assertRaisesRegex(Violation,'verification_path_invalid'):self.check()


if __name__=='__main__':unittest.main()
