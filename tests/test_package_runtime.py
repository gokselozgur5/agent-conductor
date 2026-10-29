"""Runtime kits contain tracked distribution files, never ignored local state."""
import hashlib
import json
from pathlib import Path
import subprocess
import sys
import tarfile
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'tools'))
import package_runtime


class RuntimePackageTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.repo = self.root / 'repo'
        self.repo.mkdir()
        self.patch = patch.object(package_runtime, 'REPO', self.repo)
        self.patch.start()
        self.addCleanup(self.patch.stop)
        subprocess.run(['git', 'init', '-q', str(self.repo)], check=True)
        for name, value in {'.gitignore': 'state/\ntools/private.py\n',
                            'tools/conductor': 'print("fixture")\n',
                            'docs/LOCAL_OPERATOR.md': 'Operator fixture\n',
                            'crates/store/migrations/0001.sql': 'SELECT 1;\n'}.items():
            path = self.repo / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(value)
        self.commit()
        self.bin = self.root / 'bin'
        self.bin.mkdir()
        for name in package_runtime.BINARIES:
            path = self.bin / name
            path.write_text('#!/bin/sh\nexit 0\n')
            path.chmod(0o755)

    def commit(self):
        subprocess.run(['git', '-C', str(self.repo), 'add', '.'], check=True)
        subprocess.run(['git', '-C', str(self.repo), '-c', 'user.name=Fixture',
                        '-c', 'user.email=fixture@example.invalid', '-c', 'commit.gpgsign=false',
                        'commit', '-qm', 'Fixture'], check=True)

    def test_archive_hashes_modes_and_exclusion_of_ignored_state(self):
        (self.repo / 'state').mkdir()
        (self.repo / 'state/token').write_text('fixture-private-value')
        (self.repo / 'tools/private.py').write_text('fixture-private-value')
        result = package_runtime.package(self.bin, self.root / 'kit')
        with tarfile.open(result['archive']) as archive:
            manifest = json.load(archive.extractfile('kit/manifest.json'))
            names = {entry.name for entry in archive.getmembers() if entry.isfile()}
            self.assertEqual(names, {'kit/manifest.json'} | {'kit/' + n for n in manifest['files']})
            self.assertFalse(manifest['vendor_pins_included'])
            self.assertNotIn('tools/private.py', manifest['files'])
            self.assertFalse(any('state/' in n for n in manifest['files']))
            for name, expected in manifest['files'].items():
                self.assertEqual(hashlib.sha256(archive.extractfile('kit/' + name).read()).hexdigest(), expected)
            self.assertEqual(archive.getmember('kit/bin/conductor-daemon').mode, 0o755)
        repeated = package_runtime.package(self.bin, self.root / 'second/kit')
        self.assertEqual(result['archive_sha256'], repeated['archive_sha256'])
        self.assertEqual(package_runtime.verify(self.root / 'kit')['files_verified'], result['files'])
        (self.root / 'kit/tools/conductor').write_text('changed after packaging')
        with self.assertRaisesRegex(ValueError, 'content differs'):
            package_runtime.verify(self.root / 'kit')

    def test_extra_files_and_symlinks_cannot_hide_from_verification(self):
        package_runtime.package(self.bin, self.root / 'kit')
        (self.root / 'kit/unexpected').write_text('extra')
        with self.assertRaisesRegex(ValueError, 'file set differs'):
            package_runtime.verify(self.root / 'kit')
        package_runtime.package(self.bin, self.root / 'linked-kit')
        (self.root / 'linked-kit/alias').symlink_to(self.repo)
        with self.assertRaisesRegex(ValueError, 'symlinks'):
            package_runtime.verify(self.root / 'linked-kit')

    def test_dirty_source_and_tracked_symlinks_refuse_before_publication(self):
        source = self.repo / 'tools/conductor'
        source.write_text('changed')
        with self.assertRaisesRegex(ValueError, 'clean checkout'):
            package_runtime.package(self.bin, self.root / 'dirty')
        self.assertFalse((self.root / 'dirty').exists())
        self.commit()
        (self.repo / 'docs/link').symlink_to(source)
        self.commit()
        with self.assertRaisesRegex(ValueError, 'regular tracked file'):
            package_runtime.package(self.bin, self.root / 'linked')
        self.assertFalse((self.root / 'linked').exists())

    def test_manifest_symlink_is_rejected_before_reading_its_target(self):
        package_runtime.package(self.bin, self.root / 'kit')
        manifest = self.root / 'kit/manifest.json'
        manifest.rename(self.root / 'saved-manifest.json')
        manifest.symlink_to(self.repo / 'tools/conductor')
        with self.assertRaisesRegex(ValueError, 'manifest must be a regular file'):
            package_runtime.verify(self.root / 'kit')

    def test_missing_binary_and_existing_destination_are_preserved(self):
        executable = self.bin / package_runtime.BINARIES[0]
        executable.chmod(0o644)
        with self.assertRaisesRegex(ValueError, 'every native runtime binary'):
            package_runtime.package(self.bin, self.root / 'missing')
        executable.chmod(0o755)
        target = self.root / 'existing'
        target.mkdir()
        (target / 'keep').write_text('unchanged')
        with self.assertRaisesRegex(ValueError, 'already exists'):
            package_runtime.package(self.bin, target)
        self.assertEqual((target / 'keep').read_text(), 'unchanged')
