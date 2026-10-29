"""Host-local launchers must not depend on a working ambient Python command."""
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'tools'))
from package_local import launcher


class PackageLauncherTests(unittest.TestCase):
    def test_snapshot_transfer_uses_bundled_cli_without_operator_state(self):
        with tempfile.TemporaryDirectory(prefix='transfer bundle ') as directory:
            root = Path(directory)
            (root / 'bin').mkdir()
            binary = root / 'bin/conductorctl'
            binary.write_text('#!/bin/sh\nprintf "%s\\n" "$@"\n')
            binary.chmod(0o700)
            command = root / 'conductor'
            command.write_text(launcher(Path(sys.executable).resolve()))
            command.chmod(0o700)
            arguments = ['transfer-snapshot', 'import', '--destination', '/account home/new',
                         '--expected-sha256', 'a' * 64]
            result = subprocess.run([str(command), *arguments], check=True,
                                    capture_output=True, text=True, timeout=10)
            self.assertEqual(result.stdout.splitlines(), arguments)
            self.assertFalse((root / 'state').exists())

    def test_pinned_python_survives_broken_path_and_preserves_arguments(self):
        with tempfile.TemporaryDirectory(prefix="bundle 'quoted ") as directory:
            root = Path(directory)
            (root / 'tools').mkdir()
            (root / 'tools/conductor').write_text(
                'import json, sys\nprint(json.dumps(sys.argv[1:]))\n')
            interpreter = root / "python 'pinned"
            interpreter.symlink_to(Path(sys.executable).resolve())
            command = root / 'conductor'
            command.write_text(launcher(interpreter))
            command.chmod(0o700)
            hostile = root / 'python3'
            hostile.write_text('#!/bin/sh\nexit 89\n')
            hostile.chmod(0o700)
            environment = dict(os.environ, PATH=str(root), PYTHONHOME='/missing',
                               PYTHONPATH='/missing')
            cases = [
                (['init', '--project', '/a project'],
                 ['init', '--project', '/a project', '--bin-dir', str(root / 'bin')]),
                (['--state', '/state with spaces', 'plan', '--task', 'one'],
                 ['--state', '/state with spaces', 'plan', '--task', 'one',
                  '--from-plan', str(root / 'accepted-adapters.json')]),
                (['plan', '--from-plan', '/explicit'], ['plan', '--from-plan', '/explicit']),
                (['account-check', '--role', 'worker'],
                 ['account-check', '--role', 'worker', '--from-plan', str(root / 'accepted-adapters.json')]),
                (['account-check', '--role', 'coordinator', '--from-plan', '/explicit'],
                 ['account-check', '--role', 'coordinator', '--from-plan', '/explicit']),
                (['run', '--dispatch'], ['run', '--dispatch']),
            ]
            for arguments, expected in cases:
                with self.subTest(arguments=arguments):
                    result = subprocess.run([str(command), *arguments], env=environment,
                                            cwd='/', text=True, capture_output=True,
                                            timeout=10, check=True)
                    self.assertEqual(json.loads(result.stdout), expected)
                    self.assertEqual(result.stderr, '')

    def test_package_retains_full_profile_without_copying_credentials(self):
        import hashlib
        from package_local import BINARIES, package
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            binaries = root / 'bin'
            binaries.mkdir()
            for name in BINARIES:
                executable = binaries / name
                executable.write_text('#!/bin/sh\nexit 0\n')
                executable.chmod(0o700)
            executable = Path(sys.executable).resolve()
            pin = dict(kind='codex', version='fixture', executable=str(executable),
                       executable_sha256=hashlib.sha256(executable.read_bytes()).hexdigest(),
                       conformance_sha256='a' * 64)
            profile = dict(account=dict(name='worker', uid=501, gid=20,
                                        home='/Users/worker'), path='/usr/bin:/bin')
            template = root / 'plan.json'
            template.write_text(json.dumps(dict(worker=pin, verifier=pin,
                model_timeout_ms=1000, full_capability=profile)))
            destination = root / 'bundle'
            manifest = package(binaries, template, destination)
            saved = json.loads((destination / 'accepted-adapters.json').read_text())
            self.assertEqual(saved['full_capability'], profile)
            self.assertIn('docs/FULL_CAPABILITY_EXECUTION.md', manifest['files'])
            self.assertIn('crates/store/migrations/0030_capture_eligibility.sql', manifest['files'])
            source = Path(__file__).resolve().parents[1] / 'crates/store/migrations'
            for migration in source.glob('*.sql'):
                self.assertEqual((destination / 'crates/store/migrations' / migration.name).read_bytes(), migration.read_bytes())
            self.assertNotIn('ANTHROPIC_API_KEY', json.dumps(saved))
            target = dict(socket='/receiver-private/control.sock',
                          artifacts='/receiver-private/artifacts')
            remote = dict(pin, executable='/receiver-private/vendor')
            template.write_text(json.dumps(dict(worker=remote, verifier=pin,
                model_timeout_ms=1000, full_capability=profile, account_worker=target)))
            package(binaries, template, root / 'remote-bundle')
            saved = json.loads((root / 'remote-bundle/accepted-adapters.json').read_text())
            self.assertEqual(saved['worker'], remote)
            self.assertEqual(saved['verifier'], pin)
            self.assertEqual(saved['account_worker'], target)
