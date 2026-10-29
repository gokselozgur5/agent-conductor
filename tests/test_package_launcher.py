"""Host-local launchers must not depend on a working ambient Python command."""
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

from package_local import launcher


class PackageLauncherTests(unittest.TestCase):
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
                (['run', '--dispatch'], ['run', '--dispatch']),
            ]
            for arguments, expected in cases:
                with self.subTest(arguments=arguments):
                    result = subprocess.run([str(command), *arguments], env=environment,
                                            cwd='/', text=True, capture_output=True,
                                            timeout=10, check=True)
                    self.assertEqual(json.loads(result.stdout), expected)
                    self.assertEqual(result.stderr, '')
