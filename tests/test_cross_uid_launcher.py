"""The privileged CI fixture must never run on an ordinary developer invocation."""
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'tools'))
import run_cross_uid_test


class CrossUidLauncherTests(unittest.TestCase):
    def test_non_ci_invocation_never_builds_or_requests_root(self):
        with patch.dict(os.environ, {}, clear=True), patch.object(subprocess, 'run') as run:
            with self.assertRaisesRegex(SystemExit, 'restricted to Linux'):
                run_cross_uid_test.main()
            run.assert_not_called()

    def test_privileged_child_gets_only_explicit_environment(self):
        with tempfile.TemporaryDirectory() as directory:
            binary = Path(directory) / 'test-binary'
            binary.write_text('fixture')
            output = json.dumps(dict(reason='compiler-artifact',
                target=dict(name='account_cross_uid'), executable=str(binary)))
            environment = dict(GITHUB_ACTIONS='true', RUNNER_TEMP=directory,
                               HOME='/private-home', PRIVATE_FIXTURE='must-not-forward')
            with patch.dict(os.environ, environment, clear=True), \
                    patch.object(sys, 'platform', 'linux'), \
                    patch.object(subprocess, 'run') as run:
                run.return_value.stdout = output
                run_cross_uid_test.main()
            command = run.call_args_list[1].args[0]
            self.assertEqual(command[:6], ['sudo', '-n', '/usr/bin/env', '-i',
                                          'PATH=/usr/bin:/bin', f'TMPDIR={Path(directory).resolve()}'])
            self.assertNotIn('-E', command)
            self.assertNotIn('/private-home', command)
            self.assertNotIn('must-not-forward', command)
            self.assertEqual(command[7:9], ['--ignored', '--exact'])

    def test_ambiguous_build_cannot_request_root(self):
        with patch.dict(os.environ, {'GITHUB_ACTIONS': 'true'}, clear=True), \
                patch.object(sys, 'platform', 'linux'), \
                patch.object(subprocess, 'run') as run:
            run.return_value.stdout = ''
            with self.assertRaisesRegex(SystemExit, 'one compiled'):
                run_cross_uid_test.main()
            self.assertEqual(run.call_count, 1)
