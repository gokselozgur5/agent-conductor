"""Operator plan generation retains the selected OS account execution profile."""
import hashlib
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest


class OperatorProfileTests(unittest.TestCase):
    def test_template_profile_survives_plan_generation(self):
        launcher = Path(__file__).resolve().parents[1] / 'tools/conductor'
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / 'plans').mkdir()
            config = root / 'operator.json'
            config.write_text(json.dumps({'project': str(root), 'bin_dir': str(root)}))
            config.chmod(0o600)
            binary = Path(sys.executable).resolve()
            pin = dict(kind='codex', version='fixture', executable=str(binary),
                       executable_sha256=hashlib.sha256(binary.read_bytes()).hexdigest(),
                       conformance_sha256='a' * 64)
            profile = dict(account=dict(name='worker', uid=501, gid=20,
                                        home='/Users/worker'), path='/usr/bin:/bin')
            template = root / 'template.json'
            objective = root / 'objective.txt'
            objective.write_text('Create the requested file.')
            checks = root / 'checks.json'
            checks.write_text('[]')
            target = dict(socket='/receiver-private/control.sock',
                          artifacts='/receiver-private/artifacts')
            for task, selected in [('full', profile), ('legacy', None), ('account', profile)]:
                value = dict(worker=pin, verifier=pin, model_timeout_ms=1000)
                if task == 'account':
                    value['worker'] = dict(pin, executable='/receiver-private/vendor')
                    value['account_worker'] = target
                if selected is not None:
                    value['full_capability'] = selected
                template.write_text(json.dumps(value))
                subprocess.run([sys.executable, '-E', '-s', str(launcher), '--state',
                                str(root), 'plan', '--task', task, '--from-plan', str(template),
                                '--objective-file', str(objective), '--checks-file', str(checks)],
                               check=True, capture_output=True, timeout=10)
                generated = json.loads((root / 'plans' / f'{task}.json').read_text())
                if task == 'account':
                    self.assertEqual(generated['account_worker'], target)
                    self.assertEqual(generated['worker']['executable'], '/receiver-private/vendor')
                    self.assertEqual(generated['verifier'], pin)
                if selected is not None:
                    self.assertEqual(generated['full_capability'], selected)
                else:
                    self.assertNotIn('full_capability', generated)
