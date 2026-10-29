"""Repeated stale identity rejection against an isolated real daemon."""
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
from types import SimpleNamespace
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'tools'))
from native_observation_guard import Violation
from native_stale_control import NativeStaleControl, validate_refusal
from soak_control_plane import Exercise


class RefusalTests(unittest.TestCase):
    def test_wrong_refusal_and_mutation_fail(self):
        before = {'version': 3}
        valid = dict(code='conflict', message='attempt, worker, or epoch is stale')
        validate_refusal(before, dict(before), valid)
        for response in [dict(code='conflict', message='version mismatch'), dict(code='invalid', message=valid['message'])]:
            with self.assertRaisesRegex(Violation, 'wrong_refusal'):
                validate_refusal(before, before, response)
        with self.assertRaisesRegex(Violation, 'mutated_task'):
            validate_refusal(before, {'version': 4}, valid)


@unittest.skipUnless(os.environ.get('CONDUCTOR_TEST_BIN_DIR'), 'requires real daemon/CLI binaries')
class RealStaleControlTests(unittest.TestCase):
    def test_repeated_old_grant_refuses_current_renews_and_owner_change_latches(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            exercise = Exercise(SimpleNamespace(output=root/'daemon', bin_dir=Path(os.environ['CONDUCTOR_TEST_BIN_DIR']), socket_dir=Path(tempfile.gettempdir()), duration_seconds=10, interval_seconds=1, max_rss_mib=512))
            def request(*args, ok=True):
                result = subprocess.run([str(exercise.bins['conductorctl']), '--socket', str(exercise.sock), '--json', *args], capture_output=True, timeout=10)
                self.assertEqual(result.returncode == 0, ok)
                return json.loads(result.stdout)['result']['payload']
            try:
                exercise.start()
                control = NativeStaleControl(request, root/'control')
                control.setup()
                self.assertGreater(control.current['lease']['epoch'], control.old['lease']['epoch'])
                for number in range(1, 4):
                    sample = control.sample()
                    self.assertEqual(sample['sample'], number)
                    self.assertTrue(all(sample[key] for key in ['stale_refused', 'aggregate_unchanged_on_refusal', 'valid_renewal_accepted']))
                control.finish()
                self.assertEqual(control.task()['state'], 'cancelled')
                with self.assertRaisesRegex(Violation, 'owner_changed'):
                    control.sample()
                with self.assertRaisesRegex(Violation, 'unavailable'):
                    control.sample()
                self.assertTrue(request('check-invariants')['healthy'])
            finally:
                if exercise.process is not None and exercise.process.poll() is None:
                    exercise.process.send_signal(signal.SIGINT)
                    exercise.process.wait(timeout=15)
                if exercise.log:
                    exercise.log.close()


if __name__ == '__main__':
    unittest.main()
