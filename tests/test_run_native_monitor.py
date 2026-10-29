import json
from pathlib import Path
import sys
import tempfile
import time
import unittest

sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'tools'))
from native_observation_guard import Violation
from run_native_monitor import run_native_monitor


class Guard:
    max_gap = 2
    board_reader = True

    def __init__(self, mode='healthy'):
        self.mode = mode
        self.last = time.monotonic()
        self.last_wall = time.time()

    def sample(self, invariant):
        if self.mode == 'fail':
            raise Violation('planted_violation')
        self.last = time.monotonic()
        self.last_wall = time.time()
        return dict(healthy=self.mode!='uncertain', board_projection='matching',
                    tagged_observations=0 if self.mode=='empty' else 1,
                    recorded_observations=1)


class MonitorRunTests(unittest.TestCase):
    def test_short_run_has_durable_samples_and_cannot_claim_full_acceptance(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory); pinned=root/'pin';pinned.write_text('fixed')
            result=run_native_monitor(Guard(),lambda:{},root/'run',duration_seconds=.2,interval_seconds=.1,pinned_files=[pinned])
            self.assertEqual(result['status'],'completed_short_native_probe')
            self.assertFalse(result['full_v1_accepted'])
            self.assertGreaterEqual(result['samples'],2)
            self.assertEqual(json.loads((root/'run/status.json').read_text()),result)
            self.assertEqual(len((root/'run/samples.jsonl').read_text().splitlines()),result['samples'])
            self.assertEqual((root/'run/status.json').stat().st_mode&0o777,0o600)
            with self.assertRaises(FileExistsError):
                run_native_monitor(Guard(),lambda:{},root/'run',duration_seconds=.1,interval_seconds=.1,pinned_files=[pinned])

    def test_violation_uncertainty_and_missing_native_work_fail_persistently(self):
        for mode in ['fail','uncertain','empty']:
            with self.subTest(mode=mode),tempfile.TemporaryDirectory() as directory:
                root=Path(directory);pinned=root/'pin';pinned.write_text('fixed')
                with self.assertRaises(Violation):
                    run_native_monitor(Guard(mode),lambda:{},root/'run',duration_seconds=.1,interval_seconds=.1,pinned_files=[pinned])
                receipt=json.loads((root/'run/status.json').read_text())
                self.assertEqual(receipt['status'],'failed')
                self.assertFalse(receipt['full_v1_accepted'])

    def test_changed_pin_and_reader_error_are_not_success_or_private_output(self):
        for mode in ['change','error']:
            with tempfile.TemporaryDirectory() as directory:
                root=Path(directory);pinned=root/'pin';pinned.write_text('fixed')
                def read():
                    if mode=='error':raise RuntimeError('PRIVATE_PAYLOAD')
                    pinned.write_text('changed')
                    return {}
                with self.assertRaises(Violation):
                    run_native_monitor(Guard(),read,root/'run',duration_seconds=.1,interval_seconds=.1,pinned_files=[pinned])
                text=(root/'run/status.json').read_text()
                self.assertNotIn('PRIVATE_PAYLOAD',text)
                self.assertEqual(json.loads(text)['status'],'failed')

    def test_invalid_budgets_refuse_before_output_creation(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory)
            for duration in [0, float('nan'),float('inf'),True,604801]:
                with self.assertRaises(Violation):
                    run_native_monitor(Guard(),lambda:{},root/'run',duration_seconds=duration,interval_seconds=.1,pinned_files=[__file__])
                self.assertFalse((root/'run').exists())

    def test_seven_day_run_requires_resources_before_output_creation(self):
        with tempfile.TemporaryDirectory() as directory:
            guard=Guard();guard.max_gap=20
            with self.assertRaisesRegex(Violation,'soak_resources_required'):
                run_native_monitor(guard,lambda:{},Path(directory)/'run',duration_seconds=604800,interval_seconds=10,pinned_files=[__file__])
            self.assertFalse((Path(directory)/'run').exists())


if __name__=='__main__':
    unittest.main()
