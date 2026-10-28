"""Sensitivity controls and real daemon acceptance for the endurance harness."""
import importlib.util
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
import tempfile
import time
import unittest

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location('soak_control_plane', ROOT / 'tools/soak_control_plane.py')
soak = importlib.util.module_from_spec(spec)
spec.loader.exec_module(soak)


class ObservationControls(unittest.TestCase):
    def sample(self):
        return dict(cycle=1, wall_s=101, monotonic_s=51, healthy=True,
                    projection_matches=True, live_leases=0, executions=0,
                    pending_deliveries=0, dispatch_count=0, rss_kib=2048,
                    stale_refused=True, task_count=2, event_count=2, boot=3,
                    daemon_children=0, state_bytes=1000, log_bytes=100)

    def test_each_material_violation_fails(self):
        previous = {**self.sample(), 'wall_s': 100, 'monotonic_s': 50}
        limits = dict(rss_kib=4096, max_gap_s=10, baseline_rss_kib=1024, growth_kib=2048)
        soak.validate_sample(self.sample(), previous, limits)
        changes = dict(healthy=False, projection_matches=False, live_leases=1,
                       executions=1, pending_deliveries=1, dispatch_count=1,
                       rss_kib=5000, stale_refused=False, task_count=3,
                       event_count=3, boot=4, daemon_children=1,
                       state_bytes=3 * 1024**3, log_bytes=17 * 1024**2,
                       monotonic_s=100, wall_s=150)
        for field, value in changes.items():
            with self.subTest(field=field), self.assertRaises(soak.Failure):
                soak.validate_sample({**self.sample(), field: value}, previous, limits)
        with self.assertRaisesRegex(soak.Failure, 'resident_memory_growth'):
            soak.validate_sample({**self.sample(), 'rss_kib': 3500}, previous, limits)

    def test_duration_cannot_turn_short_probe_into_soak(self):
        self.assertEqual(soak.completion_status(5, 5), 'completed_short_probe')
        self.assertEqual(soak.completion_status(soak.SEVEN_DAYS, 5), 'completed_short_probe')
        with self.assertRaisesRegex(soak.Failure, 'incomplete_duration'):
            soak.completion_status(5, soak.SEVEN_DAYS)
        self.assertEqual(soak.completion_status(soak.SEVEN_DAYS, soak.SEVEN_DAYS), 'completed_control_plane_soak')

    def test_status_replacement_is_valid_json_and_private(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / 'status.json'
            soak.atomic_json(path, {'status': 'running'})
            soak.atomic_json(path, {'status': 'failed'})
            self.assertEqual(json.loads(path.read_text()), {'status': 'failed'})
            self.assertFalse(path.with_suffix('.next').exists())
            self.assertEqual(path.stat().st_mode & 0o777, 0o600)


@unittest.skipUnless(os.environ.get('CONDUCTOR_TEST_BIN_DIR'), 'requires real daemon/CLI binaries')
class RealDaemonExercise(unittest.TestCase):
    def test_real_projection_corruption_fails_the_run(self):
        with tempfile.TemporaryDirectory() as tmp:
            output = Path(tmp).resolve() / 'corrupt'
            command = [sys.executable, str(ROOT / 'tools/soak_control_plane.py'),
                       '--bin-dir', os.environ['CONDUCTOR_TEST_BIN_DIR'],
                       '--output', str(output), '--socket-dir', str(Path(tempfile.gettempdir()).resolve()),
                       '--duration-seconds', '10', '--interval-seconds', '1']
            process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            try:
                deadline = time.monotonic() + 8
                while time.monotonic() < deadline:
                    if (output / 'status.json').exists():
                        value = json.loads((output / 'status.json').read_text())
                        if value['status'] == 'running':
                            break
                    self.assertIsNone(process.poll())
                    time.sleep(.02)
                else:
                    self.fail('monitor did not produce its first observation')
                with sqlite3.connect(output / 'state.sqlite3') as db:
                    db.execute("update tasks set state='ready' where id='soak-00000000'")
                _, error = process.communicate(timeout=10)
                self.assertNotEqual(process.returncode, 0)
                value = json.loads((output / 'status.json').read_text())
                self.assertEqual(value['status'], 'failed')
                self.assertEqual(value['error'], 'runtime_invariant_failure', error.decode())
            finally:
                if process.poll() is None:
                    process.terminate()
                    process.communicate(timeout=20)

    def test_short_run_owns_isolated_state_and_refuses_reuse(self):
        with tempfile.TemporaryDirectory() as tmp:
            parent = Path(tmp).resolve()
            output = parent / 'state'
            command = [sys.executable, str(ROOT / 'tools/soak_control_plane.py'),
                       '--bin-dir', os.environ['CONDUCTOR_TEST_BIN_DIR'],
                       '--output', str(output), '--socket-dir', str(Path(tempfile.gettempdir()).resolve()),
                       '--duration-seconds', '2', '--interval-seconds', '1']
            result = subprocess.run(command, capture_output=True, timeout=30)
            self.assertEqual(result.returncode, 0, result.stderr.decode())
            receipt = json.loads((output / 'status.json').read_text())
            self.assertEqual(receipt['status'], 'completed_short_probe')
            self.assertFalse(receipt['full_v1_accepted'])
            self.assertGreaterEqual(receipt['cycles'], 1)
            with sqlite3.connect(output / 'state.sqlite3') as db:
                self.assertEqual(db.execute("select count(*) from tasks where state!='cancelled'").fetchone()[0], 0)
                self.assertEqual(db.execute('select count(*) from executions').fetchone()[0], 0)
                before = db.execute('select count(*) from tasks').fetchone()[0]
            again = subprocess.run(command, capture_output=True, timeout=10)
            self.assertNotEqual(again.returncode, 0)
            with sqlite3.connect(output / 'state.sqlite3') as db:
                self.assertEqual(db.execute('select count(*) from tasks').fetchone()[0], before)


if __name__ == '__main__':
    unittest.main()
