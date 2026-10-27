from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from conductor import ConductorStore
from conductor.hooks import DirectoryWatcher


class DirectoryWatcherTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name) / "project"
        self.root.mkdir()
        self.store = ConductorStore(Path(self.temp.name) / "state.db")
        self.store.initialize()
        self.store.register_agent("source", "codex")
        self.store.register_agent("tests", "claude")
        self.store.register_agent("docs", "codex")
        self.store.subscribe("tests", "file.*", "src/*")
        self.store.subscribe("docs", "file.*", "docs/*")
        self.watcher = DirectoryWatcher(
            self.store, self.root, source_agent_id="source"
        )

    def tearDown(self) -> None:
        self.store.close()
        self.temp.cleanup()

    def test_routes_only_changed_matching_path(self) -> None:
        (self.root / "src").mkdir()
        source = self.root / "src" / "api.py"
        source.write_text("before\n", encoding="utf-8")
        previous = self.watcher.scan()
        source.write_text("after\n", encoding="utf-8")

        events = self.watcher.publish_changes(previous, self.watcher.scan())

        self.assertEqual(len(events), 1)
        self.assertEqual(self.store.pending_deliveries("docs"), [])
        pending = self.store.pending_deliveries("tests")
        self.assertEqual(len(pending), 1)
        self.assertEqual(pending[0]["topic"], "file.updated")
        self.assertEqual(pending[0]["path"], "src/api.py")

    def test_repeated_scan_does_not_duplicate_event(self) -> None:
        (self.root / "docs").mkdir()
        document = self.root / "docs" / "status.md"
        document.write_text("one\n", encoding="utf-8")
        previous = self.watcher.scan()
        document.write_text("two\n", encoding="utf-8")
        current = self.watcher.scan()

        first = self.watcher.publish_changes(previous, current)
        second = self.watcher.publish_changes(previous, current)

        self.assertEqual(first, second)
        self.assertEqual(len(self.store.pending_deliveries("docs")), 1)

    def test_deleted_file_generates_tombstone_event(self) -> None:
        document = self.root / "gone.txt"
        document.write_text("temporary\n", encoding="utf-8")
        previous = self.watcher.scan()
        document.unlink()

        event_id = self.watcher.publish_changes(previous, self.watcher.scan())[0]
        event = self.store.get_event(event_id)

        self.assertEqual(event["topic"], "file.deleted")
        self.assertIsNone(event["payload_json"]["after"])

    def test_ignores_runtime_and_git_directories(self) -> None:
        for directory in ("state", ".git", "__pycache__"):
            target = self.root / directory
            target.mkdir()
            (target / "ignored").write_text("ignored\n", encoding="utf-8")
        (self.root / "kept.txt").write_text("kept\n", encoding="utf-8")

        self.assertEqual(list(self.watcher.scan()), ["kept.txt"])

    def test_snapshot_round_trip_is_atomic_and_stable(self) -> None:
        (self.root / "kept.txt").write_text("kept\n", encoding="utf-8")
        snapshot = self.watcher.scan()
        snapshot_path = Path(self.temp.name) / "snapshots" / "project.json"

        self.watcher.save_snapshot(snapshot_path, snapshot)

        self.assertEqual(self.watcher.load_snapshot(snapshot_path), snapshot)
        self.assertFalse(snapshot_path.with_suffix(".json.tmp").exists())
