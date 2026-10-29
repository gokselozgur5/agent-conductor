from __future__ import annotations

import tempfile
import subprocess
import unittest
from pathlib import Path
from unittest.mock import patch

from conductor import ConductorError, ConductorStore


class Clock:
    def __init__(self) -> None:
        self.value = 1_000.0

    def __call__(self) -> float:
        return self.value

    def advance(self, seconds: float) -> None:
        self.value += seconds


class StoreTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.clock = Clock()
        self.store = ConductorStore(Path(self.temp.name) / "test.db", self.clock)
        self.store.initialize()
        self.store.register_agent("worker", "codex")
        self.store.register_agent("reviewer", "claude")

    def tearDown(self) -> None:
        self.store.close()
        self.temp.cleanup()

    def test_task_requires_independent_complete_verification(self) -> None:
        task = self.store.create_task("ship feature", ["tests", "docs"])
        self.store.claim_task(task["id"], "worker", 30)
        self.store.start_task(task["id"], "worker")
        self.store.submit_for_verification(task["id"], "worker", {"commit": "abc"})

        with self.assertRaisesRegex(ConductorError, "cannot verify its own"):
            self.store.verify_task(
                task["id"], "worker", {"tests": True, "docs": True}
            )

        completed = self.store.verify_task(
            task["id"], "reviewer", {"tests": True, "docs": True}
        )
        self.assertEqual(completed["state"], "completed")
        self.assertIsNotNone(completed["completed_at"])

    def test_rejected_verification_requeues_work(self) -> None:
        task = self.store.create_task("ship feature", ["tests", "docs"])
        self.store.claim_task(task["id"], "worker", 30)
        self.store.start_task(task["id"], "worker")
        self.store.submit_for_verification(task["id"], "worker", {"commit": "abc"})

        with self.assertRaisesRegex(ConductorError, "missing checks: docs"):
            self.store.verify_task(
                task["id"], "reviewer", {"tests": True, "docs": False}
            )
        rejected = self.store.get_task(task["id"])
        self.assertEqual(rejected["state"], "queued")
        self.assertIsNone(rejected["owner_agent_id"])
        self.assertEqual(rejected["evidence_json"], {"commit": "abc"})

    def test_expired_lease_preserves_checkpoint(self) -> None:
        task = self.store.create_task("long task")
        self.store.claim_task(task["id"], "worker", 10)
        self.store.start_task(task["id"], "worker")
        self.store.checkpoint_task(task["id"], "worker", {"next": "run tests"})
        self.clock.advance(11)

        self.assertEqual(self.store.reconcile_expired(), [task["id"]])
        recovered = self.store.get_task(task["id"])
        self.assertEqual(recovered["state"], "queued")
        self.assertEqual(recovered["checkpoint_json"], {"next": "run tests"})
        self.assertEqual(recovered["attempt"], 1)

    def test_heartbeat_extends_owned_lease(self) -> None:
        task = self.store.create_task("long task")
        self.store.claim_task(task["id"], "worker", 10)
        self.clock.advance(8)
        self.store.heartbeat("worker", 10)
        self.clock.advance(3)
        self.assertEqual(self.store.reconcile_expired(), [])

    def test_second_agent_cannot_claim_an_owned_task(self) -> None:
        task = self.store.create_task("exclusive task")
        self.store.claim_task(task["id"], "worker", 10)

        with self.assertRaisesRegex(ConductorError, "not available"):
            self.store.claim_task(task["id"], "reviewer", 10)

        owned = self.store.get_task(task["id"])
        self.assertEqual(owned["owner_agent_id"], "worker")
        self.assertEqual(owned["attempt"], 1)

    def test_project_board_tracks_real_task_state(self) -> None:
        project_root = Path(self.temp.name) / "project"
        project_root.mkdir()
        project = self.store.register_project(project_root, "Example")
        queued = self.store.create_task("queued", project_id=project["id"])
        running = self.store.create_task("running", project_id=project["id"])
        self.store.claim_task(running["id"], "worker", 30)
        self.store.start_task(running["id"], "worker")

        board = self.store.board(project["id"])

        self.assertEqual([task["id"] for task in board["queued"]], [queued["id"]])
        self.assertEqual([task["id"] for task in board["running"]], [running["id"]])
        self.assertEqual(board["completed"], [])

    def test_project_context_discovers_rules_docs_git_and_board(self) -> None:
        project_root = Path(self.temp.name) / "project"
        docs = project_root / "docs"
        docs.mkdir(parents=True)
        (project_root / "AGENTS.md").write_text("# Rules\n", encoding="utf-8")
        (Path(self.temp.name) / "AGENTS.md").symlink_to(project_root / "AGENTS.md")
        (project_root / "README.md").write_text("# Project\n", encoding="utf-8")
        (docs / "STATUS.md").write_text("# Status\n", encoding="utf-8")
        project = self.store.register_project(project_root)
        self.store.create_task("resume me", project_id=project["id"])

        # Temporary directories may live inside a developer's outer repository.
        # This fixture deliberately starts without Git ownership of its own.
        ceiling = {"GIT_CEILING_DIRECTORIES": str(project_root.parent.resolve())}
        with patch.dict("os.environ", ceiling):
            context = self.store.project_context(project["id"])
        canonical_root = project_root.resolve()

        self.assertIn(
            str(canonical_root / "AGENTS.md"), context["instruction_files"]
        )
        self.assertEqual(
            context["instruction_files"].count(str(canonical_root / "AGENTS.md")), 1
        )
        self.assertEqual(
            context["context_files"],
            [str(canonical_root / "README.md"), str(canonical_root / "docs/STATUS.md")],
        )
        self.assertFalse(context["git"]["available"])
        self.assertEqual(context["board"]["queued"][0]["title"], "resume me")
        subprocess.run(["git", "init", "--quiet", str(project_root)], check=True)
        with patch.dict("os.environ", ceiling):
            self.assertTrue(self.store.project_context(project["id"])["git"]["available"])

    def test_event_delivery_is_idempotent_and_does_not_echo_source(self) -> None:
        self.store.subscribe("worker", "file.*", "src/*")
        self.store.subscribe("reviewer", "file.*", "src/*")
        first = self.store.publish_event(
            idempotency_key="sha:1:path:src/api.py",
            source_agent_id="worker",
            topic="file.updated",
            path="src/api.py",
            payload={"sha": "1"},
        )
        second = self.store.publish_event(
            idempotency_key="sha:1:path:src/api.py",
            source_agent_id="worker",
            topic="file.updated",
            path="src/api.py",
            payload={"sha": "1"},
        )

        self.assertEqual(first["id"], second["id"])
        self.assertEqual(self.store.pending_deliveries("worker"), [])
        pending = self.store.pending_deliveries("reviewer")
        self.assertEqual([event["id"] for event in pending], [first["id"]])
        self.store.acknowledge(first["id"], "reviewer")
        self.assertEqual(self.store.pending_deliveries("reviewer"), [])

    def test_path_and_generation_bound_wakeups(self) -> None:
        self.store.subscribe("reviewer", "file.updated", "docs/*")
        self.store.publish_event(
            idempotency_key="source-change",
            topic="file.updated",
            path="src/main.py",
            payload={},
        )
        self.assertEqual(self.store.pending_deliveries("reviewer"), [])

        with self.assertRaisesRegex(ConductorError, "generation limit"):
            self.store.publish_event(
                idempotency_key="too-deep",
                topic="file.updated",
                path="docs/readme.md",
                payload={},
                generation=self.store.MAX_EVENT_GENERATION + 1,
            )


if __name__ == "__main__":
    unittest.main()
