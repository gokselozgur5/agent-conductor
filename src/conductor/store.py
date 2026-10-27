from __future__ import annotations

import fnmatch
import json
import sqlite3
import subprocess
import time
import uuid
from pathlib import Path
from typing import Any, Callable, Iterable


class ConductorError(RuntimeError):
    pass


def _json(value: Any) -> str:
    return json.dumps(value, separators=(",", ":"), sort_keys=True)


class ConductorStore:
    MAX_EVENT_GENERATION = 8

    def __init__(self, database: str | Path, clock: Callable[[], float] = time.time):
        self.database = str(database)
        self.clock = clock
        Path(self.database).parent.mkdir(parents=True, exist_ok=True)
        self.connection = sqlite3.connect(self.database)
        self.connection.row_factory = sqlite3.Row
        self.connection.execute("PRAGMA foreign_keys = ON")

    def close(self) -> None:
        self.connection.close()

    def initialize(self) -> None:
        schema = Path(__file__).with_name("schema.sql").read_text(encoding="utf-8")
        self.connection.executescript(schema)

    def register_agent(
        self, agent_id: str, adapter: str, capabilities: Iterable[str] = ()
    ) -> dict[str, Any]:
        now = self.clock()
        with self.connection:
            self.connection.execute(
                """
                INSERT INTO agents(id, adapter, capabilities_json, last_heartbeat, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(id) DO UPDATE SET
                    adapter = excluded.adapter,
                    capabilities_json = excluded.capabilities_json,
                    status = 'idle',
                    last_heartbeat = excluded.last_heartbeat,
                    updated_at = excluded.updated_at
                """,
                (agent_id, adapter, _json(list(capabilities)), now, now, now),
            )
        return self.get_agent(agent_id)

    def get_agent(self, agent_id: str) -> dict[str, Any]:
        row = self.connection.execute(
            "SELECT * FROM agents WHERE id = ?", (agent_id,)
        ).fetchone()
        if row is None:
            raise ConductorError(f"unknown agent: {agent_id}")
        return self._decode(row)

    def register_project(
        self, path: str | Path, name: str | None = None
    ) -> dict[str, Any]:
        resolved = Path(path).expanduser().resolve(strict=True)
        if not resolved.is_dir():
            raise ConductorError(f"project path is not a directory: {resolved}")
        project_id = str(uuid.uuid5(uuid.NAMESPACE_URL, f"file://{resolved}"))
        project_name = name or resolved.name
        now = self.clock()
        with self.connection:
            self.connection.execute(
                """
                INSERT INTO projects(id, name, path, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(path) DO UPDATE SET
                    name = excluded.name,
                    updated_at = excluded.updated_at
                """,
                (project_id, project_name, str(resolved), now, now),
            )
        return self.get_project(project_id)

    def get_project(self, project_id: str) -> dict[str, Any]:
        row = self.connection.execute(
            "SELECT * FROM projects WHERE id = ?", (project_id,)
        ).fetchone()
        if row is None:
            raise ConductorError(f"unknown project: {project_id}")
        return self._decode(row)

    def list_projects(self) -> list[dict[str, Any]]:
        rows = self.connection.execute(
            "SELECT * FROM projects ORDER BY name, id"
        ).fetchall()
        return [self._decode(row) for row in rows]

    def project_context(self, project_id: str) -> dict[str, Any]:
        project = self.get_project(project_id)
        root = Path(project["path"])
        instruction_files: list[str] = []
        seen_instructions: set[Path] = set()
        for directory in reversed((root, *root.parents)):
            candidate = directory / "AGENTS.md"
            if candidate.is_file():
                canonical = candidate.resolve()
                if canonical not in seen_instructions:
                    instruction_files.append(str(canonical))
                    seen_instructions.add(canonical)

        preferred = (
            "README.md",
            "docs/STATUS.md",
            "docs/PHASES.md",
            "docs/TRACKING.md",
            "docs/ARCHITECTURE.md",
        )
        context_files = [str(root / item) for item in preferred if (root / item).is_file()]
        git = self._git_context(root)
        return {
            "project": project,
            "instruction_files": instruction_files,
            "context_files": context_files,
            "git": git,
            "board": self.board(project_id),
        }

    def create_task(
        self,
        title: str,
        required_checks: Iterable[str] = (),
        project_id: str | None = None,
    ) -> dict[str, Any]:
        if project_id is not None:
            self.get_project(project_id)
        task_id = str(uuid.uuid4())
        now = self.clock()
        checks = list(dict.fromkeys(required_checks))
        with self.connection:
            self.connection.execute(
                """
                INSERT INTO tasks(id, project_id, title, state, required_checks_json, created_at, updated_at)
                VALUES (?, ?, ?, 'queued', ?, ?, ?)
                """,
                (task_id, project_id, title, _json(checks), now, now),
            )
        return self.get_task(task_id)

    def get_task(self, task_id: str) -> dict[str, Any]:
        row = self.connection.execute(
            "SELECT * FROM tasks WHERE id = ?", (task_id,)
        ).fetchone()
        if row is None:
            raise ConductorError(f"unknown task: {task_id}")
        return self._decode(row)

    def list_tasks(self) -> list[dict[str, Any]]:
        rows = self.connection.execute(
            "SELECT * FROM tasks ORDER BY created_at, id"
        ).fetchall()
        return [self._decode(row) for row in rows]

    def board(self, project_id: str | None = None) -> dict[str, list[dict[str, Any]]]:
        states = ("queued", "claimed", "running", "verifying", "blocked", "completed", "cancelled")
        board: dict[str, list[dict[str, Any]]] = {state: [] for state in states}
        if project_id is None:
            rows = self.connection.execute(
                "SELECT * FROM tasks ORDER BY updated_at DESC, id"
            ).fetchall()
        else:
            self.get_project(project_id)
            rows = self.connection.execute(
                "SELECT * FROM tasks WHERE project_id = ? ORDER BY updated_at DESC, id",
                (project_id,),
            ).fetchall()
        for row in rows:
            task = self._decode(row)
            board[task["state"]].append(task)
        return board

    def claim_task(self, task_id: str, agent_id: str, lease_seconds: float) -> dict[str, Any]:
        if lease_seconds <= 0:
            raise ConductorError("lease must be positive")
        self.get_agent(agent_id)
        now = self.clock()
        with self.connection:
            cursor = self.connection.execute(
                """
                UPDATE tasks
                SET state = 'claimed', owner_agent_id = ?, lease_expires_at = ?,
                    attempt = attempt + 1, version = version + 1, updated_at = ?
                WHERE id = ? AND state = 'queued'
                """,
                (agent_id, now + lease_seconds, now, task_id),
            )
            if cursor.rowcount != 1:
                raise ConductorError("task is not available to claim")
            self.connection.execute(
                "UPDATE agents SET status = 'busy', updated_at = ? WHERE id = ?",
                (now, agent_id),
            )
        return self.get_task(task_id)

    def start_task(self, task_id: str, agent_id: str) -> dict[str, Any]:
        return self._owned_transition(task_id, agent_id, "claimed", "running")

    def checkpoint_task(
        self, task_id: str, agent_id: str, checkpoint: dict[str, Any]
    ) -> dict[str, Any]:
        now = self.clock()
        with self.connection:
            cursor = self.connection.execute(
                """
                UPDATE tasks SET checkpoint_json = ?, version = version + 1, updated_at = ?
                WHERE id = ? AND owner_agent_id = ? AND state IN ('claimed', 'running')
                """,
                (_json(checkpoint), now, task_id, agent_id),
            )
            if cursor.rowcount != 1:
                raise ConductorError("agent does not own active task")
        return self.get_task(task_id)

    def heartbeat(self, agent_id: str, lease_seconds: float) -> None:
        if lease_seconds <= 0:
            raise ConductorError("lease must be positive")
        now = self.clock()
        with self.connection:
            cursor = self.connection.execute(
                "UPDATE agents SET last_heartbeat = ?, updated_at = ? WHERE id = ?",
                (now, now, agent_id),
            )
            if cursor.rowcount != 1:
                raise ConductorError(f"unknown agent: {agent_id}")
            self.connection.execute(
                """
                UPDATE tasks SET lease_expires_at = ?, updated_at = ?
                WHERE owner_agent_id = ? AND state IN ('claimed', 'running')
                """,
                (now + lease_seconds, now, agent_id),
            )

    def submit_for_verification(
        self, task_id: str, agent_id: str, evidence: dict[str, Any]
    ) -> dict[str, Any]:
        now = self.clock()
        with self.connection:
            cursor = self.connection.execute(
                """
                UPDATE tasks
                SET state = 'verifying', evidence_json = ?, lease_expires_at = NULL,
                    version = version + 1, updated_at = ?
                WHERE id = ? AND owner_agent_id = ? AND state = 'running'
                """,
                (_json(evidence), now, task_id, agent_id),
            )
            if cursor.rowcount != 1:
                raise ConductorError("only the running owner can submit verification")
            self.connection.execute(
                "UPDATE agents SET status = 'idle', updated_at = ? WHERE id = ?",
                (now, agent_id),
            )
        return self.get_task(task_id)

    def verify_task(
        self,
        task_id: str,
        verifier_agent_id: str,
        checks: dict[str, bool],
        note: str = "",
    ) -> dict[str, Any]:
        task = self.get_task(task_id)
        self.get_agent(verifier_agent_id)
        if task["state"] != "verifying":
            raise ConductorError("task is not awaiting verification")
        if task["owner_agent_id"] == verifier_agent_id:
            raise ConductorError("task owner cannot verify its own work")

        required = task["required_checks_json"]
        missing = [name for name in required if checks.get(name) is not True]
        accepted = not missing
        verdict = "accepted" if accepted else "rejected"
        now = self.clock()
        next_state = "completed" if accepted else "queued"
        completed_at = now if accepted else None
        next_owner = task["owner_agent_id"] if accepted else None

        with self.connection:
            self.connection.execute(
                """
                INSERT INTO verifications(task_id, verifier_agent_id, verdict, checks_json, note, created_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (task_id, verifier_agent_id, verdict, _json(checks), note, now),
            )
            self.connection.execute(
                """
                UPDATE tasks
                SET state = ?, owner_agent_id = ?, lease_expires_at = NULL,
                    version = version + 1, updated_at = ?, completed_at = ?
                WHERE id = ? AND state = 'verifying'
                """,
                (next_state, next_owner, now, completed_at, task_id),
            )
        if missing:
            raise ConductorError("verification rejected; missing checks: " + ", ".join(missing))
        return self.get_task(task_id)

    def reconcile_expired(self) -> list[str]:
        now = self.clock()
        rows = self.connection.execute(
            """
            SELECT id, owner_agent_id FROM tasks
            WHERE state IN ('claimed', 'running') AND lease_expires_at <= ?
            """,
            (now,),
        ).fetchall()
        if not rows:
            return []
        task_ids = [row["id"] for row in rows]
        owners = {row["owner_agent_id"] for row in rows if row["owner_agent_id"]}
        with self.connection:
            self.connection.executemany(
                """
                UPDATE tasks
                SET state = 'queued', owner_agent_id = NULL, lease_expires_at = NULL,
                    version = version + 1, updated_at = ?
                WHERE id = ?
                """,
                [(now, task_id) for task_id in task_ids],
            )
            self.connection.executemany(
                "UPDATE agents SET status = 'offline', updated_at = ? WHERE id = ?",
                [(now, owner) for owner in owners],
            )
        return task_ids

    def subscribe(
        self, agent_id: str, topic_pattern: str, path_pattern: str | None = None
    ) -> None:
        self.get_agent(agent_id)
        with self.connection:
            self.connection.execute(
                """
                INSERT OR IGNORE INTO subscriptions(agent_id, topic_pattern, path_pattern)
                VALUES (?, ?, ?)
                """,
                (agent_id, topic_pattern, path_pattern),
            )

    def publish_event(
        self,
        *,
        idempotency_key: str,
        topic: str,
        payload: dict[str, Any],
        source_agent_id: str | None = None,
        path: str | None = None,
        correlation_id: str | None = None,
        generation: int = 0,
        echo_source: bool = False,
    ) -> dict[str, Any]:
        if generation > self.MAX_EVENT_GENERATION:
            raise ConductorError("event generation limit exceeded")
        if source_agent_id is not None:
            self.get_agent(source_agent_id)
        existing = self.connection.execute(
            "SELECT * FROM events WHERE idempotency_key = ?", (idempotency_key,)
        ).fetchone()
        if existing is not None:
            return self._decode(existing)

        event_id = str(uuid.uuid4())
        correlation = correlation_id or event_id
        now = self.clock()
        subscriptions = self.connection.execute("SELECT * FROM subscriptions").fetchall()
        recipients: list[str] = []
        for subscription in subscriptions:
            if not echo_source and subscription["agent_id"] == source_agent_id:
                continue
            if not fnmatch.fnmatchcase(topic, subscription["topic_pattern"]):
                continue
            pattern = subscription["path_pattern"]
            if pattern is not None and (path is None or not fnmatch.fnmatchcase(path, pattern)):
                continue
            recipients.append(subscription["agent_id"])

        with self.connection:
            self.connection.execute(
                """
                INSERT INTO events(id, idempotency_key, correlation_id, generation,
                                   source_agent_id, topic, path, payload_json, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    event_id,
                    idempotency_key,
                    correlation,
                    generation,
                    source_agent_id,
                    topic,
                    path,
                    _json(payload),
                    now,
                ),
            )
            self.connection.executemany(
                """
                INSERT OR IGNORE INTO deliveries(event_id, agent_id, created_at)
                VALUES (?, ?, ?)
                """,
                [(event_id, agent_id, now) for agent_id in recipients],
            )
        return self.get_event(event_id)

    def get_event(self, event_id: str) -> dict[str, Any]:
        row = self.connection.execute(
            "SELECT * FROM events WHERE id = ?", (event_id,)
        ).fetchone()
        if row is None:
            raise ConductorError(f"unknown event: {event_id}")
        return self._decode(row)

    def pending_deliveries(self, agent_id: str) -> list[dict[str, Any]]:
        rows = self.connection.execute(
            """
            SELECT events.* FROM deliveries
            JOIN events ON events.id = deliveries.event_id
            WHERE deliveries.agent_id = ? AND deliveries.status = 'pending'
            ORDER BY deliveries.created_at, events.id
            """,
            (agent_id,),
        ).fetchall()
        return [self._decode(row) for row in rows]

    def acknowledge(self, event_id: str, agent_id: str) -> None:
        now = self.clock()
        with self.connection:
            cursor = self.connection.execute(
                """
                UPDATE deliveries SET status = 'acknowledged', acknowledged_at = ?
                WHERE event_id = ? AND agent_id = ? AND status = 'pending'
                """,
                (now, event_id, agent_id),
            )
            if cursor.rowcount != 1:
                raise ConductorError("pending delivery not found")

    def _owned_transition(
        self, task_id: str, agent_id: str, old_state: str, new_state: str
    ) -> dict[str, Any]:
        now = self.clock()
        with self.connection:
            cursor = self.connection.execute(
                """
                UPDATE tasks SET state = ?, version = version + 1, updated_at = ?
                WHERE id = ? AND owner_agent_id = ? AND state = ?
                """,
                (new_state, now, task_id, agent_id, old_state),
            )
            if cursor.rowcount != 1:
                raise ConductorError(f"invalid transition: {old_state} -> {new_state}")
        return self.get_task(task_id)

    @staticmethod
    def _git_context(root: Path) -> dict[str, Any]:
        command = [
            "git",
            "-C",
            str(root),
            "status",
            "--short",
            "--branch",
            "--untracked-files=normal",
        ]
        try:
            result = subprocess.run(
                command,
                check=False,
                capture_output=True,
                text=True,
                timeout=5,
            )
        except (OSError, subprocess.TimeoutExpired) as error:
            return {"available": False, "error": type(error).__name__}
        if result.returncode != 0:
            return {"available": False, "error": result.stderr.strip()}
        lines = result.stdout.splitlines()
        return {
            "available": True,
            "branch": lines[0][3:] if lines and lines[0].startswith("## ") else None,
            "changes": lines[1:] if lines and lines[0].startswith("## ") else lines,
        }

    @staticmethod
    def _decode(row: sqlite3.Row) -> dict[str, Any]:
        value = dict(row)
        for key in (
            "capabilities_json",
            "required_checks_json",
            "evidence_json",
            "checkpoint_json",
            "checks_json",
            "payload_json",
        ):
            if key in value:
                value[key] = json.loads(value[key])
        return value
