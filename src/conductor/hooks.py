from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

from .store import ConductorStore


@dataclass(frozen=True)
class FileState:
    digest: str
    size: int


class DirectoryWatcher:
    """Deterministic polling watcher that turns stable file states into events."""

    def __init__(
        self,
        store: ConductorStore,
        root: str | Path,
        *,
        source_agent_id: str | None = None,
        excluded_parts: Iterable[str] = (".git", ".venv", "__pycache__", "state"),
    ) -> None:
        self.store = store
        self.root = Path(root).resolve()
        self.source_agent_id = source_agent_id
        self.excluded_parts = frozenset(excluded_parts)

    def load_snapshot(self, snapshot_path: str | Path) -> dict[str, FileState]:
        path = Path(snapshot_path)
        if not path.exists():
            return {}
        raw = json.loads(path.read_text(encoding="utf-8"))
        return {key: FileState(**value) for key, value in raw.items()}

    def save_snapshot(
        self, snapshot_path: str | Path, snapshot: dict[str, FileState]
    ) -> None:
        path = Path(snapshot_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            key: {"digest": value.digest, "size": value.size}
            for key, value in sorted(snapshot.items())
        }
        temporary = path.with_suffix(path.suffix + ".tmp")
        temporary.write_text(json.dumps(payload, sort_keys=True), encoding="utf-8")
        temporary.replace(path)

    def scan(self) -> dict[str, FileState]:
        snapshot: dict[str, FileState] = {}
        for path in sorted(self.root.rglob("*")):
            if not path.is_file():
                continue
            relative = path.relative_to(self.root)
            if any(part in self.excluded_parts for part in relative.parts):
                continue
            data = path.read_bytes()
            snapshot[relative.as_posix()] = FileState(
                digest=hashlib.sha256(data).hexdigest(), size=len(data)
            )
        return snapshot

    def publish_changes(
        self,
        previous: dict[str, FileState],
        current: dict[str, FileState],
    ) -> list[str]:
        event_ids: list[str] = []
        paths = sorted(previous.keys() | current.keys())
        for relative in paths:
            before = previous.get(relative)
            after = current.get(relative)
            if before == after:
                continue
            if before is None:
                topic = "file.created"
            elif after is None:
                topic = "file.deleted"
            else:
                topic = "file.updated"
            state_token = "deleted" if after is None else after.digest
            event = self.store.publish_event(
                idempotency_key=f"watch:{self.root}:{relative}:{topic}:{state_token}",
                correlation_id=f"watch:{self.root}:{relative}",
                source_agent_id=self.source_agent_id,
                topic=topic,
                path=relative,
                payload={
                    "root": str(self.root),
                    "path": relative,
                    "before": None if before is None else before.digest,
                    "after": None if after is None else after.digest,
                    "size": None if after is None else after.size,
                },
            )
            event_ids.append(event["id"])
        return event_ids
