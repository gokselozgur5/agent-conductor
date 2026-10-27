from __future__ import annotations

import argparse
import json
from pathlib import Path

from .hooks import DirectoryWatcher
from .store import ConductorError, ConductorStore


def _print(value: object) -> None:
    print(json.dumps(value, indent=2, sort_keys=True))


def parser() -> argparse.ArgumentParser:
    root = argparse.ArgumentParser(prog="conductor")
    root.add_argument("--db", type=Path, default=Path("state/conductor.db"))
    commands = root.add_subparsers(dest="command", required=True)

    commands.add_parser("init")

    register = commands.add_parser("register-agent")
    register.add_argument("agent_id")
    register.add_argument("adapter")
    register.add_argument("--capability", action="append", default=[])

    create = commands.add_parser("create-task")
    create.add_argument("title")
    create.add_argument("--check", action="append", default=[])
    create.add_argument("--project-id")

    commands.add_parser("list-tasks")

    add_project = commands.add_parser("add-project")
    add_project.add_argument("path", type=Path)
    add_project.add_argument("--name")

    commands.add_parser("list-projects")

    project_context = commands.add_parser("project-context")
    project_context.add_argument("project_id")

    board = commands.add_parser("board")
    board.add_argument("--project-id")

    claim = commands.add_parser("claim")
    claim.add_argument("task_id")
    claim.add_argument("agent_id")
    claim.add_argument("--lease", type=float, default=60.0)

    start = commands.add_parser("start")
    start.add_argument("task_id")
    start.add_argument("agent_id")

    subscribe = commands.add_parser("subscribe")
    subscribe.add_argument("agent_id")
    subscribe.add_argument("topic_pattern")
    subscribe.add_argument("--path-pattern")

    pending = commands.add_parser("pending")
    pending.add_argument("agent_id")

    acknowledge = commands.add_parser("acknowledge")
    acknowledge.add_argument("event_id")
    acknowledge.add_argument("agent_id")

    watch = commands.add_parser("watch-once")
    watch.add_argument("root", type=Path)
    watch.add_argument("--snapshot", type=Path, required=True)
    watch.add_argument("--source-agent")
    watch.add_argument("--publish-initial", action="store_true")

    reconcile = commands.add_parser("reconcile")
    reconcile.add_argument("--json", action="store_true")
    return root


def main() -> int:
    args = parser().parse_args()
    store = ConductorStore(args.db)
    try:
        store.initialize()
        if args.command == "init":
            _print({"database": str(args.db), "initialized": True})
        elif args.command == "register-agent":
            _print(store.register_agent(args.agent_id, args.adapter, args.capability))
        elif args.command == "create-task":
            _print(store.create_task(args.title, args.check, args.project_id))
        elif args.command == "list-tasks":
            _print(store.list_tasks())
        elif args.command == "add-project":
            _print(store.register_project(args.path, args.name))
        elif args.command == "list-projects":
            _print(store.list_projects())
        elif args.command == "project-context":
            _print(store.project_context(args.project_id))
        elif args.command == "board":
            _print(store.board(args.project_id))
        elif args.command == "claim":
            _print(store.claim_task(args.task_id, args.agent_id, args.lease))
        elif args.command == "start":
            _print(store.start_task(args.task_id, args.agent_id))
        elif args.command == "subscribe":
            store.subscribe(args.agent_id, args.topic_pattern, args.path_pattern)
            _print({"subscribed": args.agent_id})
        elif args.command == "pending":
            _print(store.pending_deliveries(args.agent_id))
        elif args.command == "acknowledge":
            store.acknowledge(args.event_id, args.agent_id)
            _print({"acknowledged": args.event_id, "agent_id": args.agent_id})
        elif args.command == "watch-once":
            watcher = DirectoryWatcher(
                store, args.root, source_agent_id=args.source_agent
            )
            previous = watcher.load_snapshot(args.snapshot)
            current = watcher.scan()
            event_ids = (
                watcher.publish_changes(previous, current)
                if previous or args.publish_initial
                else []
            )
            watcher.save_snapshot(args.snapshot, current)
            _print({"events": event_ids, "files": len(current)})
        elif args.command == "reconcile":
            _print({"requeued": store.reconcile_expired()})
        return 0
    except ConductorError as error:
        print(f"error: {error}")
        return 2
    finally:
        store.close()


if __name__ == "__main__":
    raise SystemExit(main())
