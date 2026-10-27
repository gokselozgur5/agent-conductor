from __future__ import annotations

import argparse
import json
from pathlib import Path

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

    commands.add_parser("list-tasks")

    claim = commands.add_parser("claim")
    claim.add_argument("task_id")
    claim.add_argument("agent_id")
    claim.add_argument("--lease", type=float, default=60.0)

    start = commands.add_parser("start")
    start.add_argument("task_id")
    start.add_argument("agent_id")

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
            _print(store.create_task(args.title, args.check))
        elif args.command == "list-tasks":
            _print(store.list_tasks())
        elif args.command == "claim":
            _print(store.claim_task(args.task_id, args.agent_id, args.lease))
        elif args.command == "start":
            _print(store.start_task(args.task_id, args.agent_id))
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
