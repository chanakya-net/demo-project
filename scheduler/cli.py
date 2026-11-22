"""Command-line interface: ``python3 -m scheduler.cli --store PATH <command> ...``.

Every successful command prints exactly one JSON document on stdout and exits
0. Handled errors print ``{"error": {"type": ..., "message": ...}}`` on stdout
and exit 1; argparse usage errors exit 2. ``--now`` values are seconds since
the Unix epoch and default to ``time.time()``. ``next`` claims the task (marks
it ``running`` in the store); report the outcome later with ``complete`` or
``fail``. Each invocation is a single writer of the store file.
"""

import argparse
import json
import sys
import time

from .models import DEFAULT_MAX_RETRIES, SchedulerError, ValidationError
from .queue import TaskQueue
from .retry import DEFAULT_BASE_DELAY, DEFAULT_MAX_DELAY
from .store import SnapshotStore


def _add_store_option(parser, dest):
    parser.add_argument("--store", dest=dest, metavar="PATH", default=None,
                        help="path of the JSON snapshot file")


def _add_subcommands(subparsers):
    enqueue = subparsers.add_parser("enqueue", help="add a pending task")
    enqueue.add_argument("task_id")
    enqueue.add_argument("--priority", type=int, default=0)
    enqueue.add_argument("--depends-on", action="append", default=[], metavar="ID")
    enqueue.add_argument("--max-retries", type=int, default=DEFAULT_MAX_RETRIES)
    enqueue.add_argument("--payload", default=None, metavar="JSON_OBJECT")
    enqueue.add_argument("--next-run-at", type=float, default=0.0)

    nxt = subparsers.add_parser("next", help="claim the next runnable task")
    nxt.add_argument("--now", type=float, default=None)

    complete = subparsers.add_parser("complete", help="mark a running task completed")
    complete.add_argument("task_id")

    fail = subparsers.add_parser("fail", help="record a failed attempt of a running task")
    fail.add_argument("task_id")
    fail.add_argument("--error", required=True)
    fail.add_argument("--now", type=float, default=None)
    fail.add_argument("--base-delay", type=float, default=DEFAULT_BASE_DELAY)
    fail.add_argument("--max-delay", type=float, default=DEFAULT_MAX_DELAY)

    subparsers.add_parser("stats", help="count tasks by status")
    for sub in subparsers.choices.values():
        _add_store_option(sub, "store_sub")


def build_parser():
    parser = argparse.ArgumentParser(prog="python3 -m scheduler.cli",
                                     description="Persistent priority task scheduler")
    _add_store_option(parser, "store_global")
    subparsers = parser.add_subparsers(dest="command", required=True)
    _add_subcommands(subparsers)
    return parser


def _parse_payload(text):
    if text is None:
        return None
    try:
        payload = json.loads(text)
    except ValueError as exc:
        raise ValidationError(f"--payload is not valid JSON: {exc}") from None
    if not isinstance(payload, dict):
        raise ValidationError("--payload must be a JSON object")
    return payload


def _now(args):
    return time.time() if args.now is None else args.now


def _run(args, store_path):
    delays = {}
    if args.command == "fail":
        delays = {"base_delay": args.base_delay, "max_delay": args.max_delay}
    queue = TaskQueue(SnapshotStore(store_path), **delays)
    if args.command == "enqueue":
        task = queue.enqueue(args.task_id, priority=args.priority,
                             depends_on=args.depends_on, max_retries=args.max_retries,
                             payload=_parse_payload(args.payload),
                             next_run_at=args.next_run_at)
    elif args.command == "next":
        task = queue.claim_next(_now(args))
    elif args.command == "complete":
        task = queue.complete(args.task_id)
    elif args.command == "fail":
        task = queue.fail(args.task_id, args.error, _now(args))
    else:
        return queue.stats()
    return None if task is None else task.to_dict()


def main(argv=None):
    parser = build_parser()
    args = parser.parse_args(argv)
    store_path = args.store_sub or args.store_global
    if not store_path:
        parser.error("--store is required")
    try:
        result = _run(args, store_path)
    except (SchedulerError, OSError) as exc:
        result = {"error": {"type": type(exc).__name__, "message": str(exc)}}
        print(json.dumps(result, sort_keys=True))
        return 1
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    sys.exit(main())
