"""Tests for the persistent priority task scheduler (standard library only)."""

import contextlib
import io
import json
import math
import os
import subprocess
import sys
import tempfile
import unittest
from unittest import mock

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from scheduler import (  # noqa: E402
    DependencyCycleError,
    DuplicateTaskError,
    InvalidTransitionError,
    SCHEMA_VERSION,
    SnapshotStore,
    StoreCorruptionError,
    Task,
    TaskNotFoundError,
    TaskQueue,
    ValidationError,
    apply_failure,
    compute_backoff,
    find_cycle,
)
from scheduler import cli  # noqa: E402


class QueueOrderingTests(unittest.TestCase):
    def test_claims_by_priority_then_fifo(self):
        q = TaskQueue()
        q.enqueue("low", priority=5)
        q.enqueue("first", priority=1)
        q.enqueue("second", priority=1)
        claimed = [q.claim_next(now=0.0).id for _ in range(3)]
        self.assertEqual(claimed, ["first", "second", "low"])
        self.assertEqual(q.get("first").status, "running")
        self.assertIsNone(q.claim_next(now=0.0))


class ValidationTests(unittest.TestCase):
    def test_rejects_duplicate_id_and_negative_retries(self):
        q = TaskQueue()
        q.enqueue("a")
        before = q.stats()
        with self.assertRaises(DuplicateTaskError):
            q.enqueue("a", priority=9)
        with self.assertRaises(ValidationError):
            q.enqueue("b", max_retries=-1)
        self.assertEqual(q.stats(), before)
        self.assertEqual(q.get("a").priority, 0)

    def test_rejects_bad_field_types(self):
        q = TaskQueue()
        bad = [
            {"priority": True},
            {"priority": 1.5},
            {"max_retries": False},
            {"next_run_at": math.nan},
            {"next_run_at": math.inf},
            {"payload": []},
            {"payload": {"x": object()}},
            {"depends_on": ["dup", "dup"]},
            {"depends_on": [""]},
        ]
        for kwargs in bad:
            with self.subTest(kwargs=kwargs), self.assertRaises(ValidationError):
                q.enqueue("t", **kwargs)
        with self.assertRaises(ValidationError):
            q.enqueue("")
        with self.assertRaises(ValidationError):
            Task(id="x", status="bogus")
        self.assertEqual(q.stats()["total"], 0)

    def test_failed_status_is_valid_and_counted(self):
        self.assertEqual(Task(id="x", status="failed").status, "failed")
        self.assertEqual(TaskQueue().stats(), {
            "total": 0, "pending": 0, "running": 0, "completed": 0,
            "failed": 0, "dead_letter": 0,
        })

    def test_returned_tasks_are_copies(self):
        q = TaskQueue()
        task = q.enqueue("a", payload={"k": "v"})
        task.payload["k"] = "changed"
        task.status = "completed"
        self.assertEqual(q.get("a").payload, {"k": "v"})
        self.assertEqual(q.get("a").status, "pending")


class DependencyTests(unittest.TestCase):
    def test_dependent_unblocks_after_dependencies_complete(self):
        q = TaskQueue()
        q.enqueue("child", priority=0, depends_on=["a", "b"])
        q.enqueue("a", priority=5)
        q.enqueue("b", priority=6)
        self.assertEqual(q.claim_next(now=0.0).id, "a")
        q.complete("a")
        self.assertEqual(q.claim_next(now=0.0).id, "b")
        self.assertIsNone(q.claim_next(now=0.0))
        q.complete("b")
        self.assertEqual(q.claim_next(now=0.0).id, "child")

    def test_missing_dependency_keeps_task_blocked(self):
        q = TaskQueue()
        q.enqueue("child", depends_on=["ghost"])
        self.assertIsNone(q.claim_next(now=0.0))

    def test_next_run_at_gates_claim_inclusively(self):
        q = TaskQueue()
        q.enqueue("later", next_run_at=100.0)
        self.assertIsNone(q.claim_next(now=99.9))
        self.assertEqual(q.claim_next(now=100.0).id, "later")

    def test_complete_rejects_unknown_or_non_running_task(self):
        q = TaskQueue()
        q.enqueue("a")
        with self.assertRaises(TaskNotFoundError):
            q.complete("nope")
        with self.assertRaises(InvalidTransitionError):
            q.complete("a")
        q.claim_next(now=0.0)
        q.complete("a")
        with self.assertRaises(InvalidTransitionError):
            q.complete("a")
        with self.assertRaises(InvalidTransitionError):
            q.fail("a", "boom", now=0.0)


class CycleTests(unittest.TestCase):
    def test_enqueue_rejects_dependency_cycle(self):
        q = TaskQueue()
        with self.assertRaises(DependencyCycleError):
            q.enqueue("self", depends_on=["self"])
        q.enqueue("a", depends_on=["b"])
        with self.assertRaises(DependencyCycleError):
            q.enqueue("b", depends_on=["a"])
        q.enqueue("x", depends_on=["y"])
        q.enqueue("y", depends_on=["z"])
        before = q.stats()
        with self.assertRaises(DependencyCycleError):
            q.enqueue("z", depends_on=["x"])
        self.assertEqual(q.stats(), before)
        with self.assertRaises(TaskNotFoundError):
            q.get("z")
        q.enqueue("z")  # acyclic once the back edge is gone

    def test_find_cycle_handles_long_chains_and_diamonds(self):
        chain = {str(i): [str(i + 1)] for i in range(5000)}
        self.assertIsNone(find_cycle(chain))
        diamond = {"a": ["b", "c"], "b": ["d"], "c": ["d"], "d": []}
        self.assertIsNone(find_cycle(diamond))
        self.assertEqual(find_cycle({"a": ["b"], "b": ["a"]}), ["a", "b", "a"])


class RetryTests(unittest.TestCase):
    def test_compute_backoff_doubles_and_caps(self):
        self.assertEqual(
            [compute_backoff(n, 2.0, 10.0) for n in range(1, 6)],
            [2.0, 4.0, 8.0, 10.0, 10.0],
        )
        self.assertEqual(compute_backoff(10**6, 1.0, 60.0), 60.0)
        for args in [(0, 1.0, 10.0), (True, 1.0, 10.0), (1, -1.0, 10.0), (1, 1.0, math.inf)]:
            with self.subTest(args=args), self.assertRaises(ValidationError):
                compute_backoff(*args)

    def test_fail_backs_off_exponentially_then_dead_letters(self):
        q = TaskQueue(base_delay=10.0, max_delay=1000.0)
        q.enqueue("job", max_retries=3)
        q.claim_next(now=0.0)
        t = q.fail("job", "boom1", now=100.0)
        self.assertEqual((t.status, t.attempts, t.next_run_at, t.last_error),
                         ("pending", 1, 110.0, "boom1"))
        self.assertIsNone(q.claim_next(now=109.9))
        self.assertEqual(q.claim_next(now=110.0).id, "job")
        t = q.fail("job", "boom2", now=110.0)
        self.assertEqual((t.status, t.attempts, t.next_run_at), ("pending", 2, 130.0))
        q.claim_next(now=130.0)
        self.assertEqual(q.stats()["running"], 1)
        t = q.fail("job", "boom3", now=130.0)
        self.assertEqual((t.status, t.attempts, t.last_error), ("dead_letter", 3, "boom3"))
        self.assertIsNone(q.claim_next(now=10**9))
        self.assertEqual(q.stats()["dead_letter"], 1)
        with self.assertRaises(InvalidTransitionError):
            q.fail("job", "again", now=200.0)

    def test_zero_retries_dead_letters_on_first_failure(self):
        task = Task(id="x", status="running", max_retries=0)
        apply_failure(task, "boom", now=5.0)
        self.assertEqual((task.status, task.attempts), ("dead_letter", 1))

    def test_dead_letter_dependency_keeps_dependent_blocked(self):
        q = TaskQueue()
        q.enqueue("dep", max_retries=0)
        q.enqueue("child", depends_on=["dep"])
        q.claim_next(now=0.0)
        q.fail("dep", "boom", now=0.0)
        self.assertIsNone(q.claim_next(now=10**9))
        self.assertEqual(q.get("child").status, "pending")


class _TempDirCase(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.dir = self._tmp.name
        self.path = os.path.join(self.dir, "store.json")

    def read_bytes(self):
        with open(self.path, "rb") as fh:
            return fh.read()

    def write_text(self, text):
        with open(self.path, "w", encoding="utf-8") as fh:
            fh.write(text)


class PersistenceTests(_TempDirCase):
    def test_state_survives_reload_across_instances(self):
        q1 = TaskQueue(SnapshotStore(self.path), base_delay=5.0)
        q1.enqueue("a", priority=1, payload={"k": "v"})
        q1.enqueue("b", priority=1)
        q1.enqueue("c", priority=0, depends_on=["a"])
        q1.claim_next(now=0.0)
        q1.complete("a")
        q1.claim_next(now=0.0)
        q1.fail("c", "boom", now=50.0)
        with open(self.path, encoding="utf-8") as fh:
            self.assertEqual(json.load(fh)["schema_version"], SCHEMA_VERSION)
        q2 = TaskQueue(SnapshotStore(self.path))
        for tid in ("a", "b", "c"):
            self.assertEqual(q2.get(tid), q1.get(tid))
        self.assertEqual(q2.stats(), q1.stats())
        self.assertEqual(q2.claim_next(now=0.0).id, "b")
        self.assertEqual(q2.enqueue("d").created_seq, 3)
        q3 = TaskQueue(SnapshotStore(self.path))
        self.assertEqual(q3.get("b").status, "running")
        self.assertEqual(q3.get("c").next_run_at, 55.0)

    def test_failed_save_leaves_queue_and_snapshot_unchanged(self):
        store = SnapshotStore(self.path)
        q = TaskQueue(store)
        q.enqueue("a")
        before = self.read_bytes()
        with mock.patch("scheduler.store.os.replace", side_effect=OSError("disk full")):
            with self.assertRaises(OSError):
                q.claim_next(now=0.0)
            with self.assertRaises(OSError):
                q.enqueue("b")
        self.assertEqual(q.get("a").status, "pending")
        self.assertEqual(q.stats()["total"], 1)
        self.assertEqual(self.read_bytes(), before)
        self.assertEqual(q.claim_next(now=0.0).id, "a")  # retry succeeds
        self.assertEqual(q.enqueue("b").created_seq, 1)
        self.assertEqual(TaskQueue(SnapshotStore(self.path)).get("a").status, "running")


class StoreTests(_TempDirCase):
    def test_missing_file_loads_empty(self):
        self.assertEqual(SnapshotStore(self.path).load(), ([], 0))

    def test_failed_write_keeps_previous_snapshot(self):
        store = SnapshotStore(self.path)
        store.save([Task(id="a")], 1)
        before = self.read_bytes()
        for target in ("scheduler.store.os.replace", "scheduler.store.os.fsync"):
            with self.subTest(target=target):
                with mock.patch(target, side_effect=OSError("boom")):
                    with self.assertRaises(OSError):
                        store.save([Task(id="a"), Task(id="b", created_seq=1)], 2)
                self.assertEqual(self.read_bytes(), before)
                self.assertEqual(os.listdir(self.dir), ["store.json"])

    def test_save_rejects_unserializable_state_without_touching_file(self):
        store = SnapshotStore(self.path)
        store.save([Task(id="a")], 1)
        before = self.read_bytes()
        with self.assertRaises(ValidationError):
            store.save([Task(id="a")], 0)  # next_seq must exceed created_seq
        self.assertEqual(self.read_bytes(), before)
        self.assertEqual(os.listdir(self.dir), ["store.json"])

    def test_load_rejects_corrupt_snapshots(self):
        good_task = Task(id="a").to_dict()
        cases = {
            "zero_byte": "",
            "truncated": '{"schema_version": 1, "next_seq": 1, "tasks": [',
            "not_object": "[]",
            "missing_version": json.dumps({"next_seq": 0, "tasks": []}),
            "wrong_version": json.dumps({"schema_version": 2, "next_seq": 0, "tasks": []}),
            "bool_version": json.dumps({"schema_version": True, "next_seq": 0, "tasks": []}),
            "tasks_not_list": json.dumps({"schema_version": 1, "next_seq": 0, "tasks": {}}),
            "extra_top_key": json.dumps(
                {"schema_version": 1, "next_seq": 1, "tasks": [good_task], "x": 1}),
            "bad_task": json.dumps({"schema_version": 1, "next_seq": 1,
                                    "tasks": [dict(good_task, max_retries=-1)]}),
            "unknown_task_key": json.dumps({"schema_version": 1, "next_seq": 1,
                                            "tasks": [dict(good_task, extra=1)]}),
            "duplicate_ids": json.dumps({"schema_version": 1, "next_seq": 2, "tasks": [
                good_task, dict(good_task, created_seq=1)]}),
            "duplicate_seq": json.dumps({"schema_version": 1, "next_seq": 2, "tasks": [
                good_task, dict(good_task, id="b")]}),
            "bad_next_seq": json.dumps({"schema_version": 1, "next_seq": 0,
                                        "tasks": [good_task]}),
            "cycle": json.dumps({"schema_version": 1, "next_seq": 2, "tasks": [
                dict(good_task, depends_on=["b"]),
                dict(good_task, id="b", depends_on=["a"], created_seq=1)]}),
        }
        for name, text in cases.items():
            with self.subTest(name=name):
                self.write_text(text)
                with self.assertRaises(StoreCorruptionError):
                    SnapshotStore(self.path).load()
        with open(self.path, "wb") as fh:
            fh.write(b"\xff\xfe")
        with self.assertRaises(StoreCorruptionError):
            SnapshotStore(self.path).load()


class CliTests(_TempDirCase):
    def run_cli(self, *argv):
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            code = cli.main(list(argv))
        return code, json.loads(out.getvalue())

    def test_full_lifecycle_via_cli(self):
        store = ["--store", self.path]
        code, task = self.run_cli(*store, "enqueue", "a", "--priority", "2",
                                  "--payload", '{"n": 1}')
        self.assertEqual((code, task["id"], task["payload"]), (0, "a", {"n": 1}))
        code, task = self.run_cli("enqueue", "b", "--depends-on", "a",
                                  "--max-retries", "1", *store)
        self.assertEqual((code, task["depends_on"]), (0, ["a"]))
        code, task = self.run_cli(*store, "next", "--now", "10")
        self.assertEqual((code, task["id"], task["status"]), (0, "a", "running"))
        code, task = self.run_cli(*store, "next", "--now", "10")
        self.assertEqual((code, task), (0, None))
        code, task = self.run_cli(*store, "complete", "a")
        self.assertEqual(task["status"], "completed")
        self.run_cli(*store, "next", "--now", "10")
        code, task = self.run_cli(*store, "fail", "b", "--error", "boom", "--now", "10")
        self.assertEqual((code, task["status"], task["last_error"]), (0, "dead_letter", "boom"))
        code, stats = self.run_cli(*store, "stats")
        self.assertEqual(stats, {"total": 2, "pending": 0, "running": 0, "completed": 1,
                                 "failed": 0, "dead_letter": 1})

    def test_fail_applies_backoff_options(self):
        store = ["--store", self.path]
        self.run_cli(*store, "enqueue", "a")
        self.run_cli(*store, "next", "--now", "0")
        code, task = self.run_cli(*store, "fail", "a", "--error", "e", "--now", "100",
                                  "--base-delay", "7", "--max-delay", "50")
        self.assertEqual((code, task["status"], task["next_run_at"]), (0, "pending", 107.0))

    def test_errors_print_json_and_exit_1(self):
        store = ["--store", self.path]
        self.run_cli(*store, "enqueue", "a")
        cases = [
            (("enqueue", "a"), "DuplicateTaskError"),
            (("enqueue", "b", "--max-retries", "-1"), "ValidationError"),
            (("enqueue", "c", "--payload", "not json"), "ValidationError"),
            (("enqueue", "c", "--payload", "[1]"), "ValidationError"),
            (("complete", "missing"), "TaskNotFoundError"),
            (("complete", "a"), "InvalidTransitionError"),
        ]
        for argv, err_type in cases:
            with self.subTest(argv=argv):
                code, out = self.run_cli(*store, *argv)
                self.assertEqual(code, 1)
                self.assertEqual(out["error"]["type"], err_type)
        self.write_text("{broken")
        code, out = self.run_cli(*store, "stats")
        self.assertEqual((code, out["error"]["type"]), (1, "StoreCorruptionError"))

    def test_missing_store_is_usage_error(self):
        with contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit) as ctx:
            cli.main(["stats"])
        self.assertEqual(ctx.exception.code, 2)

    def test_module_entry_point_subprocess(self):
        env = dict(os.environ, PYTHONDONTWRITEBYTECODE="1")
        proc = subprocess.run(
            [sys.executable, "-m", "scheduler.cli", "--store", self.path, "enqueue", "x"],
            cwd=REPO_ROOT, env=env, capture_output=True, text=True, check=False,
        )
        self.assertEqual((proc.returncode, proc.stderr), (0, ""))
        self.assertEqual(json.loads(proc.stdout)["id"], "x")


if __name__ == "__main__":
    unittest.main()
