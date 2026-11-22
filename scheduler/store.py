"""Schema-versioned JSON snapshot store with atomic, crash-safe writes.

Snapshot format (``schema_version`` 1)::

    {"schema_version": 1, "next_seq": <int>, "tasks": [<Task.to_dict()>, ...]}

Guarantees:

* ``save`` serializes and validates the full snapshot first, writes it to a
  temp file in the target's directory, flushes and fsyncs it, then publishes
  it with ``os.replace``. Readers therefore see either the previous snapshot
  or the new one, never a partial file. If anything fails before the replace,
  the temp file is removed, the previous snapshot is left untouched and the
  error propagates. The parent directory is fsynced best-effort afterwards.
* ``load`` treats a missing file as an empty store and raises
  ``StoreCorruptionError`` for anything that is not a valid snapshot.
* Single writer: no file locking is performed. Concurrent writers to the
  same path are unsupported (last writer wins).
"""

import json
import os
import tempfile

from .models import SchedulerError, Task, ValidationError, _is_int, find_cycle

SCHEMA_VERSION = 1
_TOP_LEVEL_KEYS = {"schema_version", "next_seq", "tasks"}


class StoreCorruptionError(SchedulerError):
    """The snapshot file exists but is not a valid snapshot."""


def validate_snapshot(tasks, next_seq):
    """Check cross-task invariants; raises ``ValidationError``."""
    if not _is_int(next_seq) or next_seq < 0:
        raise ValidationError("next_seq must be a non-negative int")
    ids, seqs = set(), set()
    for task in tasks:
        if not isinstance(task, Task):
            raise ValidationError("tasks must be Task instances")
        task.validate()
        if task.id in ids:
            raise ValidationError(f"duplicate task id {task.id!r}")
        if task.created_seq in seqs or task.created_seq >= next_seq:
            raise ValidationError(f"invalid created_seq for task {task.id!r}")
        ids.add(task.id)
        seqs.add(task.created_seq)
    cycle = find_cycle({t.id: t.depends_on for t in tasks})
    if cycle:
        raise ValidationError("dependency cycle: " + " -> ".join(cycle))


class SnapshotStore:
    def __init__(self, path):
        self.path = os.path.abspath(os.fspath(path))

    def load(self):
        """Return ``(tasks, next_seq)`` from the snapshot file."""
        try:
            with open(self.path, "rb") as fh:
                raw = fh.read()
        except FileNotFoundError:
            return [], 0
        try:
            return self._parse(raw)
        except (ValueError, TypeError) as exc:  # includes ValidationError
            raise StoreCorruptionError(f"{self.path}: {exc}") from exc

    @staticmethod
    def _parse(raw):
        data = json.loads(raw.decode("utf-8"))
        if not isinstance(data, dict) or set(data) != _TOP_LEVEL_KEYS:
            raise ValueError(f"snapshot must be an object with keys {sorted(_TOP_LEVEL_KEYS)}")
        version = data["schema_version"]
        if not _is_int(version) or version != SCHEMA_VERSION:
            raise ValueError(f"unsupported schema_version {version!r}")
        if not isinstance(data["tasks"], list):
            raise ValueError("tasks must be a list")
        tasks = [Task.from_dict(record) for record in data["tasks"]]
        validate_snapshot(tasks, data["next_seq"])
        return tasks, data["next_seq"]

    def save(self, tasks, next_seq):
        """Atomically replace the snapshot with ``tasks`` and ``next_seq``."""
        tasks = list(tasks)
        validate_snapshot(tasks, next_seq)
        text = json.dumps(
            {"schema_version": SCHEMA_VERSION, "next_seq": next_seq,
             "tasks": [t.to_dict() for t in tasks]},
            sort_keys=True, indent=2, allow_nan=False,
        )
        directory = os.path.dirname(self.path)
        fd, tmp_path = tempfile.mkstemp(prefix=".snapshot-", suffix=".tmp", dir=directory)
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as fh:
                fh.write(text)
                fh.flush()
                os.fsync(fh.fileno())
            os.replace(tmp_path, self.path)
        except BaseException:
            _remove_quietly(tmp_path)
            raise
        _fsync_directory(directory)


def _remove_quietly(path):
    try:
        os.remove(path)
    except OSError:
        pass


def _fsync_directory(directory):
    try:
        fd = os.open(directory, os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(fd)
    except OSError:
        pass
    finally:
        os.close(fd)
