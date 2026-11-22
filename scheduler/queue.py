"""Priority queue with dependency gating and optional snapshot persistence.

Contract summary:

* ``now`` and ``next_run_at`` are floats in seconds since the Unix epoch.
* A task is runnable at ``now`` when it is ``pending``, ``next_run_at <= now``
  and every id in ``depends_on`` names an existing ``completed`` task. Among
  runnable tasks the smallest ``(priority, created_seq)`` is chosen.
* Running-task handoff: ``claim_next`` moves the chosen task to ``running`` and
  persists that before returning it. The caller then owns the task and must
  report the outcome with ``complete`` or ``fail``. There is no lease or
  timeout; a task left ``running`` (e.g. after a worker crash) stays
  ``running`` until the caller resolves it.
* Persistence: with a store, every mutation is written as a full snapshot
  before it becomes visible in memory. If the write fails the exception
  propagates and both the in-memory queue and the snapshot on disk keep their
  previous state, so the same call can be retried.
* Single writer: one ``TaskQueue`` instance per snapshot file at a time.
  Concurrent writers (threads or processes) are not supported; there is no
  locking and the last writer wins.
"""

import copy

from .models import (
    COMPLETED,
    DEFAULT_MAX_RETRIES,
    PENDING,
    RUNNING,
    STATUSES,
    InvalidTransitionError,
    Task,
    TaskNotFoundError,
    validate_enqueue,
)
from .retry import DEFAULT_BASE_DELAY, DEFAULT_MAX_DELAY, apply_failure, compute_backoff


class TaskQueue:
    def __init__(self, store=None, *, base_delay=DEFAULT_BASE_DELAY,
                 max_delay=DEFAULT_MAX_DELAY):
        compute_backoff(1, base_delay, max_delay)  # validates the delays
        self._store = store
        self._base_delay = base_delay
        self._max_delay = max_delay
        self._tasks = {}
        self._next_seq = 0
        if store is not None:
            tasks, self._next_seq = store.load()
            self._tasks = {t.id: t for t in tasks}

    def enqueue(self, task_id, *, priority=0, depends_on=None,
                max_retries=DEFAULT_MAX_RETRIES, payload=None, next_run_at=0.0):
        """Validate and add a new ``pending`` task; returns a copy of it."""
        task = Task(
            id=task_id,
            priority=priority,
            depends_on=list(depends_on) if depends_on is not None else [],
            max_retries=max_retries,
            payload=copy.deepcopy(payload) if payload is not None else {},
            next_run_at=next_run_at,
            created_seq=self._next_seq,
        )
        validate_enqueue(task, self._tasks)
        self._commit(task, self._next_seq + 1)
        return copy.deepcopy(task)

    def claim_next(self, now):
        """Mark the next runnable task ``running`` and return a copy, or None."""
        runnable = [t for t in self._tasks.values() if self._is_runnable(t, now)]
        if not runnable:
            return None
        chosen = min(runnable, key=lambda t: (t.priority, t.created_seq))
        return self._transition(chosen.id, PENDING, self._mark_running)

    def complete(self, task_id):
        """Move a ``running`` task to ``completed``; returns a copy."""
        return self._transition(task_id, RUNNING, self._mark_completed)

    def fail(self, task_id, error, now):
        """Record a failed attempt of a ``running`` task; returns a copy.

        The task returns to ``pending`` with a backoff delay, or moves to
        ``dead_letter`` once its retries are exhausted (see ``apply_failure``).
        """
        def mutate(task):
            apply_failure(task, error, now, self._base_delay, self._max_delay)
        return self._transition(task_id, RUNNING, mutate)

    def stats(self):
        counts = {status: 0 for status in STATUSES}
        for task in self._tasks.values():
            counts[task.status] += 1
        return {"total": len(self._tasks), **counts}

    def get(self, task_id):
        return copy.deepcopy(self._get(task_id))

    def _get(self, task_id):
        try:
            return self._tasks[task_id]
        except KeyError:
            raise TaskNotFoundError(f"task {task_id!r} not found") from None

    def _is_runnable(self, task, now):
        if task.status != PENDING or task.next_run_at > now:
            return False
        for dep_id in task.depends_on:
            dep = self._tasks.get(dep_id)
            if dep is None or dep.status != COMPLETED:
                return False
        return True

    def _transition(self, task_id, expected_status, mutate):
        current = self._get(task_id)
        if current.status != expected_status:
            raise InvalidTransitionError(
                f"task {task_id!r} is {current.status!r}, expected {expected_status!r}")
        updated = copy.deepcopy(current)
        mutate(updated)
        self._commit(updated, self._next_seq)
        return copy.deepcopy(updated)

    @staticmethod
    def _mark_running(task):
        task.status = RUNNING

    @staticmethod
    def _mark_completed(task):
        task.status = COMPLETED

    def _commit(self, task, next_seq):
        """Persist the candidate state first, then make it live in memory."""
        candidate = dict(self._tasks)
        candidate[task.id] = task
        if self._store is not None:
            self._store.save(candidate.values(), next_seq)
        self._tasks = candidate
        self._next_seq = next_seq
