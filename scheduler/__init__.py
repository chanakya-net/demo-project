"""Persistent, crash-safe priority task scheduler (standard library only).

Modules: ``models`` (Task + validation), ``retry`` (backoff policy),
``store`` (atomic JSON snapshots), ``queue`` (TaskQueue), ``cli``.
All timestamps are floats in seconds since the Unix epoch.
"""

from .models import (
    COMPLETED,
    DEAD_LETTER,
    DEFAULT_MAX_RETRIES,
    FAILED,
    PENDING,
    RUNNING,
    STATUSES,
    DependencyCycleError,
    DuplicateTaskError,
    InvalidTransitionError,
    SchedulerError,
    Task,
    TaskNotFoundError,
    ValidationError,
    find_cycle,
)
from .queue import TaskQueue
from .retry import DEFAULT_BASE_DELAY, DEFAULT_MAX_DELAY, apply_failure, compute_backoff
from .store import SCHEMA_VERSION, SnapshotStore, StoreCorruptionError

__all__ = [
    "COMPLETED", "DEAD_LETTER", "DEFAULT_BASE_DELAY", "DEFAULT_MAX_DELAY",
    "DEFAULT_MAX_RETRIES", "FAILED", "PENDING", "RUNNING", "STATUSES",
    "DependencyCycleError", "DuplicateTaskError", "InvalidTransitionError",
    "SCHEMA_VERSION", "SchedulerError", "SnapshotStore", "StoreCorruptionError", "Task", "TaskNotFoundError", "TaskQueue", "ValidationError",
    "apply_failure", "compute_backoff", "find_cycle",
]
