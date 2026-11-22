"""Task data model, validation rules, and dependency-cycle detection.

Timestamps (``next_run_at`` and every ``now`` argument in this package) are
floats in seconds since the Unix epoch, as returned by ``time.time()``.
"""

import json
import math
from dataclasses import asdict, dataclass, field, fields

PENDING = "pending"
RUNNING = "running"
COMPLETED = "completed"
FAILED = "failed"
DEAD_LETTER = "dead_letter"
STATUSES = (PENDING, RUNNING, COMPLETED, FAILED, DEAD_LETTER)

DEFAULT_MAX_RETRIES = 3


class SchedulerError(Exception):
    """Base class for every error raised by the scheduler package."""


class ValidationError(SchedulerError, ValueError):
    """A task or argument violates the data model."""


class DuplicateTaskError(ValidationError):
    """A task with the same id already exists."""


class DependencyCycleError(ValidationError):
    """Accepting a task would create a dependency cycle."""


class TaskNotFoundError(SchedulerError):
    """No task with the requested id exists."""


class InvalidTransitionError(SchedulerError):
    """The task's current status does not allow the requested transition."""


def _is_int(value):
    return isinstance(value, int) and not isinstance(value, bool)


def _is_finite_number(value):
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return False
    return math.isfinite(value)


def _require(condition, message):
    if not condition:
        raise ValidationError(message)


@dataclass
class Task:
    """One schedulable unit of work.

    ``priority``: lower values run first. ``attempts``: number of failed
    attempts recorded so far (see ``scheduler.retry.apply_failure``).
    ``created_seq``: enqueue order, used as the FIFO tie-breaker.
    """

    id: str
    priority: int = 0
    status: str = PENDING
    depends_on: list = field(default_factory=list)
    max_retries: int = DEFAULT_MAX_RETRIES
    attempts: int = 0
    next_run_at: float = 0.0
    payload: dict = field(default_factory=dict)
    last_error: str = None
    created_seq: int = 0

    def __post_init__(self):
        self.validate()

    def validate(self):
        _require(isinstance(self.id, str) and self.id != "", "id must be a non-empty string")
        _require(_is_int(self.priority), "priority must be an int")
        _require(self.status in STATUSES, f"status must be one of {STATUSES}")
        self._validate_depends_on()
        _require(_is_int(self.max_retries) and self.max_retries >= 0,
                 "max_retries must be a non-negative int")
        _require(_is_int(self.attempts) and self.attempts >= 0,
                 "attempts must be a non-negative int")
        _require(_is_finite_number(self.next_run_at), "next_run_at must be a finite number")
        _require(self.last_error is None or isinstance(self.last_error, str),
                 "last_error must be a string or None")
        _require(_is_int(self.created_seq) and self.created_seq >= 0,
                 "created_seq must be a non-negative int")
        self._validate_payload()

    def _validate_depends_on(self):
        deps = self.depends_on
        _require(isinstance(deps, list), "depends_on must be a list")
        _require(all(isinstance(d, str) and d for d in deps),
                 "depends_on entries must be non-empty strings")
        _require(len(set(deps)) == len(deps), "depends_on must not contain duplicates")

    def _validate_payload(self):
        _require(isinstance(self.payload, dict), "payload must be a dict")
        try:
            json.dumps(self.payload, allow_nan=False)
        except (TypeError, ValueError) as exc:
            raise ValidationError(f"payload must be JSON-serializable: {exc}") from None

    def to_dict(self):
        return asdict(self)

    @classmethod
    def from_dict(cls, data):
        """Build a task from a dict holding exactly the task's fields."""
        _require(isinstance(data, dict), "task record must be an object")
        expected = {f.name for f in fields(cls)}
        _require(set(data) == expected,
                 f"task record keys must be exactly {sorted(expected)}")
        return cls(**data)


def find_cycle(graph):
    """Return one dependency cycle as a list of ids, or None.

    ``graph`` maps a task id to the ids it depends on. Ids without an entry
    are treated as leaves (not-yet-enqueued forward references).
    """
    state = {}  # id -> 1 while on the DFS stack, 2 once fully explored
    for root in graph:
        if state.get(root):
            continue
        cycle = _explore(graph, root, state)
        if cycle:
            return cycle
    return None


def _explore(graph, root, state):
    path = [root]
    stack = [iter(graph.get(root, ()))]
    state[root] = 1
    while stack:
        child = next(stack[-1], None)
        if child is None:
            state[path.pop()] = 2
            stack.pop()
            continue
        if state.get(child) == 1:
            return path[path.index(child):] + [child]
        if state.get(child) is None:
            state[child] = 1
            path.append(child)
            stack.append(iter(graph.get(child, ())))
    return None


def validate_enqueue(task, existing):
    """Reject ``task`` if its id is taken or it would close a dependency cycle."""
    if task.id in existing:
        raise DuplicateTaskError(f"task {task.id!r} already exists")
    graph = {tid: t.depends_on for tid, t in existing.items()}
    graph[task.id] = task.depends_on
    cycle = find_cycle(graph)
    if cycle:
        raise DependencyCycleError("dependency cycle: " + " -> ".join(cycle))
