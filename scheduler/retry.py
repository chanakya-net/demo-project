"""Exponential backoff and the failure state transition.

Retry counting: ``Task.attempts`` counts *failed* attempts. Each failure
increments it; once ``attempts >= max_retries`` the task is dead-lettered.
So ``max_retries=3`` allows three failed runs and ``max_retries=0`` dead-letters
on the first failure. Delays and timestamps are in seconds (epoch floats).
"""

from .models import DEAD_LETTER, PENDING, ValidationError, _is_finite_number, _is_int

DEFAULT_BASE_DELAY = 1.0
DEFAULT_MAX_DELAY = 300.0


def compute_backoff(attempt, base_delay, max_delay):
    """Delay in seconds before retry number ``attempt`` (1-based).

    ``min(max_delay, base_delay * 2 ** (attempt - 1))``, without jitter.
    """
    if not _is_int(attempt) or attempt < 1:
        raise ValidationError("attempt must be an int >= 1")
    for name, value in (("base_delay", base_delay), ("max_delay", max_delay)):
        if not _is_finite_number(value) or value < 0:
            raise ValidationError(f"{name} must be a finite number >= 0")
    try:
        delay = float(base_delay) * (2.0 ** (attempt - 1))
    except OverflowError:
        return float(max_delay)
    return float(min(max_delay, delay))


def apply_failure(task, error, now, base_delay=DEFAULT_BASE_DELAY,
                  max_delay=DEFAULT_MAX_DELAY):
    """Record a failed attempt on ``task`` in place and return it.

    Sets ``last_error``, increments ``attempts``, then either dead-letters the
    task (``attempts >= max_retries``) or returns it to ``pending`` with
    ``next_run_at = now + compute_backoff(attempts, base_delay, max_delay)``.
    """
    if not isinstance(error, str):
        raise ValidationError("error must be a string")
    if not _is_finite_number(now):
        raise ValidationError("now must be a finite number")
    delay = compute_backoff(task.attempts + 1, base_delay, max_delay)
    task.attempts += 1
    task.last_error = error
    if task.attempts >= task.max_retries:
        task.status = DEAD_LETTER
    else:
        task.status = PENDING
        task.next_run_at = float(now) + delay
    return task
