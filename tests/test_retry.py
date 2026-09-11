"""The retry policy, including the promises the brief makes about it.

These are pure functions, so they are checked against the *production* defaults rather
than the compressed test timings - the point is to catch someone tuning the schedule
down to something that no longer survives a recipient being down for half an hour.
"""

from __future__ import annotations

import pytest
from pydantic_core import PydanticUndefined

from app.config import ConfigurationError, Settings
from app.retry import (
    base_delay_seconds,
    is_exhausted,
    next_delay_seconds,
    schedule_preview,
    total_retry_window,
)

HALF_AN_HOUR = 30 * 60
ONE_DAY = 24 * 3600


@pytest.fixture
def defaults() -> Settings:
    """Exactly the settings the service ships with.

    Built from the field defaults rather than by instantiating Settings(), because the
    test session sets INTAKE_* variables with compressed timings and those would
    otherwise leak in and make these assertions meaningless.
    """
    shipped = {
        name: field.default
        for name, field in Settings.model_fields.items()
        if field.default is not PydanticUndefined
    }
    return Settings.model_construct(**shipped)


def test_delays_grow_then_flatten_at_the_cap(defaults: Settings) -> None:
    assert schedule_preview(defaults, limit=11) == [
        5.0, 10.0, 20.0, 40.0, 80.0, 160.0, 320.0, 640.0, 1280.0, 2560.0, 3600.0
    ]  # fmt: skip


def test_survives_half_an_hour_of_downtime(defaults: Settings) -> None:
    """The brief calls this mandatory."""
    elapsed = 0.0
    attempts_within_window = 0
    for attempt in range(1, defaults.retry_max_attempts):
        elapsed += base_delay_seconds(attempt, defaults)
        if elapsed <= HALF_AN_HOUR:
            attempts_within_window += 1

    assert total_retry_window(defaults) > HALF_AN_HOUR
    # Not just "still retrying" - retrying often enough to catch the recovery quickly.
    assert attempts_within_window >= 6


def test_survives_a_day_of_downtime(defaults: Settings) -> None:
    """The brief calls this desirable; the shipped defaults do it."""
    assert total_retry_window(defaults) >= ONE_DAY


def test_recovers_from_a_short_blip_within_a_minute(defaults: Settings) -> None:
    """The common case: a redeploy. Four attempts inside the first 40 seconds."""
    elapsed = sum(base_delay_seconds(n, defaults) for n in range(1, 4))
    assert elapsed <= 40


def test_jitter_stays_within_the_configured_band(defaults: Settings) -> None:
    delays = [next_delay_seconds(6, defaults) for _ in range(200)]
    nominal = base_delay_seconds(6, defaults)
    assert min(delays) >= nominal * (1 - defaults.retry_jitter_ratio) - 1e-9
    assert max(delays) <= nominal * (1 + defaults.retry_jitter_ratio) + 1e-9
    # It must actually vary, otherwise a recovering recipient gets the whole backlog at once.
    assert len(set(delays)) > 100


def test_jitter_never_schedules_in_the_past(defaults: Settings) -> None:
    jumpy = defaults.model_copy(update={"retry_base_seconds": 0.01, "retry_jitter_ratio": 0.9})
    assert all(next_delay_seconds(1, jumpy) > 0 for _ in range(100))


def test_budget_is_exhausted_exactly_at_max_attempts(defaults: Settings) -> None:
    assert not is_exhausted(defaults.retry_max_attempts - 1, defaults)
    assert is_exhausted(defaults.retry_max_attempts, defaults)
    assert is_exhausted(defaults.retry_max_attempts + 1, defaults)


def test_attempt_counter_must_be_positive(defaults: Settings) -> None:
    with pytest.raises(ValueError):
        base_delay_seconds(0, defaults)


# ---------------------------------------------------------------------------
# Configuration invariants
# ---------------------------------------------------------------------------
def test_lease_shorter_than_an_attempt_is_rejected(defaults: Settings) -> None:
    """Otherwise a delivery could be claimed twice while the first attempt is running,
    and the recipient would get two concurrent copies of the same lead."""
    broken = defaults.model_copy(update={"lease_seconds": 10.0, "attempt_deadline_seconds": 30.0})
    with pytest.raises(ConfigurationError, match="lease_seconds"):
        broken.validate_invariants()


def test_timeouts_exceeding_the_attempt_deadline_are_rejected(defaults: Settings) -> None:
    broken = defaults.model_copy(
        update={
            "connect_timeout_seconds": 20.0,
            "read_timeout_seconds": 40.0,
            "attempt_deadline_seconds": 45.0,
            "lease_seconds": 90.0,
        }
    )
    with pytest.raises(ConfigurationError, match="attempt_deadline_seconds"):
        broken.validate_invariants()


def test_batch_larger_than_concurrency_is_rejected(defaults: Settings) -> None:
    broken = defaults.model_copy(update={"worker_batch_size": 64, "worker_concurrency": 8})
    with pytest.raises(ConfigurationError, match="worker_batch_size"):
        broken.validate_invariants()


def test_shipped_defaults_are_self_consistent(defaults: Settings) -> None:
    defaults.validate_invariants()
