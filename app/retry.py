"""Retry schedule.

Exponential backoff with a ceiling and proportional jitter.

Why these defaults (base 5s, factor 2, cap 1h, 34 attempts):

  * The first attempts are seconds apart, because most failures are a blip - a
    redeploy, a dropped connection - and recover almost immediately.
  * The gap doubles up to one hour. Attempts 1-10 all land inside the first 85
    minutes, which comfortably covers the "must survive half an hour" requirement.
  * After that the service keeps knocking once an hour. 34 attempts span ~24.4
    hours in total, covering the "a day would be nice" case, without hammering a
    recipient that has been down all night.
  * Jitter spreads the herd: when a CRM comes back up, the thousand leads queued
    for it do not arrive in the same millisecond.

Every parameter is configurable; see app/config.py.
"""

from __future__ import annotations

import random

from app.config import Settings

# Never schedule a retry closer than this, no matter what jitter produces.
MIN_DELAY_SECONDS = 0.5


def base_delay_seconds(attempts_made: int, settings: Settings) -> float:
    """Un-jittered delay before the next attempt, given how many already happened.

    `attempts_made` is 1 after the first attempt failed.
    """
    if attempts_made < 1:
        raise ValueError("attempts_made must be >= 1")
    exponent = attempts_made - 1
    raw = settings.retry_base_seconds * (settings.retry_factor**exponent)
    return min(raw, settings.retry_cap_seconds)


def next_delay_seconds(attempts_made: int, settings: Settings) -> float:
    """Delay before the next attempt, with jitter applied."""
    delay = base_delay_seconds(attempts_made, settings)
    if settings.retry_jitter_ratio:
        spread = delay * settings.retry_jitter_ratio
        delay += random.uniform(-spread, spread)
    return max(MIN_DELAY_SECONDS, delay)


def is_exhausted(attempts_made: int, settings: Settings) -> bool:
    """True when the attempt budget is spent and the pair must go to 'failed'."""
    return attempts_made >= settings.retry_max_attempts


def total_retry_window(settings: Settings) -> float:
    """Total un-jittered time from the first attempt to the last one."""
    return sum(base_delay_seconds(n, settings) for n in range(1, settings.retry_max_attempts))


def schedule_preview(settings: Settings, limit: int = 12) -> list[float]:
    """First few un-jittered delays - handy in /healthz and when explaining the policy."""
    upper = min(limit, max(settings.retry_max_attempts - 1, 0))
    return [round(base_delay_seconds(n, settings), 3) for n in range(1, upper + 1)]
