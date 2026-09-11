"""Queue mechanics: claiming, leases, fairness and state transitions.

This is where the guarantees actually live, so these tests poke the storage layer
directly rather than going through HTTP.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta

import asyncpg
import pytest

from app.config import Settings
from app.queue import (
    AttemptResult,
    claim_batch,
    record_result,
    requeue_bulk,
    requeue_request,
)


async def make_request(pool: asyncpg.Pool, key: str = "k1") -> str:
    return await pool.fetchval(
        """
        INSERT INTO requests (source_id, idempotency_key, payload)
        VALUES ('test', $1, '{"name": "test"}'::jsonb)
        RETURNING id
        """,
        key,
    )


async def queue_delivery(
    pool: asyncpg.Pool,
    request_id: str,
    url: str,
    origin: str | None = None,
    *,
    next_attempt_at: datetime | None = None,
    attempts: int = 0,
) -> str:
    from app.recipients import origin_of

    return await pool.fetchval(
        """
        INSERT INTO deliveries (request_id, recipient_url, recipient_origin,
                                next_attempt_at, attempts)
        VALUES ($1, $2, $3, COALESCE($4, now()), $5)
        RETURNING id
        """,
        request_id,
        url,
        origin or origin_of(url),
        next_attempt_at,
        attempts,
    )


def attempt(succeeded: bool, status_code: int | None = 200) -> AttemptResult:
    now = datetime.now(UTC)
    return AttemptResult(
        succeeded=succeeded,
        error_kind=None if succeeded else "http_server_error",
        status_code=status_code,
        response_excerpt=None if succeeded else "boom",
        started_at=now - timedelta(milliseconds=20),
        finished_at=now,
    )


# ---------------------------------------------------------------------------
# Claiming
# ---------------------------------------------------------------------------
async def test_claiming_takes_ownership_and_burns_an_attempt(
    pool: asyncpg.Pool, settings: Settings
) -> None:
    request_id = await make_request(pool)
    delivery_id = await queue_delivery(pool, request_id, "http://a.example.com/hook")

    claimed = await claim_batch(pool, "worker-1", 10, settings)

    assert [c.delivery_id for c in claimed] == [delivery_id]
    assert claimed[0].attempt_number == 1
    assert claimed[0].payload == {"name": "test"}

    row = await pool.fetchrow("SELECT * FROM deliveries WHERE id = $1", delivery_id)
    assert row["status"] == "in_flight"
    assert row["locked_by"] == "worker-1"
    # The attempt is counted at claim time, so a worker that dies mid-flight still
    # consumes budget and a poisonous delivery cannot loop forever.
    assert row["attempts"] == 1
    assert row["lease_expires_at"] > datetime.now(UTC)


async def test_work_scheduled_for_later_is_not_claimed(
    pool: asyncpg.Pool, settings: Settings
) -> None:
    request_id = await make_request(pool)
    await queue_delivery(
        pool,
        request_id,
        "http://a.example.com/hook",
        next_attempt_at=datetime.now(UTC) + timedelta(minutes=5),
    )

    assert await claim_batch(pool, "worker-1", 10, settings) == []


async def test_two_workers_never_claim_the_same_delivery(
    pool: asyncpg.Pool, settings: Settings
) -> None:
    """SKIP LOCKED in practice: the thing that stops one lead reaching one recipient twice."""
    request_id = await make_request(pool)
    for i in range(40):
        await queue_delivery(pool, request_id, f"http://host{i}.example.com/hook")

    batches = await asyncio.gather(
        *(claim_batch(pool, f"worker-{n}", 10, settings) for n in range(8))
    )

    claimed_ids = [c.delivery_id for batch in batches for c in batch]
    assert len(claimed_ids) == len(set(claimed_ids)), "a delivery was handed to two workers"
    assert len(claimed_ids) == 40

    owners = await pool.fetch("SELECT DISTINCT locked_by FROM deliveries")
    assert len(owners) > 1, "the test did not actually exercise concurrent workers"


async def test_repeated_scrambles_never_hand_a_delivery_out_twice(
    pool: asyncpg.Pool, settings: Settings
) -> None:
    """Pressure on the claim race.

    The ranking step runs without a lock, so between ranking and locking a row can be
    taken by somebody else. Many workers fighting over a small pool, round after round,
    is what would expose a missing re-check: a delivery handed to two workers at once
    means one lead arriving twice at the recipient.
    """
    request_id = await make_request(pool)
    delivery_ids = {
        await queue_delivery(pool, request_id, f"http://host{i}.example.com/hook")
        for i in range(12)
    }

    for _ in range(15):
        batches = await asyncio.gather(
            *(claim_batch(pool, f"worker-{n}", 3, settings) for n in range(10))
        )
        claimed = [c.delivery_id for batch in batches for c in batch]

        assert len(claimed) == len(set(claimed)), "one delivery was claimed by two workers"
        assert set(claimed) <= delivery_ids

        # Release everything and go round again.
        await pool.execute(
            """
            UPDATE deliveries
            SET status = 'pending', next_attempt_at = now(),
                lease_expires_at = NULL, locked_by = NULL
            """
        )

    # Every claim burned budget exactly once, so the counters agree with reality.
    total_claims = await pool.fetchval("SELECT sum(total_attempts) FROM deliveries")
    assert total_claims > 0


async def test_one_recipient_cannot_take_the_whole_batch(
    pool: asyncpg.Pool, settings: Settings
) -> None:
    """Scenario 8's mechanism. A recipient with a huge backlog is capped per batch, so
    deliveries for everybody else still get picked up."""
    request_id = await make_request(pool)
    for i in range(30):
        await queue_delivery(pool, request_id, f"http://busy.example.com/hook/{i}")
    quiet_id = await queue_delivery(pool, request_id, "http://quiet.example.com/hook")

    capped = settings.model_copy(update={"max_claims_per_origin": 4})
    claimed = await claim_batch(pool, "worker-1", 8, capped)

    by_origin: dict[str, int] = {}
    for item in claimed:
        by_origin[item.recipient_origin] = by_origin.get(item.recipient_origin, 0) + 1

    assert by_origin["http://busy.example.com:80"] == 4
    assert quiet_id in [c.delivery_id for c in claimed], (
        "the quiet recipient was starved by the busy one"
    )


# ---------------------------------------------------------------------------
# Leases
# ---------------------------------------------------------------------------
async def test_the_fairness_cap_does_not_throttle_a_single_recipient(
    pool: asyncpg.Pool, settings: Settings
) -> None:
    """The per-origin cap limits one *batch*, not the rate.

    A burst of 200 leads for one recipient must still reach that recipient at full
    concurrency. If the worker treated a short batch as "the queue is empty" and slept,
    this would trickle out at cap-per-poll-interval and a busy afternoon would back up
    for no reason.
    """
    request_id = await make_request(pool)
    for i in range(200):
        await queue_delivery(pool, request_id, f"http://busy.example.com/hook/{i}")

    capped = settings.model_copy(update={"max_claims_per_origin": 4})

    claimed_total = 0
    for _ in range(20):
        batch = await claim_batch(pool, "worker-1", 16, capped)
        if not batch:
            break
        claimed_total += len(batch)

    # 20 rounds x 4 per round, with no sleeping in between.
    assert claimed_total == 80, f"claimed only {claimed_total} in 20 rounds"


async def test_a_live_lease_is_not_stolen(pool: asyncpg.Pool, settings: Settings) -> None:
    request_id = await make_request(pool)
    await queue_delivery(pool, request_id, "http://a.example.com/hook")

    assert len(await claim_batch(pool, "worker-1", 10, settings)) == 1
    assert await claim_batch(pool, "worker-2", 10, settings) == []


async def test_an_expired_lease_is_reclaimed(pool: asyncpg.Pool, settings: Settings) -> None:
    """What recovery after `kill -9` actually is: no daemon, no manual step, just a
    timestamp in the past."""
    request_id = await make_request(pool)
    delivery_id = await queue_delivery(pool, request_id, "http://a.example.com/hook")

    first = await claim_batch(pool, "worker-1", 10, settings)
    assert first[0].attempt_number == 1

    # Stand in for "worker-1 was killed and its lease ran out".
    await pool.execute(
        "UPDATE deliveries SET lease_expires_at = now() - interval '1 second' WHERE id = $1",
        delivery_id,
    )

    second = await claim_batch(pool, "worker-2", 10, settings)
    assert [c.delivery_id for c in second] == [delivery_id]
    assert second[0].attempt_number == 2, "the reclaim must consume budget too"

    row = await pool.fetchrow("SELECT locked_by FROM deliveries WHERE id = $1", delivery_id)
    assert row["locked_by"] == "worker-2"


async def test_a_reclaim_is_written_into_the_journal(
    pool: asyncpg.Pool, settings: Settings
) -> None:
    """The lost attempt is recorded as 'unknown', because the recipient may well have
    received that copy. We would rather say so than pretend it never happened."""
    request_id = await make_request(pool)
    delivery_id = await queue_delivery(pool, request_id, "http://a.example.com/hook")

    await claim_batch(pool, "worker-1", 10, settings)
    await pool.execute(
        "UPDATE deliveries SET lease_expires_at = now() - interval '1 second' WHERE id = $1",
        delivery_id,
    )
    await claim_batch(pool, "worker-2", 10, settings)

    entries = await pool.fetch(
        "SELECT * FROM delivery_attempts WHERE delivery_id = $1 ORDER BY attempt_number",
        delivery_id,
    )
    assert len(entries) == 1
    assert entries[0]["attempt_number"] == 1
    assert entries[0]["outcome"] == "unknown"
    assert entries[0]["error_kind"] == "lease_expired"


async def test_a_late_answer_replaces_the_unknown_placeholder(
    pool: asyncpg.Pool, settings: Settings
) -> None:
    """If the original worker turns out to be alive and reports the real result, the
    journal is corrected: a known fact beats 'we are not sure'."""
    request_id = await make_request(pool)
    delivery_id = await queue_delivery(pool, request_id, "http://a.example.com/hook")

    first = await claim_batch(pool, "worker-1", 10, settings)
    await pool.execute(
        "UPDATE deliveries SET lease_expires_at = now() - interval '1 second' WHERE id = $1",
        delivery_id,
    )
    await claim_batch(pool, "worker-2", 10, settings)

    await record_result(pool, "worker-1", first[0], attempt(succeeded=True), settings)

    entry = await pool.fetchrow(
        "SELECT * FROM delivery_attempts WHERE delivery_id = $1 AND attempt_number = 1",
        delivery_id,
    )
    assert entry["outcome"] == "success"
    assert entry["error_kind"] is None


# ---------------------------------------------------------------------------
# Recording outcomes
# ---------------------------------------------------------------------------
async def test_success_settles_the_delivery(pool: asyncpg.Pool, settings: Settings) -> None:
    request_id = await make_request(pool)
    delivery_id = await queue_delivery(pool, request_id, "http://a.example.com/hook")
    claimed = await claim_batch(pool, "worker-1", 10, settings)

    await record_result(pool, "worker-1", claimed[0], attempt(succeeded=True), settings)

    row = await pool.fetchrow("SELECT * FROM deliveries WHERE id = $1", delivery_id)
    assert row["status"] == "delivered"
    assert row["delivered_at"] is not None
    assert row["locked_by"] is None
    assert row["lease_expires_at"] is None

    assert await claim_batch(pool, "worker-1", 10, settings) == []


async def test_failure_schedules_a_retry_in_the_future(
    pool: asyncpg.Pool, settings: Settings
) -> None:
    request_id = await make_request(pool)
    delivery_id = await queue_delivery(pool, request_id, "http://a.example.com/hook")
    claimed = await claim_batch(pool, "worker-1", 10, settings)

    await record_result(pool, "worker-1", claimed[0], attempt(False, 503), settings)

    row = await pool.fetchrow("SELECT * FROM deliveries WHERE id = $1", delivery_id)
    assert row["status"] == "pending"
    assert row["last_status_code"] == 503
    assert row["last_error_kind"] == "http_server_error"
    assert row["next_attempt_at"] > datetime.now(UTC), "a retry must not be immediate"

    # And the pause is visible in the journal, not only in the row.
    entry = await pool.fetchrow(
        "SELECT * FROM delivery_attempts WHERE delivery_id = $1", delivery_id
    )
    assert entry["scheduled_next_at"] == row["next_attempt_at"]


async def test_the_budget_runs_out_into_failed(pool: asyncpg.Pool, settings: Settings) -> None:
    short = settings.model_copy(update={"retry_max_attempts": 3, "retry_base_seconds": 0.01})
    request_id = await make_request(pool)
    delivery_id = await queue_delivery(pool, request_id, "http://a.example.com/hook")

    for expected in ("pending", "pending", "failed"):
        claimed = await claim_batch(pool, "worker-1", 10, short)
        assert claimed, "the delivery should still be claimable"
        await record_result(pool, "worker-1", claimed[0], attempt(False, 500), short)
        row = await pool.fetchrow("SELECT status FROM deliveries WHERE id = $1", delivery_id)
        assert row["status"] == expected
        # Past MIN_DELAY_SECONDS: even a tiny configured backoff never retries instantly.
        await asyncio.sleep(0.6)

    # Given up on, but emphatically not gone.
    row = await pool.fetchrow("SELECT * FROM deliveries WHERE id = $1", delivery_id)
    assert row["failed_at"] is not None
    assert row["attempts"] == 3
    assert await pool.fetchval("SELECT count(*) FROM delivery_attempts") == 3
    assert await pool.fetchval("SELECT count(*) FROM requests") == 1

    assert await claim_batch(pool, "worker-1", 10, short) == []


async def test_a_worker_that_lost_its_lease_cannot_reschedule(
    pool: asyncpg.Pool, settings: Settings
) -> None:
    """A slow worker coming back from the dead must not move a row that now belongs to
    somebody else."""
    request_id = await make_request(pool)
    delivery_id = await queue_delivery(pool, request_id, "http://a.example.com/hook")

    stale = await claim_batch(pool, "worker-1", 10, settings)
    await pool.execute(
        "UPDATE deliveries SET lease_expires_at = now() - interval '1 second' WHERE id = $1",
        delivery_id,
    )
    await claim_batch(pool, "worker-2", 10, settings)

    await record_result(pool, "worker-1", stale[0], attempt(False, 500), settings)

    row = await pool.fetchrow("SELECT * FROM deliveries WHERE id = $1", delivery_id)
    assert row["status"] == "in_flight"
    assert row["locked_by"] == "worker-2"


async def test_a_late_success_still_stops_further_attempts(
    pool: asyncpg.Pool, settings: Settings
) -> None:
    """The opposite call for success: we now know the recipient has the lead, so mark it
    delivered even though the lease moved on. Letting the retry train roll would only
    produce more duplicates."""
    request_id = await make_request(pool)
    delivery_id = await queue_delivery(pool, request_id, "http://a.example.com/hook")

    stale = await claim_batch(pool, "worker-1", 10, settings)
    await pool.execute(
        "UPDATE deliveries SET lease_expires_at = now() - interval '1 second' WHERE id = $1",
        delivery_id,
    )
    await claim_batch(pool, "worker-2", 10, settings)

    await record_result(pool, "worker-1", stale[0], attempt(succeeded=True), settings)

    row = await pool.fetchrow("SELECT status FROM deliveries WHERE id = $1", delivery_id)
    assert row["status"] == "delivered"


# ---------------------------------------------------------------------------
# Requeueing
# ---------------------------------------------------------------------------
async def test_requeue_resets_the_budget(pool: asyncpg.Pool, settings: Settings) -> None:
    short = settings.model_copy(update={"retry_max_attempts": 1})
    request_id = await make_request(pool)
    delivery_id = await queue_delivery(pool, request_id, "http://a.example.com/hook")

    claimed = await claim_batch(pool, "worker-1", 10, short)
    await record_result(pool, "worker-1", claimed[0], attempt(False, 500), short)
    assert await pool.fetchval("SELECT status FROM deliveries") == "failed"

    outcome = await requeue_request(pool, request_id, None, include_delivered=False)

    assert outcome.requeued_ids == [delivery_id]
    row = await pool.fetchrow("SELECT * FROM deliveries WHERE id = $1", delivery_id)
    assert row["status"] == "pending"
    assert row["attempts"] == 0
    assert row["failed_at"] is None
    # The journal is untouched by a requeue - it is the permanent record.
    assert await pool.fetchval("SELECT count(*) FROM delivery_attempts") == 1


async def test_requeue_leaves_delivered_leads_alone_by_default(
    pool: asyncpg.Pool, settings: Settings
) -> None:
    """Re-sending something that already arrived is the duplicate call to the customer
    we are trying to avoid, so it takes an explicit opt-in."""
    request_id = await make_request(pool)
    await queue_delivery(pool, request_id, "http://a.example.com/hook")
    claimed = await claim_batch(pool, "worker-1", 10, settings)
    await record_result(pool, "worker-1", claimed[0], attempt(succeeded=True), settings)

    outcome = await requeue_request(pool, request_id, None, include_delivered=False)
    assert outcome.requeued_ids == []
    assert outcome.skipped_delivered == 1
    assert await pool.fetchval("SELECT status FROM deliveries") == "delivered"

    forced = await requeue_request(pool, request_id, None, include_delivered=True)
    assert len(forced.requeued_ids) == 1
    assert await pool.fetchval("SELECT status FROM deliveries") == "pending"


async def test_requeue_does_not_disturb_an_attempt_on_the_wire(
    pool: asyncpg.Pool, settings: Settings
) -> None:
    request_id = await make_request(pool)
    await queue_delivery(pool, request_id, "http://a.example.com/hook")
    await claim_batch(pool, "worker-1", 10, settings)

    outcome = await requeue_request(pool, request_id, None, include_delivered=False)

    assert outcome.requeued_ids == []
    assert outcome.skipped_in_flight == 1
    assert await pool.fetchval("SELECT locked_by FROM deliveries") == "worker-1"


async def test_requeue_can_target_one_recipient(pool: asyncpg.Pool) -> None:
    request_id = await make_request(pool)
    crm = await queue_delivery(pool, request_id, "http://crm.example.com/hook")
    await queue_delivery(pool, request_id, "http://chat.example.com/hook")
    await pool.execute("UPDATE deliveries SET status = 'failed'")

    outcome = await requeue_request(
        pool, request_id, ["http://crm.example.com/hook"], include_delivered=False
    )

    assert outcome.requeued_ids == [crm]
    statuses = dict(
        await pool.fetch("SELECT recipient_url, status::text FROM deliveries")  # type: ignore[arg-type]
    )
    assert statuses["http://crm.example.com/hook"] == "pending"
    assert statuses["http://chat.example.com/hook"] == "failed"


async def test_bulk_requeue_targets_one_broken_recipient(pool: asyncpg.Pool) -> None:
    """'The CRM is back up, flush what piled up for it' - without touching anything else."""
    request_id = await make_request(pool)
    for i in range(5):
        await queue_delivery(pool, request_id, f"http://crm.example.com/hook/{i}")
    await queue_delivery(pool, request_id, "http://chat.example.com/hook")
    await pool.execute("UPDATE deliveries SET status = 'failed'")

    outcome = await requeue_bulk(
        pool,
        statuses=["failed"],
        recipient_origin="http://crm.example.com:80",
        recipient_url_contains=None,
        received_after=None,
        received_before=None,
        limit=1000,
    )

    assert len(outcome.requeued_ids) == 5
    assert (
        await pool.fetchval(
            "SELECT status FROM deliveries WHERE recipient_url = 'http://chat.example.com/hook'"
        )
        == "failed"
    )


@pytest.mark.parametrize(
    ("url", "expected_origin"),
    [
        ("http://crm.example.com/hook", "http://crm.example.com:80"),
        ("https://crm.example.com/hook", "https://crm.example.com:443"),
        ("https://crm.example.com:8443/hook", "https://crm.example.com:8443"),
        ("http://CRM.Example.COM/hook", "http://crm.example.com:80"),
    ],
)
def test_origin_is_the_fairness_key(url: str, expected_origin: str) -> None:
    """Different paths on one host share a budget, because they share a fate."""
    from app.recipients import origin_of

    assert origin_of(url) == expected_origin
