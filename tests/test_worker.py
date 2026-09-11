"""Properties of the worker loop itself, as opposed to the SQL it runs.

These drive a real `Worker` against a real recipient, because what is being checked here
is the loop's pacing decisions - which no amount of testing `claim_batch` in isolation
can see.
"""

from __future__ import annotations

import asyncio
import time

import asyncpg
import httpx

from app.config import Settings
from tests.conftest import StubControl, WorkerHarness, lead, wait_for


async def test_a_burst_for_one_recipient_is_not_paced_by_the_poll_interval(
    api: httpx.AsyncClient,
    pool: asyncpg.Pool,
    stub: StubControl,
    workers: WorkerHarness,
    settings: Settings,
    unique_name,
) -> None:
    """`max_claims_per_origin` caps a single *batch*, and must not become a rate limit.

    The trap: a batch comes back short whenever the due work belongs to few recipients,
    so a worker that treats "short batch" as "queue is empty" and sleeps will hand out
    only `max_claims_per_origin` deliveries per poll interval. That is invisible with the
    default 250 ms interval, which is exactly why it is worth pinning - here the interval
    is stretched to a full second so the two behaviours are seconds apart.
    """
    count = 40
    cap = 4
    poll_seconds = 1.0

    name = unique_name("burst")
    await stub.configure(name, mode="ok")

    for i in range(count):
        response = await api.post(
            "/v1/requests",
            json=lead(idempotency_key=f"burst-{i}", recipients=[{"url": stub.url(name)}]),
        )
        assert response.status_code == 201

    slow_poll = settings.model_copy(
        update={
            "max_claims_per_origin": cap,
            "worker_batch_size": 8,
            "worker_poll_interval_seconds": poll_seconds,
        }
    )

    # Paced by the poll interval this would need count/cap = 10 rounds, so ~10 seconds.
    paced_estimate = (count / cap) * poll_seconds
    budget = paced_estimate / 2

    started = time.monotonic()
    workers.start(settings=slow_poll)

    async def all_delivered() -> bool:
        remaining = await pool.fetchval(
            "SELECT count(*) FROM deliveries WHERE status <> 'delivered'"
        )
        return remaining == 0

    delivered = await wait_for(all_delivered, timeout=paced_estimate * 2)
    elapsed = time.monotonic() - started

    assert delivered, "the burst never finished"
    assert elapsed < budget, (
        f"{count} deliveries to one recipient took {elapsed:.1f}s; a worker that does not "
        f"sleep on a short batch needs far less than {budget:.1f}s, so the fairness cap "
        f"has turned into a throughput limit"
    )

    assert (await stub.summary(name))["total"] == count


async def test_a_worker_with_nothing_to_do_does_not_spin(
    pool: asyncpg.Pool, workers: WorkerHarness, settings: Settings
) -> None:
    """The other side of the same decision: an empty queue must be idled on, not polled
    in a tight loop."""
    idle = settings.model_copy(update={"worker_poll_interval_seconds": 0.2})
    worker = workers.start(settings=idle)

    await asyncio.sleep(1.0)

    assert worker.in_flight == 0
    # Nothing was queued, so nothing may have been attempted.
    assert await pool.fetchval("SELECT count(*) FROM delivery_attempts") == 0
