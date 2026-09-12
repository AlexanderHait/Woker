"""The ten scenarios from section 4 of the brief, one test each.

They run end to end: the real API, real workers, a real recipient over a real socket,
and a real PostgreSQL. Where a scenario is about time (backoff growing, a lease
expiring, a timeout firing) the test waits for it rather than faking the clock - so
these are the slowest tests in the suite, and the ones actually worth trusting.

Scenario 7 goes further and starts the worker as a separate OS process so it can be
killed with SIGKILL for real.
"""

from __future__ import annotations

import asyncio
import os
import sys
import time

import asyncpg
import httpx
import pytest

from app.config import Settings
from tests.conftest import (
    StubControl,
    WorkerHarness,
    delivery_rows,
    lead,
    statuses,
    wait_for,
)

pytestmark = pytest.mark.scenario


async def post_lead(api: httpx.AsyncClient, **kwargs) -> dict:
    response = await api.post("/v1/requests", json=lead(**kwargs))
    assert response.status_code in (200, 201), response.text
    return response.json()


async def status_of(api: httpx.AsyncClient, request_id: str) -> dict:
    response = await api.get(f"/v1/requests/{request_id}")
    assert response.status_code == 200, response.text
    return response.json()


# ---------------------------------------------------------------------------
# 1. An ordinary lead, recipient working
# ---------------------------------------------------------------------------
async def test_scenario_01_ordinary_lead_is_delivered(
    api: httpx.AsyncClient,
    pool: asyncpg.Pool,
    stub: StubControl,
    workers: WorkerHarness,
    unique_name,
) -> None:
    name = unique_name("crm")
    await stub.configure(name, mode="ok")

    accepted = await post_lead(api, recipients=[{"url": stub.url(name), "name": "crm"}])
    workers.start()

    delivered = await wait_for(lambda: _all_delivered(pool, accepted["request_id"]), timeout=15)
    assert delivered, await statuses(pool, accepted["request_id"])

    # The recipient really got it, exactly once.
    received = await stub.received(name)
    assert len(received) == 1
    assert received[0]["body"]["payload"]["phone"] == "+7 900 000-00-00"
    assert received[0]["body"]["request_id"] == accepted["request_id"]

    # And the state is visible without going near the database.
    state = await status_of(api, accepted["request_id"])
    assert state["state"] == "delivered"
    recipient = state["recipients"][0]
    assert recipient["status"] == "delivered"
    assert recipient["recipient_name"] == "crm"
    assert recipient["attempts"] == 1
    assert recipient["delivered_at"] is not None
    assert len(recipient["attempt_log"]) == 1
    assert recipient["attempt_log"][0]["outcome"] == "success"
    assert recipient["attempt_log"][0]["status_code"] == 200


# ---------------------------------------------------------------------------
# 2. The same lead sent twice
# ---------------------------------------------------------------------------
async def test_scenario_02_a_repeat_delivers_one_copy(
    api: httpx.AsyncClient,
    pool: asyncpg.Pool,
    stub: StubControl,
    workers: WorkerHarness,
    unique_name,
) -> None:
    name = unique_name("crm")
    await stub.configure(name, mode="ok")
    body = lead(recipients=[{"url": stub.url(name)}])

    first = await api.post("/v1/requests", json=body)
    second = await api.post("/v1/requests", json=body)

    assert first.status_code == 201 and first.json()["duplicate"] is False
    # Same success shape as the original: the sender must not treat this as an error.
    assert second.status_code == 200 and second.json()["duplicate"] is True
    assert second.json()["request_id"] == first.json()["request_id"]

    workers.start()
    assert await wait_for(lambda: _all_delivered(pool, first.json()["request_id"]), timeout=15)

    # One lead stored, and one copy at the recipient - not two.
    assert await pool.fetchval("SELECT count(*) FROM requests") == 1
    summary = await stub.summary(name)
    assert summary["total"] == 1
    assert summary["duplicate_deliveries"] == 0


# ---------------------------------------------------------------------------
# 3. Three errors, then success
# ---------------------------------------------------------------------------
async def test_scenario_03_retries_until_it_works_and_logs_the_pauses(
    api: httpx.AsyncClient,
    pool: asyncpg.Pool,
    stub: StubControl,
    workers: WorkerHarness,
    settings: Settings,
    unique_name,
) -> None:
    name = unique_name("flaky")
    await stub.configure(name, mode="error", status_code=500, fail_first=3)

    # Visible, growing pauses: 0.5s, 1s, 2s.
    policy = settings.model_copy(
        update={"retry_base_seconds": 0.5, "retry_factor": 2.0, "retry_jitter_ratio": 0.0}
    )

    accepted = await post_lead(api, recipients=[{"url": stub.url(name)}])
    workers.start(settings=policy)

    assert await wait_for(lambda: _all_delivered(pool, accepted["request_id"]), timeout=30), (
        await statuses(pool, accepted["request_id"])
    )

    log = (await status_of(api, accepted["request_id"]))["recipients"][0]["attempt_log"]
    assert len(log) == 4, "all four attempts must be in the journal"
    assert [entry["outcome"] for entry in log] == ["failure", "failure", "failure", "success"]
    assert [entry["status_code"] for entry in log] == [500, 500, 500, 200]

    # The pauses are recorded, and they grow.
    planned = [
        _seconds_between(entry["finished_at"], entry["scheduled_next_at"]) for entry in log[:3]
    ]
    assert planned == pytest.approx([0.5, 1.0, 2.0], abs=0.2), planned
    assert planned[0] < planned[1] < planned[2]

    # And the wall clock agrees: the attempts really were spaced out.
    observed = [_seconds_between(log[i]["finished_at"], log[i + 1]["started_at"]) for i in range(3)]
    assert all(o >= p - 0.15 for o, p in zip(observed, planned, strict=True)), observed
    assert observed[2] > observed[0]

    assert (await stub.summary(name))["total"] == 4


# ---------------------------------------------------------------------------
# 4. A recipient that accepts the connection and says nothing
# ---------------------------------------------------------------------------
async def test_scenario_04_a_silent_recipient_times_out_without_hanging_the_service(
    api: httpx.AsyncClient,
    pool: asyncpg.Pool,
    stub: StubControl,
    workers: WorkerHarness,
    unique_name,
) -> None:
    name = unique_name("silent")
    await stub.configure(name, mode="silent")

    accepted = await post_lead(api, recipients=[{"url": stub.url(name)}])
    workers.start()

    # While the attempt is stuck on that socket, the service must stay responsive.
    assert await wait_for(lambda: _in_flight(pool, accepted["request_id"]), timeout=10)
    started = time.monotonic()
    probe = await api.get("/v1/problems")
    elapsed = time.monotonic() - started
    assert probe.status_code == 200
    assert elapsed < 1.0, f"the status endpoint took {elapsed:.2f}s while a delivery was stuck"

    # The read timeout fires and the delivery goes back into the queue, not into failure.
    assert await wait_for(
        lambda: _attempt_count(pool, accepted["request_id"], minimum=1), timeout=20
    )
    entry = (await status_of(api, accepted["request_id"]))["recipients"][0]["attempt_log"][0]
    assert entry["outcome"] == "failure"
    assert entry["error_kind"] in ("timeout", "deadline_exceeded")
    assert entry["status_code"] is None
    assert entry["duration_ms"] >= 1500, "it should have waited for the read timeout"

    row = (await delivery_rows(pool, accepted["request_id"]))[0]
    assert row["status"] in ("pending", "in_flight")


# ---------------------------------------------------------------------------
# 5. A recipient that stays down past the attempt budget
# ---------------------------------------------------------------------------
async def test_scenario_05_a_dead_recipient_ends_in_failed_and_in_the_problem_list(
    api: httpx.AsyncClient,
    pool: asyncpg.Pool,
    stub: StubControl,
    workers: WorkerHarness,
    settings: Settings,
    unique_name,
) -> None:
    name = unique_name("dead")
    await stub.configure(name, mode="error", status_code=503)

    policy = settings.model_copy(
        update={"retry_max_attempts": 3, "retry_base_seconds": 0.3, "retry_jitter_ratio": 0.0}
    )

    accepted = await post_lead(api, recipients=[{"url": stub.url(name), "name": "crm"}])
    workers.start(settings=policy)

    assert await wait_for(lambda: _all_failed(pool, accepted["request_id"]), timeout=30), (
        await statuses(pool, accepted["request_id"])
    )
    await workers.stop_all()

    state = await status_of(api, accepted["request_id"])
    assert state["state"] == "failed"
    recipient = state["recipients"][0]
    assert recipient["status"] == "failed"
    assert recipient["attempts"] == 3
    assert recipient["last_status_code"] == 503
    assert recipient["last_error_kind"] == "http_server_error"
    assert recipient["failed_at"] is not None

    # The lead itself is untouched - giving up on delivery never discards data.
    assert state["payload"]["phone"] == "+7 900 000-00-00"
    assert len(recipient["attempt_log"]) == 3

    problems = (await api.get("/v1/problems")).json()
    assert problems["counts"]["failed"] == 1
    entry = next(p for p in problems["items"] if p["reason"] == "failed")
    assert entry["request_id"] == accepted["request_id"]
    assert entry["recipient_name"] == "crm"
    assert entry["last_status_code"] == 503


# ---------------------------------------------------------------------------
# 6. Fix the recipient, then press retry
# ---------------------------------------------------------------------------
async def test_scenario_06_manual_retry_delivers_after_the_recipient_is_fixed(
    api: httpx.AsyncClient,
    pool: asyncpg.Pool,
    stub: StubControl,
    workers: WorkerHarness,
    settings: Settings,
    unique_name,
) -> None:
    name = unique_name("crm")
    await stub.configure(name, mode="error", status_code=503)
    policy = settings.model_copy(
        update={"retry_max_attempts": 2, "retry_base_seconds": 0.3, "retry_jitter_ratio": 0.0}
    )

    accepted = await post_lead(api, recipients=[{"url": stub.url(name), "name": "crm"}])
    workers.start(settings=policy)
    assert await wait_for(lambda: _all_failed(pool, accepted["request_id"]), timeout=30)
    await workers.stop_all()

    # The CRM is repaired.
    await stub.configure(name, mode="ok")

    response = await api.post(f"/v1/requests/{accepted['request_id']}/retry")
    assert response.status_code == 200
    assert response.json()["requeued"] == 1

    workers.start(settings=policy)
    assert await wait_for(lambda: _all_delivered(pool, accepted["request_id"]), timeout=30), (
        await statuses(pool, accepted["request_id"])
    )

    state = await status_of(api, accepted["request_id"])
    assert state["state"] == "delivered"
    # The budget was reset, but the journal still holds every attempt ever made.
    assert state["recipients"][0]["attempts"] == 1
    assert state["recipients"][0]["total_attempts"] == 3

    assert (await api.get("/v1/problems")).json()["total"] == 0


# ---------------------------------------------------------------------------
# 7. Killed mid-delivery, then restarted
# ---------------------------------------------------------------------------
@pytest.mark.slow
async def test_scenario_07_survives_sigkill_during_delivery(
    api: httpx.AsyncClient,
    pool: asyncpg.Pool,
    stub: StubControl,
    unique_name,
) -> None:
    """A genuine SIGKILL of a genuine worker process - no simulation.

    The worker is killed while an attempt is on the wire, so its rows stay `in_flight`
    with nobody to finish them. Recovery is entirely automatic: the lease expires and
    the next worker takes over.
    """
    name = unique_name("slow")
    await stub.configure(name, mode="slow", delay_seconds=30)

    accepted = await post_lead(api, recipients=[{"url": stub.url(name)}])

    env = {**os.environ, "INTAKE_LEASE_SECONDS": "4", "INTAKE_ATTEMPT_DEADLINE_SECONDS": "3"}
    victim = await asyncio.create_subprocess_exec(sys.executable, "-m", "app.run_worker", env=env)
    try:
        assert await wait_for(lambda: _in_flight(pool, accepted["request_id"]), timeout=20), (
            "the worker never picked the delivery up"
        )

        victim.kill()  # SIGKILL: no signal handler, no draining, no chance to tidy up
        await asyncio.wait_for(victim.wait(), timeout=10)
    finally:
        if victim.returncode is None:
            victim.kill()
            await victim.wait()

    # The row is stranded: claimed, nobody working on it, no result recorded.
    row = (await delivery_rows(pool, accepted["request_id"]))[0]
    assert row["status"] == "in_flight"
    assert row["locked_by"] is not None
    assert await pool.fetchval("SELECT count(*) FROM delivery_attempts") == 0

    # Nothing was lost, so the fixed recipient now gets it from the next worker.
    await stub.configure(name, mode="ok")
    survivor = await asyncio.create_subprocess_exec(sys.executable, "-m", "app.run_worker", env=env)
    try:
        assert await wait_for(lambda: _all_delivered(pool, accepted["request_id"]), timeout=45), (
            await statuses(pool, accepted["request_id"])
        )
    finally:
        survivor.terminate()
        await asyncio.wait_for(survivor.wait(), timeout=20)

    state = await status_of(api, accepted["request_id"])
    assert state["state"] == "delivered"

    # No runaway loop: the lost attempt burned budget like any other.
    log = state["recipients"][0]["attempt_log"]
    assert 2 <= len(log) <= 4, log
    assert log[0]["outcome"] == "unknown", "the interrupted attempt must be journalled honestly"
    assert log[0]["error_kind"] == "lease_expired"
    assert log[-1]["outcome"] == "success"


# ---------------------------------------------------------------------------
# 8. Three recipients, one of them slow
# ---------------------------------------------------------------------------
async def test_scenario_08_a_slow_recipient_does_not_hold_up_the_fast_ones(
    api: httpx.AsyncClient,
    pool: asyncpg.Pool,
    stub: StubControl,
    workers: WorkerHarness,
    settings: Settings,
    unique_name,
) -> None:
    slow_delay = 6.0
    fast_a, fast_b, slow = unique_name("fast-a"), unique_name("fast-b"), unique_name("slow")
    await stub.configure(fast_a, mode="ok")
    await stub.configure(fast_b, mode="ok")
    await stub.configure(slow, mode="slow", delay_seconds=slow_delay)

    # Room for the slow recipient to finish rather than time out: it is slow, not broken.
    policy = settings.model_copy(
        update={
            "connect_timeout_seconds": 2.0,
            "read_timeout_seconds": 15.0,
            "attempt_deadline_seconds": 20.0,
            "lease_seconds": 30.0,
        }
    )

    accepted = await post_lead(
        api,
        recipients=[
            {"url": stub.url(fast_a), "name": "fast-a"},
            {"url": stub.url(fast_b), "name": "fast-b"},
            {"url": stub.url(slow), "name": "slow"},
        ],
    )
    started = time.monotonic()
    workers.start(settings=policy)

    async def fast_ones_done() -> bool:
        rows = await delivery_rows(pool, accepted["request_id"])
        done = {r["recipient_url"] for r in rows if r["status"] == "delivered"}
        return stub.url(fast_a) in done and stub.url(fast_b) in done

    assert await wait_for(fast_ones_done, timeout=slow_delay - 1), (
        "the fast recipients waited for the slow one"
    )
    elapsed = time.monotonic() - started
    assert elapsed < slow_delay - 1, f"fast recipients took {elapsed:.1f}s"

    # The slow one is still going at that point, and finishes on its own schedule.
    state = await status_of(api, accepted["request_id"])
    assert state["state"] == "in_progress"
    assert await wait_for(lambda: _all_delivered(pool, accepted["request_id"]), timeout=30)
    assert (await status_of(api, accepted["request_id"]))["state"] == "delivered"


# ---------------------------------------------------------------------------
# 9. Five hundred leads in one go
# ---------------------------------------------------------------------------
@pytest.mark.slow
async def test_scenario_09_five_hundred_leads_at_once(
    api: httpx.AsyncClient,
    pool: asyncpg.Pool,
    stub: StubControl,
    workers: WorkerHarness,
    unique_name,
) -> None:
    count = 500
    name = unique_name("bulk")
    await stub.configure(name, mode="ok")

    bodies = [
        lead(idempotency_key=f"bulk-{i}", recipients=[{"url": stub.url(name)}])
        for i in range(count)
    ]

    started = time.monotonic()
    responses = await asyncio.gather(*(api.post("/v1/requests", json=b) for b in bodies))
    intake_seconds = time.monotonic() - started

    assert all(r.status_code == 201 for r in responses)
    assert await pool.fetchval("SELECT count(*) FROM requests") == count
    assert intake_seconds < 30, f"accepting {count} leads took {intake_seconds:.1f}s"

    workers.start(worker_id="bulk-1")
    workers.start(worker_id="bulk-2")

    # The status endpoint has to keep answering while all of this is in flight.
    probe_latencies: list[float] = []
    sample_id = responses[0].json()["request_id"]

    async def everything_delivered() -> bool:
        probe_started = time.monotonic()
        response = await api.get(f"/v1/requests/{sample_id}")
        probe_latencies.append(time.monotonic() - probe_started)
        assert response.status_code == 200
        outstanding = await pool.fetchval(
            "SELECT count(*) FROM deliveries WHERE status <> 'delivered'"
        )
        return outstanding == 0

    assert await wait_for(everything_delivered, timeout=120, interval=0.2), await pool.fetch(
        "SELECT status, count(*) FROM deliveries GROUP BY status"
    )

    assert len(probe_latencies) > 3, "the status endpoint was not actually sampled"
    assert max(probe_latencies) < 2.0, f"slowest status probe: {max(probe_latencies):.2f}s"

    # Every lead arrived, and none of them twice.
    summary = await stub.summary(name)
    assert summary["total"] == count
    assert summary["unique_idempotency_keys"] == count
    assert summary["duplicate_deliveries"] == 0

    stats = (await api.get("/v1/stats")).json()
    assert stats["requests_accepted"] == count
    assert stats["deliveries_delivered"] == count
    assert stats["deliveries_failed"] == 0


# ---------------------------------------------------------------------------
# 10. A lead with no recipients at all
# ---------------------------------------------------------------------------
async def test_scenario_10_a_lead_with_nowhere_to_go(
    api: httpx.AsyncClient,
    pool: asyncpg.Pool,
    stub: StubControl,
    workers: WorkerHarness,
    unique_name,
) -> None:
    accepted = await post_lead(api, recipients=[])
    assert accepted["has_recipients"] is False

    workers.start()
    await asyncio.sleep(0.5)  # a worker must find nothing to do, not spin or crash

    state = await status_of(api, accepted["request_id"])
    assert state["state"] == "no_recipients"
    assert state["recipients"] == []
    assert state["payload"]["name"] == "Иван"

    problems = (await api.get("/v1/problems")).json()
    assert problems["counts"]["no_recipients"] == 1
    entry = next(p for p in problems["items"] if p["reason"] == "no_recipients")
    assert entry["request_id"] == accepted["request_id"]
    assert entry["delivery_id"] is None

    stats = (await api.get("/v1/stats")).json()
    assert stats["requests_accepted"] == 1
    assert stats["requests_without_recipients"] == 1

    # And it can be resolved: give it an address and it goes out immediately.
    name = unique_name("late-crm")
    await stub.configure(name, mode="ok")
    attach = await api.post(
        f"/v1/requests/{accepted['request_id']}/recipients",
        json={"recipients": [{"url": stub.url(name), "name": "crm"}]},
    )
    assert attach.status_code == 200
    assert attach.json()["added"] == 1

    assert await wait_for(lambda: _all_delivered(pool, accepted["request_id"]), timeout=15)
    assert (await api.get("/v1/problems")).json()["total"] == 0


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------
async def _all_delivered(pool: asyncpg.Pool, request_id: str) -> bool:
    rows = await statuses(pool, request_id)
    return bool(rows) and all(s == "delivered" for s in rows)


async def _all_failed(pool: asyncpg.Pool, request_id: str) -> bool:
    rows = await statuses(pool, request_id)
    return bool(rows) and all(s == "failed" for s in rows)


async def _in_flight(pool: asyncpg.Pool, request_id: str) -> bool:
    return any(s == "in_flight" for s in await statuses(pool, request_id))


async def _attempt_count(pool: asyncpg.Pool, request_id: str, minimum: int) -> bool:
    count = await pool.fetchval(
        "SELECT count(*) FROM delivery_attempts WHERE request_id = $1::uuid", request_id
    )
    return count >= minimum


def _seconds_between(earlier_iso: str, later_iso: str) -> float:
    from datetime import datetime

    return (datetime.fromisoformat(later_iso) - datetime.fromisoformat(earlier_iso)).total_seconds()
