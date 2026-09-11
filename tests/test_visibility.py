"""The endpoints a human uses: the problem list, counters, and the repair buttons."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import asyncpg
import httpx

from tests.conftest import lead

BLACKHOLE = "http://127.0.0.1:1/hook"


async def make_lead(api: httpx.AsyncClient, **kwargs) -> str:
    response = await api.post("/v1/requests", json=lead(**kwargs))
    assert response.status_code == 201, response.text
    return response.json()["request_id"]


async def age_delivery(pool: asyncpg.Pool, request_id: str, minutes: int) -> None:
    """Backdate a queued delivery so it counts as stalled."""
    await pool.execute(
        "UPDATE deliveries SET created_at = now() - $2::interval WHERE request_id = $1::uuid",
        request_id,
        timedelta(minutes=minutes),
    )


# ---------------------------------------------------------------------------
# Problem list
# ---------------------------------------------------------------------------
async def test_problems_reports_all_three_kinds_of_trouble(
    api: httpx.AsyncClient, pool: asyncpg.Pool
) -> None:
    orphan = await make_lead(api, recipients=[])
    stuck = await make_lead(api, recipients=[{"url": BLACKHOLE}])
    dead = await make_lead(api, recipients=[{"url": BLACKHOLE}])
    healthy = await make_lead(api, recipients=[{"url": BLACKHOLE}])

    await age_delivery(pool, stuck, minutes=60)
    await pool.execute(
        "UPDATE deliveries SET status = 'failed', failed_at = now() WHERE request_id = $1::uuid",
        dead,
    )

    body = (await api.get("/v1/problems", params={"stale_minutes": 15})).json()

    assert body["counts"] == {"no_recipients": 1, "failed": 1, "stalled": 1}
    assert body["total"] == 3
    by_request = {item["request_id"]: item["reason"] for item in body["items"]}
    assert by_request[orphan] == "no_recipients"
    assert by_request[stuck] == "stalled"
    assert by_request[dead] == "failed"
    # Still inside its retry window and not yet late - not a problem yet.
    assert healthy not in by_request


async def test_a_young_delivery_is_not_yet_a_problem(
    api: httpx.AsyncClient, pool: asyncpg.Pool
) -> None:
    request_id = await make_lead(api, recipients=[{"url": BLACKHOLE}])
    await age_delivery(pool, request_id, minutes=5)

    late = (await api.get("/v1/problems", params={"stale_minutes": 15})).json()
    assert late["counts"]["stalled"] == 0

    # The threshold is the caller's to choose.
    strict = (await api.get("/v1/problems", params={"stale_minutes": 1})).json()
    assert strict["counts"]["stalled"] == 1


async def test_problems_are_listed_worst_first(api: httpx.AsyncClient, pool: asyncpg.Pool) -> None:
    """Oldest trouble first, because that is the order to work through them in."""
    recent = await make_lead(api, recipients=[{"url": BLACKHOLE}])
    ancient = await make_lead(api, recipients=[{"url": BLACKHOLE}])
    await age_delivery(pool, recent, minutes=20)
    await age_delivery(pool, ancient, minutes=600)

    items = (await api.get("/v1/problems", params={"stale_minutes": 15})).json()["items"]

    assert [item["request_id"] for item in items] == [ancient, recent]
    assert items[0]["age_seconds"] > items[1]["age_seconds"]


async def test_problems_can_be_filtered_and_paged(
    api: httpx.AsyncClient, pool: asyncpg.Pool
) -> None:
    for _ in range(5):
        await make_lead(api, recipients=[])
    failed = await make_lead(api, recipients=[{"url": BLACKHOLE}])
    await pool.execute(
        "UPDATE deliveries SET status = 'failed' WHERE request_id = $1::uuid", failed
    )

    filtered = (await api.get("/v1/problems", params={"reason": "no_recipients"})).json()
    assert filtered["total"] == 5
    assert {item["reason"] for item in filtered["items"]} == {"no_recipients"}
    # The counts stay global even when the list is filtered, so nothing hides.
    assert filtered["counts"]["failed"] == 1

    page = (await api.get("/v1/problems", params={"limit": 2, "offset": 2})).json()
    assert len(page["items"]) == 2
    assert page["total"] == 6

    # Past the end the list is empty, but the totals must still tell the truth -
    # otherwise paging through looks like the problems fixed themselves.
    beyond = (await api.get("/v1/problems", params={"offset": 500})).json()
    assert beyond["items"] == []
    assert beyond["total"] == 6
    assert beyond["counts"]["no_recipients"] == 5

    beyond_filtered = (
        await api.get("/v1/problems", params={"reason": "failed", "offset": 500})
    ).json()
    assert beyond_filtered["items"] == []
    assert beyond_filtered["total"] == 1


async def test_problems_is_empty_when_everything_is_fine(api: httpx.AsyncClient) -> None:
    body = (await api.get("/v1/problems")).json()
    assert body["total"] == 0
    assert body["items"] == []
    assert body["counts"] == {"no_recipients": 0, "failed": 0, "stalled": 0}


# ---------------------------------------------------------------------------
# Counters
# ---------------------------------------------------------------------------
async def test_stats_counts_each_state(api: httpx.AsyncClient, pool: asyncpg.Pool) -> None:
    await make_lead(api, recipients=[])
    delivered = await make_lead(api, recipients=[{"url": BLACKHOLE}])
    failed = await make_lead(api, recipients=[{"url": BLACKHOLE}])
    await make_lead(api, recipients=[{"url": BLACKHOLE}])

    await pool.execute(
        "UPDATE deliveries SET status = 'delivered' WHERE request_id = $1::uuid", delivered
    )
    await pool.execute(
        "UPDATE deliveries SET status = 'failed' WHERE request_id = $1::uuid", failed
    )

    body = (await api.get("/v1/stats")).json()

    assert body["requests_accepted"] == 4
    assert body["requests_without_recipients"] == 1
    assert body["deliveries_total"] == 3
    assert body["deliveries_delivered"] == 1
    assert body["deliveries_failed"] == 1
    assert body["deliveries_queued"] == 1


async def test_stats_respects_the_period(api: httpx.AsyncClient, pool: asyncpg.Pool) -> None:
    old = await make_lead(api, recipients=[])
    await pool.execute(
        "UPDATE requests SET received_at = now() - interval '3 days' WHERE id = $1::uuid", old
    )
    await make_lead(api, recipients=[])

    assert (await api.get("/v1/stats")).json()["requests_accepted"] == 1

    wide = await api.get(
        "/v1/stats",
        params={"from": (datetime.now(UTC) - timedelta(days=7)).isoformat()},
    )
    assert wide.json()["requests_accepted"] == 2


async def test_stats_rejects_a_backwards_period(api: httpx.AsyncClient) -> None:
    now = datetime.now(UTC)
    response = await api.get(
        "/v1/stats",
        params={"from": now.isoformat(), "to": (now - timedelta(hours=1)).isoformat()},
    )
    assert response.status_code == 422


# ---------------------------------------------------------------------------
# Repair actions
# ---------------------------------------------------------------------------
async def test_bulk_retry_flushes_one_recipient(api: httpx.AsyncClient, pool: asyncpg.Pool) -> None:
    crm = "http://crm.example.com/hook"
    chat = "http://chat.example.com/hook"
    for i in range(3):
        await make_lead(api, idempotency_key=f"crm-{i}", recipients=[{"url": crm}])
    await make_lead(api, idempotency_key="chat-1", recipients=[{"url": chat}])
    await pool.execute("UPDATE deliveries SET status = 'failed', failed_at = now()")

    response = await api.post(
        "/v1/deliveries/retry",
        json={"status": ["failed"], "recipient_origin": "http://crm.example.com:80"},
    )

    assert response.status_code == 200
    assert response.json()["requeued"] == 3
    assert await pool.fetchval("SELECT count(*) FROM deliveries WHERE status = 'pending'") == 3
    assert (
        await pool.fetchval("SELECT status FROM deliveries WHERE recipient_url = $1", chat)
        == "failed"
    )


async def test_bulk_retry_with_no_matches_is_not_an_error(api: httpx.AsyncClient) -> None:
    response = await api.post("/v1/deliveries/retry", json={"status": ["failed"]})
    assert response.status_code == 200
    assert response.json()["requeued"] == 0


async def test_attaching_recipients_twice_is_idempotent(
    api: httpx.AsyncClient, pool: asyncpg.Pool
) -> None:
    request_id = await make_lead(api, recipients=[])
    body = {"recipients": [{"url": BLACKHOLE, "name": "crm"}]}

    first = await api.post(f"/v1/requests/{request_id}/recipients", json=body)
    second = await api.post(f"/v1/requests/{request_id}/recipients", json=body)

    assert first.json()["added"] == 1
    assert second.json()["added"] == 0
    assert second.json()["already_present"] == 1
    assert await pool.fetchval("SELECT count(*) FROM deliveries") == 1


async def test_attaching_recipients_validates_the_address(api: httpx.AsyncClient) -> None:
    request_id = await make_lead(api, recipients=[])
    response = await api.post(
        f"/v1/requests/{request_id}/recipients", json={"recipients": [{"url": "nonsense"}]}
    )
    assert response.status_code == 422


async def test_unknown_ids_are_reported_as_missing(api: httpx.AsyncClient) -> None:
    missing = "00000000-0000-0000-0000-000000000000"
    assert (await api.get(f"/v1/requests/{missing}")).status_code == 404
    assert (await api.post(f"/v1/requests/{missing}/retry")).status_code == 404
    assert (
        await api.post(
            f"/v1/requests/{missing}/recipients", json={"recipients": [{"url": BLACKHOLE}]}
        )
    ).status_code == 404


async def test_a_malformed_id_is_rejected_not_crashed(api: httpx.AsyncClient) -> None:
    assert (await api.get("/v1/requests/not-a-uuid")).status_code == 422


# ---------------------------------------------------------------------------
# Operational endpoints
# ---------------------------------------------------------------------------
async def test_healthz_reports_the_policy_actually_in_force(api: httpx.AsyncClient) -> None:
    """So a reviewer can see the retry schedule without reading the source or the env."""
    body = (await api.get("/healthz")).json()

    assert body["status"] == "ok"
    assert body["retry_policy"]["max_attempts"] > 0
    assert body["retry_policy"]["first_delays_seconds"][0] > 0
    assert body["timeouts"]["lease_seconds"] > body["timeouts"]["attempt_deadline_seconds"]


async def test_readyz_checks_the_database(api: httpx.AsyncClient) -> None:
    assert (await api.get("/readyz")).json() == {"status": "ready"}
