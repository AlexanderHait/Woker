"""Accepting leads: validation, durability, and deduplication on the way in."""

from __future__ import annotations

import asyncio
import time

import asyncpg
import httpx
import pytest

from tests.conftest import lead

# Nothing listens here. Used to prove intake does not depend on recipients being alive.
BLACKHOLE = "http://127.0.0.1:1/hook"


# ---------------------------------------------------------------------------
# The happy path and the promise attached to it
# ---------------------------------------------------------------------------
async def test_accepts_a_lead_and_returns_an_id(api: httpx.AsyncClient) -> None:
    response = await api.post("/v1/requests", json=lead(recipients=[{"url": BLACKHOLE}]))

    assert response.status_code == 201
    body = response.json()
    assert body["duplicate"] is False
    assert body["recipient_count"] == 1
    assert body["has_recipients"] is True
    assert body["request_id"]


async def test_accepted_means_on_disk(api: httpx.AsyncClient, pool: asyncpg.Pool) -> None:
    """The core promise: once we answer "accepted", losing power must not lose the lead."""
    response = await api.post("/v1/requests", json=lead(recipients=[{"url": BLACKHOLE}]))
    request_id = response.json()["request_id"]

    # Read through a connection that had nothing to do with the request that wrote it.
    stored = await pool.fetchrow("SELECT * FROM requests WHERE id = $1::uuid", request_id)
    assert stored is not None
    assert stored["payload"]["phone"] == "+7 900 000-00-00"

    queued = await pool.fetch("SELECT * FROM deliveries WHERE request_id = $1::uuid", request_id)
    assert len(queued) == 1
    assert queued[0]["status"] == "pending"
    assert queued[0]["attempts"] == 0


async def test_answers_immediately_even_when_every_recipient_is_down(
    api: httpx.AsyncClient,
) -> None:
    """Intake performs no network I/O, so unreachable recipients cost nothing here."""
    body = lead(recipients=[{"url": f"http://127.0.0.1:{port}/hook"} for port in (1, 2, 3, 4, 5)])

    started = time.monotonic()
    response = await api.post("/v1/requests", json=body)
    elapsed = time.monotonic() - started

    assert response.status_code == 201
    assert response.json()["recipient_count"] == 5
    # Generously above what this costs in practice, but far below any connect timeout.
    assert elapsed < 1.0, f"intake took {elapsed:.2f}s; it must not wait on recipients"


# ---------------------------------------------------------------------------
# Deduplication
# ---------------------------------------------------------------------------
async def test_repeat_of_a_known_key_returns_the_original(
    api: httpx.AsyncClient, pool: asyncpg.Pool
) -> None:
    body = lead(recipients=[{"url": BLACKHOLE}])

    first = await api.post("/v1/requests", json=body)
    second = await api.post("/v1/requests", json=body)

    assert first.status_code == 201
    assert first.json()["duplicate"] is False

    # A repeat is a success, not an error: the sender did nothing wrong.
    assert second.status_code == 200
    assert second.json()["duplicate"] is True
    assert second.json()["request_id"] == first.json()["request_id"]

    assert await pool.fetchval("SELECT count(*) FROM requests") == 1
    assert await pool.fetchval("SELECT count(*) FROM deliveries") == 1


async def test_the_same_key_from_a_different_source_is_a_different_lead(
    api: httpx.AsyncClient, pool: asyncpg.Pool
) -> None:
    """Keys are scoped per source, so two partners can both number their leads from 1."""
    first = await api.post("/v1/requests", json=lead(source_id="partner-a", idempotency_key="1"))
    second = await api.post("/v1/requests", json=lead(source_id="partner-b", idempotency_key="1"))

    assert first.status_code == 201
    assert second.status_code == 201
    assert first.json()["request_id"] != second.json()["request_id"]
    assert await pool.fetchval("SELECT count(*) FROM requests") == 2


async def test_a_repeat_does_not_overwrite_the_stored_lead(
    api: httpx.AsyncClient, pool: asyncpg.Pool
) -> None:
    """First write wins. A repeat is a repeat, even if its body drifted."""
    key = "stable-key"
    await api.post("/v1/requests", json=lead(idempotency_key=key, payload={"name": "original"}))
    response = await api.post(
        "/v1/requests", json=lead(idempotency_key=key, payload={"name": "changed"})
    )

    assert response.json()["duplicate"] is True
    stored = await pool.fetchval("SELECT payload FROM requests")
    assert stored == {"name": "original"}


async def test_simultaneous_repeats_still_create_exactly_one_lead(
    api: httpx.AsyncClient, pool: asyncpg.Pool
) -> None:
    """The double-click and the sender's own retry, racing each other."""
    body = lead(recipients=[{"url": BLACKHOLE}])

    responses = await asyncio.gather(*(api.post("/v1/requests", json=body) for _ in range(20)))

    assert all(r.status_code in (200, 201) for r in responses)
    ids = {r.json()["request_id"] for r in responses}
    assert len(ids) == 1, "all callers must be given the same request id"
    assert sum(1 for r in responses if r.status_code == 201) == 1

    assert await pool.fetchval("SELECT count(*) FROM requests") == 1
    assert await pool.fetchval("SELECT count(*) FROM deliveries") == 1


async def test_the_same_recipient_listed_twice_is_queued_once(
    api: httpx.AsyncClient, pool: asyncpg.Pool
) -> None:
    response = await api.post(
        "/v1/requests",
        json=lead(recipients=[{"url": BLACKHOLE, "name": "crm"}, {"url": BLACKHOLE}]),
    )
    assert response.json()["recipient_count"] == 1
    assert await pool.fetchval("SELECT count(*) FROM deliveries") == 1


# ---------------------------------------------------------------------------
# A lead with nowhere to go
# ---------------------------------------------------------------------------
async def test_a_lead_without_recipients_is_accepted_and_flagged(
    api: httpx.AsyncClient, pool: asyncpg.Pool
) -> None:
    """Explicitly not an error: the address was simply never configured. Rejecting it
    would throw away a lead that has already been paid for."""
    response = await api.post("/v1/requests", json=lead(recipients=[]))

    assert response.status_code == 201
    assert response.json()["has_recipients"] is False
    assert response.json()["recipient_count"] == 0
    assert await pool.fetchval("SELECT count(*) FROM requests") == 1

    problems = (await api.get("/v1/problems")).json()
    assert problems["counts"]["no_recipients"] == 1
    assert problems["items"][0]["request_id"] == response.json()["request_id"]


async def test_recipients_may_be_omitted_entirely(api: httpx.AsyncClient) -> None:
    body = lead()
    body.pop("recipients")
    response = await api.post("/v1/requests", json=body)
    assert response.status_code == 201
    assert response.json()["has_recipients"] is False


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------
@pytest.mark.parametrize(
    ("mutate", "expected_field"),
    [
        pytest.param(lambda b: b.pop("source_id"), "source_id", id="missing-source"),
        pytest.param(lambda b: b.update(source_id="   "), "source_id", id="blank-source"),
        pytest.param(lambda b: b.pop("idempotency_key"), "idempotency_key", id="missing-key"),
        pytest.param(lambda b: b.update(idempotency_key=""), "idempotency_key", id="empty-key"),
        pytest.param(lambda b: b.pop("payload"), "payload", id="missing-payload"),
        pytest.param(lambda b: b.update(payload={}), "payload", id="empty-payload"),
        pytest.param(
            lambda b: b.update(payload="just a string"), "payload", id="payload-not-object"
        ),
        pytest.param(
            lambda b: b.update(recipients=[{"url": "ftp://crm.example.com/in"}]),
            "recipients",
            id="wrong-scheme",
        ),
        pytest.param(
            lambda b: b.update(recipients=[{"url": "not-a-url"}]),
            "recipients",
            id="not-a-url",
        ),
        pytest.param(
            lambda b: b.update(recipients=[{"url": "https:///no-host"}]),
            "recipients",
            id="no-host",
        ),
        pytest.param(
            lambda b: b.update(recipients=[{"url": "https://crm.example.com:notaport/in"}]),
            "recipients",
            id="bad-port",
        ),
        pytest.param(
            lambda b: b.update(recipients=[{"uri": "https://crm.example.com/in"}]),
            "recipients",
            id="unknown-recipient-field",
        ),
        pytest.param(
            lambda b: b.update(unexpected="field"), "unexpected", id="unknown-top-level-field"
        ),
    ],
)
async def test_malformed_leads_are_rejected_with_the_offending_field(
    api: httpx.AsyncClient, pool: asyncpg.Pool, mutate, expected_field: str
) -> None:
    body = lead()
    mutate(body)

    response = await api.post("/v1/requests", json=body)

    assert response.status_code == 422
    problem = response.json()
    assert problem["error"] == "validation_error"
    # The sender has to be able to fix this without reading our source.
    assert expected_field in problem["message"]
    assert any(expected_field in detail["field"] for detail in problem["details"])

    # A rejected lead leaves nothing behind.
    assert await pool.fetchval("SELECT count(*) FROM requests") == 0


NUL = "\x00"


@pytest.mark.parametrize(
    ("body_patch", "where"),
    [
        pytest.param({"payload": {"name": f"Иван{NUL}"}}, "payload", id="in-a-value"),
        pytest.param({"payload": {f"name{NUL}": "Иван"}}, "payload", id="in-a-key"),
        pytest.param({"payload": {"quiz": [{"answer": f"да{NUL}"}]}}, "payload", id="nested"),
        pytest.param({"source_id": f"landing{NUL}"}, "source_id", id="in-source-id"),
        pytest.param({"idempotency_key": f"k{NUL}"}, "idempotency_key", id="in-key"),
        pytest.param(
            {"recipients": [{"url": BLACKHOLE, "name": f"crm{NUL}"}]},
            "recipients",
            id="in-recipient-name",
        ),
        pytest.param(
            {"recipients": [{"url": f"http://crm.example.com/hook{NUL}"}]},
            "recipients",
            id="in-recipient-url",
        ),
    ],
)
async def test_a_null_byte_is_a_clean_rejection_not_a_crash(
    api: httpx.AsyncClient, pool: asyncpg.Pool, body_patch: dict, where: str
) -> None:
    """PostgreSQL cannot store U+0000, and mis-encoded form input really does contain it.

    Without an explicit check this fails deep inside the INSERT and surfaces as a 500
    with nothing actionable in it.
    """
    response = await api.post("/v1/requests", json=lead(**body_patch))

    assert response.status_code == 422, response.text
    assert "null byte" in response.json()["message"]
    assert any(where in detail["field"] for detail in response.json()["details"])
    assert await pool.fetchval("SELECT count(*) FROM requests") == 0


async def test_a_payload_may_contain_the_literal_text_backslash_u0000(
    api: httpx.AsyncClient, pool: asyncpg.Pool
) -> None:
    """The six characters \\u0000 are ordinary text, not a null byte. Someone writing
    about escape sequences in a comment field must not be rejected."""
    escape_text = chr(92) + "u0000"  # the six characters \u0000, not a null byte
    assert len(escape_text) == 6 and chr(0) not in escape_text
    payload = {"comment": f"он прислал {escape_text} в форме"}
    response = await api.post("/v1/requests", json=lead(payload=payload))

    assert response.status_code == 201
    assert await pool.fetchval("SELECT payload FROM requests") == payload


async def test_oversized_payload_is_rejected(api: httpx.AsyncClient) -> None:
    response = await api.post("/v1/requests", json=lead(payload={"blob": "x" * (256 * 1024 + 1)}))
    assert response.status_code == 422
    assert "limit" in response.json()["message"]


async def test_too_many_recipients_is_rejected(api: httpx.AsyncClient) -> None:
    response = await api.post(
        "/v1/requests",
        json=lead(recipients=[{"url": f"http://127.0.0.1:1/hook/{i}"} for i in range(51)]),
    )
    assert response.status_code == 422
    assert "limit is 50" in response.json()["message"]


async def test_payload_keeps_arbitrary_extra_fields(
    api: httpx.AsyncClient, pool: asyncpg.Pool
) -> None:
    """The set of lead fields is not fixed and changes per source, so nothing inside
    `payload` is validated or dropped."""
    payload = {
        "name": "Иван",
        "utm_source": "yandex",
        "quiz_answers": [{"q": 1, "a": "да"}],
        "nested": {"deep": {"value": 42}},
    }
    response = await api.post("/v1/requests", json=lead(payload=payload))

    assert response.status_code == 201
    stored = await pool.fetchval("SELECT payload FROM requests")
    assert stored == payload
