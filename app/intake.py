"""Accepting a lead.

The contract of this module is the promise in the brief: once we answer "accepted",
the lead is on disk. Everything here runs in a single short transaction and does no
network I/O, so the response time does not depend on whether any recipient is alive.
"""

from __future__ import annotations

import asyncpg

from app.recipients import origin_of
from app.schemas import IntakeAccepted, IntakeRequest, RecipientIn

_INSERT_REQUEST = """
    INSERT INTO requests (source_id, idempotency_key, payload)
    VALUES ($1, $2, $3)
    ON CONFLICT (source_id, idempotency_key) DO NOTHING
    RETURNING id, received_at
"""

_SELECT_EXISTING = """
    SELECT r.id,
           r.received_at,
           (SELECT count(*) FROM deliveries d WHERE d.request_id = r.id) AS recipient_count
    FROM requests r
    WHERE r.source_id = $1 AND r.idempotency_key = $2
"""

_INSERT_DELIVERIES = """
    INSERT INTO deliveries (request_id, recipient_name, recipient_url, recipient_origin)
    SELECT $1, t.name, t.url, t.origin
    FROM unnest($2::text[], $3::text[], $4::text[]) AS t(name, url, origin)
    ON CONFLICT (request_id, recipient_url) DO NOTHING
    RETURNING id
"""


def _dedupe(recipients: list[RecipientIn]) -> list[RecipientIn]:
    """Collapse the same URL listed twice in one request. First mention wins."""
    seen: set[str] = set()
    unique: list[RecipientIn] = []
    for recipient in recipients:
        if recipient.url in seen:
            continue
        seen.add(recipient.url)
        unique.append(recipient)
    return unique


async def _insert_deliveries(
    conn: asyncpg.Connection, request_id, recipients: list[RecipientIn]
) -> list:
    if not recipients:
        return []
    rows = await conn.fetch(
        _INSERT_DELIVERIES,
        request_id,
        [r.name for r in recipients],
        [r.url for r in recipients],
        [origin_of(r.url) for r in recipients],
    )
    return [row["id"] for row in rows]


async def accept_request(pool: asyncpg.Pool, payload: IntakeRequest) -> IntakeAccepted:
    """Store a lead and queue it for every recipient. Idempotent on (source_id, key)."""
    recipients = _dedupe(payload.recipients)

    async with pool.acquire() as conn, conn.transaction():
        row = await conn.fetchrow(
            _INSERT_REQUEST, payload.source_id, payload.idempotency_key, payload.payload
        )

        if row is None:
            # Someone already accepted this key. Report the original request and say so;
            # a repeat is not an error for the sender, so this is still a 2xx.
            #
            # Note on the race: ON CONFLICT DO NOTHING waits for a concurrent insert of
            # the same key to commit before it reports the conflict, so by the time we
            # get here the original row is visible to this READ COMMITTED transaction.
            existing = await conn.fetchrow(
                _SELECT_EXISTING, payload.source_id, payload.idempotency_key
            )
            return IntakeAccepted(
                request_id=existing["id"],
                duplicate=True,
                received_at=existing["received_at"],
                recipient_count=existing["recipient_count"],
                has_recipients=existing["recipient_count"] > 0,
            )

        delivery_ids = await _insert_deliveries(conn, row["id"], recipients)

    # A lead with no recipients is accepted exactly like any other. It simply has no
    # queue rows, which is what /v1/problems reports as "nowhere to deliver".
    return IntakeAccepted(
        request_id=row["id"],
        duplicate=False,
        received_at=row["received_at"],
        recipient_count=len(delivery_ids),
        has_recipients=bool(delivery_ids),
    )


async def attach_recipients(
    pool: asyncpg.Pool, request_id, recipients: list[RecipientIn]
) -> tuple[int, int, list] | None:
    """Add recipients to a lead that was accepted without any (or with fewer).

    This closes the loop on the "the address was never configured" case: without it,
    such a lead would sit in /v1/problems forever with no way to resolve it, because
    a manual retry has nothing to retry.

    Returns (added, already_present, new_delivery_ids), or None if the request is unknown.
    """
    unique = _dedupe(recipients)

    async with pool.acquire() as conn, conn.transaction():
        exists = await conn.fetchval("SELECT 1 FROM requests WHERE id = $1", request_id)
        if exists is None:
            return None

        new_ids = await _insert_deliveries(conn, request_id, unique)

    return len(new_ids), len(unique) - len(new_ids), new_ids
