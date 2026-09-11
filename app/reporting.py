"""Read-side queries: status of one lead, the problem list, and counters.

The brief is blunt about why this exists - a silently losing intake already exists and
is worthless. So the rule here is that every state a lead can be in is reachable from
an HTTP endpoint, including the awkward ones (accepted but nowhere to deliver, given up
on, delivered only to some recipients).
"""

from __future__ import annotations

from datetime import datetime, timedelta
from uuid import UUID

import asyncpg

from app.config import Settings
from app.schemas import (
    AttemptOut,
    DeliveryOut,
    ProblemItem,
    ProblemsResponse,
    RequestState,
    RequestStatus,
    StatsResponse,
)

_SELECT_REQUEST = """
    SELECT id, source_id, idempotency_key, payload, received_at
    FROM requests WHERE id = $1
"""

_SELECT_DELIVERIES = """
    SELECT id, recipient_name, recipient_url, status, attempts, total_attempts,
           next_attempt_at, last_attempt_at, last_outcome, last_error_kind,
           last_status_code, last_error, delivered_at, failed_at
    FROM deliveries
    WHERE request_id = $1
    ORDER BY created_at, recipient_url
"""

_SELECT_ATTEMPTS = """
    SELECT delivery_id, attempt_number, budget_attempt, started_at, finished_at, duration_ms,
           outcome, error_kind, status_code, response_excerpt, scheduled_next_at, worker_id
    FROM delivery_attempts
    WHERE request_id = $1
    ORDER BY delivery_id, attempt_number
"""


def _request_state(statuses: list[str]) -> RequestState:
    if not statuses:
        return RequestState.no_recipients
    if any(s in ("pending", "in_flight") for s in statuses):
        return RequestState.in_progress
    if all(s == "delivered" for s in statuses):
        return RequestState.delivered
    if all(s == "failed" for s in statuses):
        return RequestState.failed
    return RequestState.partially_delivered


async def get_request_status(
    pool: asyncpg.Pool, request_id: UUID, settings: Settings
) -> RequestStatus | None:
    async with pool.acquire() as conn:
        request = await conn.fetchrow(_SELECT_REQUEST, request_id)
        if request is None:
            return None
        delivery_rows = await conn.fetch(_SELECT_DELIVERIES, request_id)
        attempt_rows = await conn.fetch(_SELECT_ATTEMPTS, request_id)

    attempts_by_delivery: dict[UUID, list[AttemptOut]] = {}
    for row in attempt_rows:
        attempts_by_delivery.setdefault(row["delivery_id"], []).append(
            AttemptOut(
                attempt_number=row["attempt_number"],
                budget_attempt=row["budget_attempt"],
                started_at=row["started_at"],
                finished_at=row["finished_at"],
                duration_ms=row["duration_ms"],
                outcome=row["outcome"],
                error_kind=row["error_kind"],
                status_code=row["status_code"],
                response_excerpt=row["response_excerpt"],
                scheduled_next_at=row["scheduled_next_at"],
                worker_id=row["worker_id"],
            )
        )

    deliveries = []
    for row in delivery_rows:
        log = attempts_by_delivery.get(row["id"], [])
        deliveries.append(
            DeliveryOut(
                delivery_id=row["id"],
                recipient_name=row["recipient_name"],
                recipient_url=row["recipient_url"],
                status=row["status"],
                attempts=row["attempts"],
                total_attempts=row["total_attempts"],
                max_attempts=settings.retry_max_attempts,
                # Only meaningful while the delivery is still going somewhere.
                next_attempt_at=(
                    row["next_attempt_at"] if row["status"] in ("pending", "in_flight") else None
                ),
                last_attempt_at=row["last_attempt_at"],
                last_outcome=row["last_outcome"],
                last_error_kind=row["last_error_kind"],
                last_status_code=row["last_status_code"],
                last_error=row["last_error"],
                delivered_at=row["delivered_at"],
                failed_at=row["failed_at"],
                attempt_log=log,
            )
        )

    return RequestStatus(
        request_id=request["id"],
        source_id=request["source_id"],
        idempotency_key=request["idempotency_key"],
        received_at=request["received_at"],
        state=_request_state([d.status.value for d in deliveries]),
        payload=request["payload"],
        recipients=deliveries,
    )


# ---------------------------------------------------------------------------
# Problems
# ---------------------------------------------------------------------------
# The three ways a lead can be "not in order", in one list:
#   no_recipients - accepted, but nobody was ever configured to receive it
#   failed        - the attempt budget is spent and we gave up
#   stalled       - still trying, but it has been outstanding too long
#
# Ordered oldest-trouble-first, because that is the order a human wants to work through
# them in the morning.
_PROBLEMS_CTE = """
WITH problems AS (
    SELECT 'no_recipients'::text  AS reason,
           r.id                   AS request_id,
           r.source_id            AS source_id,
           r.received_at          AS received_at,
           NULL::uuid             AS delivery_id,
           NULL::text             AS recipient_name,
           NULL::text             AS recipient_url,
           NULL::text             AS status,
           NULL::int              AS attempts,
           NULL::timestamptz      AS next_attempt_at,
           NULL::text             AS last_error_kind,
           NULL::int              AS last_status_code,
           NULL::text             AS last_error,
           r.received_at          AS trouble_since
    FROM requests r
    WHERE NOT EXISTS (SELECT 1 FROM deliveries d WHERE d.request_id = r.id)

    UNION ALL

    SELECT 'failed',
           r.id, r.source_id, r.received_at,
           d.id, d.recipient_name, d.recipient_url, d.status::text, d.attempts,
           NULL::timestamptz,
           d.last_error_kind, d.last_status_code, d.last_error,
           COALESCE(d.failed_at, d.created_at)
    FROM deliveries d
    JOIN requests r ON r.id = d.request_id
    WHERE d.status = 'failed'

    UNION ALL

    SELECT 'stalled',
           r.id, r.source_id, r.received_at,
           d.id, d.recipient_name, d.recipient_url, d.status::text, d.attempts,
           d.next_attempt_at,
           d.last_error_kind, d.last_status_code, d.last_error,
           d.created_at
    FROM deliveries d
    JOIN requests r ON r.id = d.request_id
    WHERE d.status IN ('pending', 'in_flight')
      AND d.created_at < now() - $1::interval
)
"""

_PROBLEMS_PAGE = (
    _PROBLEMS_CTE
    + """
SELECT *
FROM problems
WHERE ($2::text IS NULL OR reason = $2)
ORDER BY trouble_since ASC, request_id
LIMIT $3 OFFSET $4
"""
)

_PROBLEMS_COUNTS = (
    _PROBLEMS_CTE
    + """
SELECT reason, count(*) AS count FROM problems GROUP BY reason
"""
)


async def get_problems(
    pool: asyncpg.Pool,
    settings: Settings,
    stale_minutes: int,
    reason: str | None,
    limit: int,
    offset: int,
) -> ProblemsResponse:
    stale = timedelta(minutes=stale_minutes)

    async with pool.acquire() as conn:
        count_rows = await conn.fetch(_PROBLEMS_COUNTS, stale)
        page_rows = await conn.fetch(_PROBLEMS_PAGE, stale, reason, limit, offset)
        now = await conn.fetchval("SELECT now()")

    counts = {"no_recipients": 0, "failed": 0, "stalled": 0}
    for row in count_rows:
        counts[row["reason"]] = row["count"]

    items = [
        ProblemItem(
            reason=row["reason"],
            request_id=row["request_id"],
            source_id=row["source_id"],
            received_at=row["received_at"],
            age_seconds=(now - row["trouble_since"]).total_seconds(),
            delivery_id=row["delivery_id"],
            recipient_name=row["recipient_name"],
            recipient_url=row["recipient_url"],
            status=row["status"],
            attempts=row["attempts"],
            next_attempt_at=row["next_attempt_at"],
            last_error_kind=row["last_error_kind"],
            last_status_code=row["last_status_code"],
            last_error=row["last_error"],
        )
        for row in page_rows
    ]

    # Derived from the counts, not from the page: a page past the end still has to
    # report how many problems there are, or paging looks like everything got fixed.
    total = sum(counts.values()) if reason is None else counts.get(reason, 0)

    return ProblemsResponse(
        generated_at=now,
        stale_after_seconds=stale_minutes * 60,
        counts=counts,
        total=total,
        limit=limit,
        offset=offset,
        items=items,
    )


# ---------------------------------------------------------------------------
# Counters
# ---------------------------------------------------------------------------
# Request- and delivery-shaped counters are scoped by when the *lead* was accepted, so
# the numbers on one line describe one cohort of leads. Attempt counters are scoped by
# when the attempt happened, which is what "how much did we retry last hour" means.
_STATS = """
SELECT
  (SELECT count(*) FROM requests r
     WHERE r.received_at >= $1 AND r.received_at < $2)                      AS requests_accepted,
  (SELECT count(*) FROM requests r
     WHERE r.received_at >= $1 AND r.received_at < $2
       AND NOT EXISTS (SELECT 1 FROM deliveries d WHERE d.request_id = r.id))
                                              AS requests_without_recipients,
  (SELECT count(*) FROM deliveries d JOIN requests r ON r.id = d.request_id
     WHERE r.received_at >= $1 AND r.received_at < $2)                      AS deliveries_total,
  (SELECT count(*) FROM deliveries d JOIN requests r ON r.id = d.request_id
     WHERE r.received_at >= $1 AND r.received_at < $2
       AND d.status = 'delivered')                                          AS deliveries_delivered,
  (SELECT count(*) FROM deliveries d JOIN requests r ON r.id = d.request_id
     WHERE r.received_at >= $1 AND r.received_at < $2
       AND d.status IN ('pending', 'in_flight'))                            AS deliveries_queued,
  (SELECT count(*) FROM deliveries d JOIN requests r ON r.id = d.request_id
     WHERE r.received_at >= $1 AND r.received_at < $2
       AND d.status = 'failed')                                             AS deliveries_failed,
  (SELECT count(*) FROM delivery_attempts a
     WHERE a.started_at >= $1 AND a.started_at < $2)                        AS attempts_total,
  (SELECT count(*) FROM delivery_attempts a
     WHERE a.started_at >= $1 AND a.started_at < $2
       AND a.outcome <> 'success')                                          AS attempts_failed
"""


async def get_stats(
    pool: asyncpg.Pool, period_from: datetime, period_to: datetime
) -> StatsResponse:
    row = await pool.fetchrow(_STATS, period_from, period_to)
    return StatsResponse(
        period_from=period_from,
        period_to=period_to,
        requests_accepted=row["requests_accepted"],
        requests_without_recipients=row["requests_without_recipients"],
        deliveries_total=row["deliveries_total"],
        deliveries_delivered=row["deliveries_delivered"],
        deliveries_queued=row["deliveries_queued"],
        deliveries_failed=row["deliveries_failed"],
        attempts_total=row["attempts_total"],
        attempts_failed=row["attempts_failed"],
    )
