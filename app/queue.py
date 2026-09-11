"""The queue.

`deliveries` is both the state table and the work queue. This module owns every
statement that moves a row between states, so the state machine can be read in one
place:

    pending ──claim──> in_flight ──success──> delivered
                          │
                          ├──failure, budget left──> pending (next_attempt_at in the future)
                          ├──failure, budget spent─> failed
                          └──worker died──────────> reclaimed by the next claim once
                                                    the lease expires

Two properties do the heavy lifting:

* **SKIP LOCKED** - concurrent workers never hand the same delivery to two HTTP calls,
  and a locked row does not block anyone; they just take the next one.
* **Leases** - a claimed row carries `lease_expires_at`. A worker killed with -9 leaves
  its rows locked only until that timestamp passes, after which any worker may take
  them. No cleanup daemon, no manual intervention, nothing to reset after a crash.

The attempt counter is incremented *at claim time*, not after the HTTP call. That is
deliberate: a process that dies mid-attempt still burns the attempt, so a delivery that
reliably kills workers cannot loop forever.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any
from uuid import UUID

import asyncpg

from app.config import Settings
from app.retry import is_exhausted, next_delay_seconds

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ClaimedDelivery:
    """A delivery this worker owns until `lease_expires_at`."""

    delivery_id: UUID
    request_id: UUID
    recipient_name: str | None
    recipient_url: str
    recipient_origin: str
    # Position in the current retry budget - drives backoff and giving up.
    budget_attempt: int
    # Position in the delivery's whole history - the journal key, never reset.
    attempt_number: int
    source_id: str
    idempotency_key: str
    received_at: datetime
    payload: dict[str, Any]


@dataclass(frozen=True)
class AttemptResult:
    """What happened during one HTTP attempt."""

    succeeded: bool
    error_kind: str | None
    status_code: int | None
    response_excerpt: str | None
    started_at: datetime
    finished_at: datetime

    @property
    def duration_ms(self) -> int:
        return max(0, int((self.finished_at - self.started_at).total_seconds() * 1000))


# ---------------------------------------------------------------------------
# Claiming
# ---------------------------------------------------------------------------
#
# `ranked` looks at a window of due rows and numbers them per recipient host; `picked`
# keeps at most `max_claims_per_origin` of each and locks only those. That cap is what
# keeps one unreachable recipient from filling the whole batch with its own backlog
# while other recipients wait - scenario 8 in the brief.
#
# The ranking deliberately runs *without* a lock and only the finally chosen rows are
# locked. Locking the whole scan window instead would make every worker hold rows it
# was never going to take, and its peers - which use SKIP LOCKED - would find nothing
# to do and go back to sleep. Measured: with an 8-worker scramble that shape claimed
# 30 of 40 ready deliveries per round; this one claims all 40.
#
# Because the ranking is unlocked, a row can change between being ranked and being
# locked. The status and due-time conditions are therefore repeated in `picked`, where
# they apply to the table itself: Postgres re-checks them against the latest version of
# each row after taking the lock, so a delivery another worker has just claimed drops
# out instead of being handed out twice.
#
# `claimable_at` is maintained by a trigger and means "the earliest time a worker may
# take this row": the backoff deadline for pending rows, the lease deadline for
# in-flight ones. One column, one index, one ordered scan.
_CLAIM = """
WITH ranked AS (
    SELECT d.id,
           d.claimable_at,
           row_number() OVER (PARTITION BY d.recipient_origin
                              ORDER BY d.claimable_at, d.id) AS rn
    FROM deliveries d
    WHERE d.status IN ('pending', 'in_flight')
      AND d.claimable_at <= now()
    ORDER BY d.claimable_at
    LIMIT $3
),
picked AS (
    SELECT d.id,
           d.request_id,
           r.claimable_at,
           d.status          AS prev_status,
           d.attempts        AS prev_attempts,
           d.total_attempts  AS prev_total_attempts,
           d.last_attempt_at AS prev_last_attempt_at
    FROM deliveries d
    JOIN ranked r ON r.id = d.id
    WHERE r.rn <= $4
      AND d.status IN ('pending', 'in_flight')
      AND d.claimable_at <= now()
    ORDER BY r.claimable_at
    LIMIT $2
    FOR UPDATE OF d SKIP LOCKED
)
UPDATE deliveries d
SET status           = 'in_flight',
    attempts         = d.attempts + 1,
    total_attempts   = d.total_attempts + 1,
    last_attempt_at  = now(),
    lease_expires_at = now() + $5::interval,
    locked_by        = $1
FROM picked p
-- The lead itself comes back with the claim rather than in a second query: a worker has
-- no use for a claimed delivery without the payload it is supposed to send.
JOIN requests req ON req.id = p.request_id
WHERE d.id = p.id
RETURNING d.id             AS delivery_id,
          d.request_id,
          d.recipient_name,
          d.recipient_url,
          d.recipient_origin,
          d.attempts       AS budget_attempt,
          d.total_attempts AS attempt_number,
          req.source_id,
          req.idempotency_key,
          req.payload,
          req.received_at,
          p.prev_status,
          p.prev_attempts,
          p.prev_total_attempts,
          p.prev_last_attempt_at
"""

# A row reclaimed from a dead worker gets an honest journal entry: we know an attempt
# was started and we do not know whether the recipient saw it. This is the at-least-once
# window made visible rather than hidden.
_RECORD_LOST_ATTEMPTS = """
    INSERT INTO delivery_attempts (
        delivery_id, request_id, attempt_number, budget_attempt, recipient_url,
        started_at, finished_at, duration_ms, outcome, error_kind, worker_id
    )
    SELECT t.delivery_id, t.request_id, t.attempt_number, t.budget_attempt, t.recipient_url,
           COALESCE(t.started_at, now()), now(),
           GREATEST(0, EXTRACT(EPOCH FROM (now() - COALESCE(t.started_at, now()))) * 1000)::int,
           'unknown', 'lease_expired', t.worker_id
    FROM unnest($1::uuid[], $2::uuid[], $3::int[], $4::int[], $5::text[],
                $6::timestamptz[], $7::text[])
         AS t(delivery_id, request_id, attempt_number, budget_attempt, recipient_url,
              started_at, worker_id)
    ON CONFLICT (delivery_id, attempt_number) DO NOTHING
"""


async def claim_batch(
    pool: asyncpg.Pool, worker_id: str, limit: int, settings: Settings
) -> list[ClaimedDelivery]:
    """Take up to `limit` due deliveries for this worker."""
    if limit <= 0:
        return []

    scan_window = limit * settings.claim_scan_multiplier
    lease = timedelta(seconds=settings.lease_seconds)

    async with pool.acquire() as conn, conn.transaction():
        rows = await conn.fetch(
            _CLAIM, worker_id, limit, scan_window, settings.max_claims_per_origin, lease
        )
        if not rows:
            return []

        reclaimed = [
            r for r in rows if r["prev_status"] == "in_flight" and r["prev_total_attempts"] > 0
        ]
        if reclaimed:
            logger.warning(
                "reclaimed %d deliveries from expired leases: %s",
                len(reclaimed),
                ", ".join(str(r["delivery_id"]) for r in reclaimed[:10]),
            )
            await conn.execute(
                _RECORD_LOST_ATTEMPTS,
                [r["delivery_id"] for r in reclaimed],
                [r["request_id"] for r in reclaimed],
                [r["prev_total_attempts"] for r in reclaimed],
                [r["prev_attempts"] for r in reclaimed],
                [r["recipient_url"] for r in reclaimed],
                [r["prev_last_attempt_at"] for r in reclaimed],
                [worker_id] * len(reclaimed),
            )

    return [
        ClaimedDelivery(
            delivery_id=row["delivery_id"],
            request_id=row["request_id"],
            recipient_name=row["recipient_name"],
            recipient_url=row["recipient_url"],
            recipient_origin=row["recipient_origin"],
            budget_attempt=row["budget_attempt"],
            attempt_number=row["attempt_number"],
            source_id=row["source_id"],
            idempotency_key=row["idempotency_key"],
            received_at=row["received_at"],
            payload=row["payload"],
        )
        for row in rows
    ]


# ---------------------------------------------------------------------------
# Recording the outcome
# ---------------------------------------------------------------------------
# If a reclaiming worker already wrote an 'unknown' placeholder for this attempt and the
# original worker then comes back with the real answer, the real answer replaces the
# placeholder. Any other conflict is left alone - the journal never rewrites known facts.
_INSERT_ATTEMPT = """
    INSERT INTO delivery_attempts (
        delivery_id, request_id, attempt_number, budget_attempt, recipient_url,
        started_at, finished_at, duration_ms,
        outcome, error_kind, status_code, response_excerpt, scheduled_next_at, worker_id
    )
    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
    ON CONFLICT (delivery_id, attempt_number) DO UPDATE
    SET finished_at       = EXCLUDED.finished_at,
        duration_ms       = EXCLUDED.duration_ms,
        outcome           = EXCLUDED.outcome,
        error_kind        = EXCLUDED.error_kind,
        status_code       = EXCLUDED.status_code,
        response_excerpt  = EXCLUDED.response_excerpt,
        scheduled_next_at = EXCLUDED.scheduled_next_at,
        worker_id         = EXCLUDED.worker_id
    WHERE delivery_attempts.outcome = 'unknown'
"""

# Success is recorded without checking the lease on purpose. If our lease expired and
# another worker already took the row, we still know the recipient has the lead; marking
# it delivered stops any further attempts instead of letting the duplicate train roll on.
_MARK_DELIVERED = """
    UPDATE deliveries
    SET status           = 'delivered',
        delivered_at     = now(),
        failed_at        = NULL,
        lease_expires_at = NULL,
        locked_by        = NULL,
        last_outcome     = 'success',
        last_error_kind  = NULL,
        last_status_code = $2,
        last_error       = NULL
    WHERE id = $1 AND status <> 'delivered'
"""

# Failure is only recorded while we still hold the lease. A stale worker must not push
# around the schedule of a row that now belongs to somebody else.
_MARK_FAILURE = """
    UPDATE deliveries
    SET status           = CASE WHEN $6 THEN 'failed'::delivery_status
                                ELSE 'pending'::delivery_status END,
        next_attempt_at  = CASE WHEN $6 THEN next_attempt_at ELSE now() + $7::interval END,
        failed_at        = CASE WHEN $6 THEN now() ELSE NULL END,
        lease_expires_at = NULL,
        locked_by        = NULL,
        last_outcome     = 'failure',
        last_error_kind  = $3,
        last_status_code = $4,
        last_error       = $5
    WHERE id = $1 AND locked_by = $2 AND status = 'in_flight'
    RETURNING next_attempt_at, status
"""


async def record_result(
    pool: asyncpg.Pool,
    worker_id: str,
    claim: ClaimedDelivery,
    result: AttemptResult,
    settings: Settings,
) -> None:
    """Write the journal entry and move the delivery to its next state."""
    excerpt = _truncate(result.response_excerpt, settings.response_excerpt_bytes)

    async with pool.acquire() as conn, conn.transaction():
        if result.succeeded:
            await conn.execute(_MARK_DELIVERED, claim.delivery_id, result.status_code)
            scheduled_next_at = None
        else:
            # Backoff and giving up are decided by the *budget*, so a manual retry
            # genuinely starts the schedule over instead of resuming at hour-long gaps.
            exhausted = is_exhausted(claim.budget_attempt, settings)
            delay = timedelta(seconds=next_delay_seconds(claim.budget_attempt, settings))
            row = await conn.fetchrow(
                _MARK_FAILURE,
                claim.delivery_id,
                worker_id,
                result.error_kind,
                result.status_code,
                excerpt,
                exhausted,
                delay,
            )
            if row is None:
                # Our lease expired while the request was on the wire and somebody else
                # owns the row now. The attempt still happened, so it is still journalled,
                # but the schedule is not ours to touch any more.
                logger.warning(
                    "lease lost before recording failure for delivery %s attempt %d",
                    claim.delivery_id,
                    claim.attempt_number,
                )
                scheduled_next_at = None
            else:
                scheduled_next_at = None if row["status"] == "failed" else row["next_attempt_at"]

        await conn.execute(
            _INSERT_ATTEMPT,
            claim.delivery_id,
            claim.request_id,
            claim.attempt_number,
            claim.budget_attempt,
            claim.recipient_url,
            result.started_at,
            result.finished_at,
            result.duration_ms,
            "success" if result.succeeded else "failure",
            result.error_kind,
            result.status_code,
            excerpt,
            scheduled_next_at,
            worker_id,
        )


def _truncate(text: str | None, limit: int) -> str | None:
    if text is None:
        return None
    return text if len(text) <= limit else text[:limit] + "…"


# ---------------------------------------------------------------------------
# Manual retry
# ---------------------------------------------------------------------------
# Requeuing resets the attempt budget: "the CRM is fixed, start over" is the intent.
# Nothing is lost by that - delivery_attempts keeps every attempt ever made.
#
# Rows a worker is actively sending (a live lease) are skipped rather than reset, so a
# manual retry never races an attempt already on the wire.
#
# Only the WHERE clause is built as text, because the set of filters varies. Everything
# else - the row cap and the include-delivered switch - goes in as a bind parameter:
# `LIMIT $n` accepts NULL to mean "no limit", so the two callers need no separate SQL.
_REQUEUE_TEMPLATE = """
WITH target AS (
    SELECT id, status, lease_expires_at
    FROM deliveries
    WHERE {where}
    ORDER BY created_at
    LIMIT {limit_param}
    FOR UPDATE SKIP LOCKED
),
classified AS (
    SELECT id,
           CASE
               WHEN status = 'in_flight' AND lease_expires_at > now() THEN 'skip_in_flight'
               WHEN status = 'delivered' AND NOT {include_delivered_param} THEN 'skip_delivered'
               ELSE 'requeue'
           END AS action
    FROM target
),
updated AS (
    UPDATE deliveries d
    SET status           = 'pending',
        attempts         = 0,
        next_attempt_at  = now(),
        lease_expires_at = NULL,
        locked_by        = NULL,
        failed_at        = NULL,
        delivered_at     = NULL
    FROM classified c
    WHERE d.id = c.id AND c.action = 'requeue'
    RETURNING d.id
)
SELECT COALESCE((SELECT array_agg(id) FROM updated), '{{}}'::uuid[])        AS requeued_ids,
       (SELECT count(*) FROM classified WHERE action = 'skip_in_flight')    AS skipped_in_flight,
       (SELECT count(*) FROM classified WHERE action = 'skip_delivered')    AS skipped_delivered
"""


@dataclass(frozen=True)
class RequeueOutcome:
    requeued_ids: list[UUID]
    skipped_in_flight: int
    skipped_delivered: int


def _build_requeue_sql(
    where: str, params: list[Any], include_delivered: bool, limit: int | None
) -> tuple[str, list[Any]]:
    """Finish a requeue statement by appending its two trailing bind parameters."""
    params = [*params, include_delivered, limit]
    sql = _REQUEUE_TEMPLATE.format(
        where=where,
        include_delivered_param=f"${len(params) - 1}::boolean",
        limit_param=f"${len(params)}",
    )
    return sql, params


def _outcome(row: asyncpg.Record) -> RequeueOutcome:
    return RequeueOutcome(
        requeued_ids=list(row["requeued_ids"]),
        skipped_in_flight=row["skipped_in_flight"],
        skipped_delivered=row["skipped_delivered"],
    )


async def requeue_request(
    pool: asyncpg.Pool,
    request_id: UUID,
    recipients: list[str] | None,
    include_delivered: bool,
) -> RequeueOutcome | None:
    """Requeue one lead: all of its recipients, or the named ones (by URL or label).

    Returns None when there is no such lead. That check lives here rather than in the
    API so both it and the requeue run on one connection, and so "unknown lead" cannot be
    confused with "lead that happens to have no recipients".
    """
    where = "request_id = $1"
    params: list[Any] = [request_id]
    if recipients:
        params.append(recipients)
        ref = f"${len(params)}"
        where += f" AND (recipient_url = ANY({ref}) OR recipient_name = ANY({ref}))"

    # No row cap: a lead has at most `max_recipients_per_request` deliveries anyway.
    sql, params = _build_requeue_sql(where, params, include_delivered, None)

    async with pool.acquire() as conn:
        known = await conn.fetchval(
            "SELECT EXISTS (SELECT 1 FROM requests WHERE id = $1)", request_id
        )
        if not known:
            return None
        return _outcome(await conn.fetchrow(sql, *params))


async def requeue_bulk(
    pool: asyncpg.Pool,
    statuses: list[str],
    recipient_origin: str | None,
    recipient_url_contains: str | None,
    received_after: datetime | None,
    received_before: datetime | None,
    limit: int,
) -> RequeueOutcome:
    """Requeue everything matching a filter - the 'flush what piled up' button."""
    params: list[Any] = [statuses]
    # Cast the parameter to the enum, never the column: `status::text = ANY($1)` makes the
    # status index unusable and forces a sequential scan (measured 9x slower, and it also
    # wrecks the planner's row estimate).
    clauses = ["status = ANY($1::delivery_status[])"]

    if recipient_origin:
        params.append(recipient_origin)
        clauses.append(f"recipient_origin = ${len(params)}")
    if recipient_url_contains:
        params.append(f"%{recipient_url_contains}%")
        clauses.append(f"recipient_url LIKE ${len(params)}")
    if received_after:
        params.append(received_after)
        clauses.append(
            f"request_id IN (SELECT id FROM requests WHERE received_at >= ${len(params)})"
        )
    if received_before:
        params.append(received_before)
        clauses.append(
            f"request_id IN (SELECT id FROM requests WHERE received_at <= ${len(params)})"
        )

    # Bulk requeue is filter-driven: naming 'delivered' in `statuses` is itself the opt-in,
    # so nothing needs to be skipped for already having arrived.
    sql, params = _build_requeue_sql(
        " AND ".join(clauses), params, include_delivered=True, limit=limit
    )
    return _outcome(await pool.fetchrow(sql, *params))
