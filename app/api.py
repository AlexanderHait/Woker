"""HTTP endpoints.

Two groups:

  * the intake - one endpoint, deliberately dull and deliberately fast;
  * the visibility surface - status, the problem list, counters, manual retry, and
    attaching recipients after the fact.

There is no authentication anywhere, as the brief asks.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from uuid import UUID

import asyncpg
from fastapi import APIRouter, Depends, HTTPException, Query, Request, Response, status

from app.config import Settings, get_settings
from app.intake import accept_request, attach_recipients
from app.queue import requeue_bulk, requeue_request
from app.reporting import get_problems, get_request_status, get_stats
from app.retry import schedule_preview, total_retry_window
from app.schemas import (
    AttachRecipientsRequest,
    AttachRecipientsResponse,
    BulkRetryRequest,
    IntakeAccepted,
    IntakeRequest,
    ProblemsResponse,
    RequestStatus,
    RetryRequest,
    RetryResponse,
    StatsResponse,
)

router = APIRouter()


def get_pool(request: Request) -> asyncpg.Pool:
    return request.app.state.pool


def settings_dep() -> Settings:
    return get_settings()


def _as_utc(value: datetime) -> datetime:
    """Accept naive timestamps in query strings and read them as UTC."""
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value


# ---------------------------------------------------------------------------
# Intake
# ---------------------------------------------------------------------------
@router.post(
    "/v1/requests",
    response_model=IntakeAccepted,
    status_code=status.HTTP_201_CREATED,
    summary="Accept a lead",
    responses={
        200: {"description": "Already accepted earlier; the original request is returned."},
        201: {"description": "Accepted and queued."},
        422: {"description": "The lead is malformed; nothing was stored."},
    },
)
async def create_request(
    payload: IntakeRequest,
    response: Response,
    pool: asyncpg.Pool = Depends(get_pool),
) -> IntakeAccepted:
    """Store the lead and queue it. Returns before any delivery is attempted.

    A repeat of a known (source_id, idempotency_key) answers 200 with the original
    request id and `duplicate: true` - a success, because the sender did nothing wrong.
    """
    result = await accept_request(pool, payload)
    if result.duplicate:
        response.status_code = status.HTTP_200_OK
    return result


# ---------------------------------------------------------------------------
# Visibility
# ---------------------------------------------------------------------------
@router.get(
    "/v1/requests/{request_id}",
    response_model=RequestStatus,
    summary="Everything known about one lead",
)
async def read_request(
    request_id: UUID,
    pool: asyncpg.Pool = Depends(get_pool),
    settings: Settings = Depends(settings_dep),
) -> RequestStatus:
    result = await get_request_status(pool, request_id, settings)
    if result is None:
        raise HTTPException(status_code=404, detail=f"no request with id {request_id}")
    return result


@router.get(
    "/v1/problems",
    response_model=ProblemsResponse,
    summary="What is not in order right now",
)
async def read_problems(
    pool: asyncpg.Pool = Depends(get_pool),
    settings: Settings = Depends(settings_dep),
    stale_minutes: int | None = Query(
        default=None,
        gt=0,
        description="Undelivered for longer than this counts as stalled.",
    ),
    reason: str | None = Query(default=None, pattern="^(no_recipients|failed|stalled)$"),
    limit: int = Query(default=100, gt=0, le=1000),
    offset: int = Query(default=0, ge=0),
) -> ProblemsResponse:
    """The single endpoint a human opens in the morning.

    Oldest trouble first. `counts` always covers all three categories, even when the
    list itself is filtered to one of them.
    """
    return await get_problems(
        pool,
        settings,
        stale_minutes=stale_minutes or settings.default_stale_minutes,
        reason=reason,
        limit=limit,
        offset=offset,
    )


@router.get("/v1/stats", response_model=StatsResponse, summary="Counters for a period")
async def read_stats(
    pool: asyncpg.Pool = Depends(get_pool),
    period_from: datetime | None = Query(default=None, alias="from"),
    period_to: datetime | None = Query(default=None, alias="to"),
) -> StatsResponse:
    """Defaults to the last 24 hours."""
    now = datetime.now(UTC)
    start = _as_utc(period_from) if period_from else now - timedelta(days=1)
    end = _as_utc(period_to) if period_to else now
    if start >= end:
        raise HTTPException(status_code=422, detail="'from' must be earlier than 'to'")
    return await get_stats(pool, start, end)


# ---------------------------------------------------------------------------
# Repair actions
# ---------------------------------------------------------------------------
@router.post(
    "/v1/requests/{request_id}/retry",
    response_model=RetryResponse,
    summary="Try this lead again now",
)
async def retry_request(
    request_id: UUID,
    body: RetryRequest | None = None,
    pool: asyncpg.Pool = Depends(get_pool),
) -> RetryResponse:
    """Requeue one lead - all recipients, or the ones named by URL or label.

    The attempt budget is reset, so a lead that had given up gets a full set of retries
    again. Deliveries a worker is sending right now are left alone.
    """
    body = body or RetryRequest()

    async with pool.acquire() as conn:
        exists = await conn.fetchval("SELECT 1 FROM requests WHERE id = $1", request_id)
    if exists is None:
        raise HTTPException(status_code=404, detail=f"no request with id {request_id}")

    outcome = await requeue_request(pool, request_id, body.recipients, body.include_delivered)
    return RetryResponse(
        requeued=len(outcome.requeued_ids),
        skipped_in_flight=outcome.skipped_in_flight,
        skipped_delivered=outcome.skipped_delivered,
        delivery_ids=outcome.requeued_ids,
    )


@router.post(
    "/v1/deliveries/retry",
    response_model=RetryResponse,
    summary="Requeue everything matching a filter",
)
async def retry_bulk(
    body: BulkRetryRequest,
    pool: asyncpg.Pool = Depends(get_pool),
) -> RetryResponse:
    """The "the CRM is back, flush the backlog" button.

    Defaults to every delivery in `failed`. Narrow it with `recipient_origin` when only
    one recipient was broken.
    """
    outcome = await requeue_bulk(
        pool,
        statuses=[s.value for s in body.status],
        recipient_origin=body.recipient_origin,
        recipient_url_contains=body.recipient_url_contains,
        received_after=_as_utc(body.received_after) if body.received_after else None,
        received_before=_as_utc(body.received_before) if body.received_before else None,
        limit=body.limit,
    )
    return RetryResponse(
        requeued=len(outcome.requeued_ids),
        skipped_in_flight=outcome.skipped_in_flight,
        skipped_delivered=outcome.skipped_delivered,
        delivery_ids=outcome.requeued_ids,
    )


@router.post(
    "/v1/requests/{request_id}/recipients",
    response_model=AttachRecipientsResponse,
    summary="Attach recipients to an already-accepted lead",
)
async def add_recipients(
    request_id: UUID,
    body: AttachRecipientsRequest,
    pool: asyncpg.Pool = Depends(get_pool),
) -> AttachRecipientsResponse:
    """Resolves the "the address was never configured" case.

    A lead accepted with no recipients has nothing to retry, so a manual retry cannot
    help it. This is how it leaves the problem list: give it somewhere to go, and the
    new deliveries are queued immediately.
    """
    result = await attach_recipients(pool, request_id, body.recipients)
    if result is None:
        raise HTTPException(status_code=404, detail=f"no request with id {request_id}")

    added, already_present, delivery_ids = result
    return AttachRecipientsResponse(
        request_id=request_id,
        added=added,
        already_present=already_present,
        delivery_ids=delivery_ids,
    )


# ---------------------------------------------------------------------------
# Operational
# ---------------------------------------------------------------------------
@router.get("/healthz", summary="Liveness plus the effective delivery policy")
async def healthz(settings: Settings = Depends(settings_dep)) -> dict:
    window = total_retry_window(settings)
    return {
        "status": "ok",
        "retry_policy": {
            "max_attempts": settings.retry_max_attempts,
            "first_delays_seconds": schedule_preview(settings),
            "cap_seconds": settings.retry_cap_seconds,
            "total_window_seconds": round(window),
            "total_window_hours": round(window / 3600, 2),
        },
        "timeouts": {
            "connect_seconds": settings.connect_timeout_seconds,
            "read_seconds": settings.read_timeout_seconds,
            "attempt_deadline_seconds": settings.attempt_deadline_seconds,
            "lease_seconds": settings.lease_seconds,
        },
    }


@router.get("/readyz", summary="Readiness - checks the database is reachable")
async def readyz(pool: asyncpg.Pool = Depends(get_pool)) -> dict:
    try:
        await pool.fetchval("SELECT 1")
    except Exception as exc:  # noqa: BLE001 - the point is to report, not to raise
        raise HTTPException(status_code=503, detail=f"database unavailable: {exc}") from exc
    return {"status": "ready"}
