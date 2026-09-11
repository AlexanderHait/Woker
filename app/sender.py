"""Delivering one lead to one recipient over HTTP.

Success is exactly "the recipient answered 2xx". Everything else - a 500, a redirect,
a refused connection, a socket that accepts and then says nothing - is a failure and
goes back to the queue.

Every attempt carries `Idempotency-Key`, which is the delivery id and therefore stable
across all retries of the same (lead, recipient) pair. That is the recipient's handle
for dropping a duplicate: if a worker dies between sending and recording, we will send
again, and the key lets the other side recognise it as the same delivery.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import UTC, datetime

import httpx

from app.config import Settings
from app.queue import AttemptResult, ClaimedDelivery

logger = logging.getLogger(__name__)

# Error kinds recorded in the journal. They do not change the retry policy - the brief
# says every non-2xx is a failure - but they tell whoever reads /v1/problems whether to
# wait or to go fix an address.
ERROR_TIMEOUT = "timeout"
ERROR_CONNECTION = "connection"
ERROR_DEADLINE = "deadline_exceeded"
ERROR_REDIRECT = "http_redirect"
ERROR_CLIENT = "http_client_error"  # 4xx: usually a wrong address or a rejected body
ERROR_RETRYABLE = "http_retryable"  # 408 / 425 / 429: the recipient asked us to wait
ERROR_SERVER = "http_server_error"  # 5xx: the recipient is broken, keep knocking
ERROR_UNEXPECTED = "unexpected_error"

_EXPLICITLY_TRANSIENT = {408, 425, 429}


def classify_status(status_code: int) -> str | None:
    """Error kind for a response, or None when the response counts as delivered."""
    if 200 <= status_code < 300:
        return None
    if 300 <= status_code < 400:
        return ERROR_REDIRECT
    if status_code in _EXPLICITLY_TRANSIENT:
        return ERROR_RETRYABLE
    if 400 <= status_code < 500:
        return ERROR_CLIENT
    return ERROR_SERVER


def build_body(claim: ClaimedDelivery) -> dict:
    """The JSON we POST to the recipient."""
    return {
        "delivery_id": str(claim.delivery_id),
        "request_id": str(claim.request_id),
        "source_id": claim.source_id,
        "idempotency_key": claim.idempotency_key,
        "received_at": claim.received_at.isoformat(),
        "attempt": claim.attempt_number,
        "payload": claim.payload,
    }


def build_headers(claim: ClaimedDelivery, settings: Settings) -> dict[str, str]:
    return {
        "Content-Type": "application/json",
        "User-Agent": settings.user_agent,
        # Stable for the lifetime of this (lead, recipient) pair, across every retry.
        "Idempotency-Key": str(claim.delivery_id),
        "X-Intake-Request-Id": str(claim.request_id),
        "X-Intake-Source-Id": claim.source_id,
        "X-Intake-Attempt": str(claim.attempt_number),
    }


class Sender:
    """Owns the outbound HTTP client. One per worker process."""

    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._client = httpx.AsyncClient(
            timeout=httpx.Timeout(
                connect=settings.connect_timeout_seconds,
                read=settings.read_timeout_seconds,
                write=settings.write_timeout_seconds,
                pool=settings.connect_timeout_seconds,
            ),
            # The pool must never be the bottleneck: if it were, a slow recipient
            # holding connections would stall deliveries to healthy ones.
            limits=httpx.Limits(
                max_connections=settings.worker_concurrency + 10,
                max_keepalive_connections=settings.worker_concurrency,
            ),
            # A recipient that redirects is misconfigured; we want that visible in the
            # journal rather than silently followed to somewhere we never agreed to post.
            follow_redirects=False,
        )

    async def aclose(self) -> None:
        await self._client.aclose()

    async def send(self, claim: ClaimedDelivery) -> AttemptResult:
        started_at = datetime.now(UTC)
        try:
            # Belt and braces over the per-phase timeouts: one attempt can never occupy
            # a worker slot for longer than this, whatever the transport does.
            async with asyncio.timeout(self._settings.attempt_deadline_seconds):
                response = await self._client.post(
                    claim.recipient_url,
                    json=build_body(claim),
                    headers=build_headers(claim, self._settings),
                )
        except TimeoutError:
            return self._failure(started_at, ERROR_DEADLINE, None, "attempt deadline exceeded")
        except httpx.TimeoutException as exc:
            return self._failure(started_at, ERROR_TIMEOUT, None, f"{type(exc).__name__}: {exc}")
        except httpx.TransportError as exc:
            return self._failure(started_at, ERROR_CONNECTION, None, f"{type(exc).__name__}: {exc}")
        except Exception as exc:  # noqa: BLE001 - never let one recipient kill the worker
            logger.exception("unexpected error delivering %s", claim.delivery_id)
            return self._failure(started_at, ERROR_UNEXPECTED, None, f"{type(exc).__name__}: {exc}")

        error_kind = classify_status(response.status_code)
        excerpt = _response_excerpt(response, self._settings.response_excerpt_bytes)

        return AttemptResult(
            succeeded=error_kind is None,
            error_kind=error_kind,
            status_code=response.status_code,
            response_excerpt=excerpt,
            started_at=started_at,
            finished_at=datetime.now(UTC),
        )

    @staticmethod
    def _failure(
        started_at: datetime, kind: str, status_code: int | None, message: str
    ) -> AttemptResult:
        return AttemptResult(
            succeeded=False,
            error_kind=kind,
            status_code=status_code,
            response_excerpt=message,
            started_at=started_at,
            finished_at=datetime.now(UTC),
        )


def _response_excerpt(response: httpx.Response, limit: int) -> str | None:
    """Keep a slice of the recipient's answer so the error is debuggable later."""
    try:
        text = response.text
    except Exception:  # noqa: BLE001 - undecodable body must not fail the attempt
        return f"<{len(response.content)} bytes, undecodable>"
    text = text.strip()
    if not text:
        return None
    return text[:limit]
