"""The delivery worker.

One process runs one `Worker`. Several workers may run at once (the compose file starts
two) - they coordinate purely through the database, so scaling out means starting more
processes and nothing else.

How a slow recipient is prevented from holding everyone up, concretely:

  * Sending is asynchronous. A recipient that takes 30 seconds occupies one of
    `worker_concurrency` slots and zero CPU; deliveries to healthy recipients continue
    past it in the same event loop.
  * `max_claims_per_origin` caps how much of a single batch one recipient host may take,
    so a recipient with a 10 000-lead backlog cannot crowd the others out of the queue.
  * `attempt_deadline_seconds` bounds how long a slot can be held at all.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import socket
import uuid

import asyncpg

from app.config import Settings
from app.queue import ClaimedDelivery, claim_batch, record_result
from app.sender import Sender

logger = logging.getLogger(__name__)


def generate_worker_id() -> str:
    """Identifies the lease holder. Must be unique across processes and restarts."""
    return f"{socket.gethostname()}-{os.getpid()}-{uuid.uuid4().hex[:8]}"


class Worker:
    def __init__(
        self,
        pool: asyncpg.Pool,
        settings: Settings,
        worker_id: str | None = None,
        sender: Sender | None = None,
    ) -> None:
        self.pool = pool
        self.settings = settings
        self.worker_id = worker_id or generate_worker_id()
        self._sender = sender or Sender(settings)
        self._owns_sender = sender is None
        self._in_flight: set[asyncio.Task] = set()
        self._stop = asyncio.Event()

    # -- lifecycle ---------------------------------------------------------
    def request_stop(self) -> None:
        self._stop.set()

    @property
    def in_flight(self) -> int:
        return len(self._in_flight)

    async def run(self) -> None:
        logger.info(
            "worker %s starting (concurrency=%d, batch=%d, lease=%.0fs, max_attempts=%d)",
            self.worker_id,
            self.settings.worker_concurrency,
            self.settings.worker_batch_size,
            self.settings.lease_seconds,
            self.settings.retry_max_attempts,
        )
        try:
            while not self._stop.is_set():
                await self._tick()
        finally:
            await self._drain()

    async def _tick(self) -> None:
        """One pass: top up the in-flight set, then either loop hot or idle briefly."""
        free_slots = self.settings.worker_concurrency - len(self._in_flight)
        if free_slots <= 0:
            # Saturated: wait for a slot instead of spinning on the database.
            await self._wait_for_slot()
            return

        limit = min(free_slots, self.settings.worker_batch_size)
        claimed = await claim_batch(self.pool, self.worker_id, limit, self.settings)

        for claim in claimed:
            self._spawn(claim)

        if not claimed:
            # Nothing due. Sleep, but wake early if we are asked to stop.
            await self._sleep_unless_stopped(self.settings.worker_poll_interval_seconds)

        # A short batch is *not* a reason to sleep: `max_claims_per_origin` deliberately
        # returns fewer rows than asked for when the due work belongs to few recipients.
        # Sleeping on that would turn a fairness rule into a throughput limit - a burst
        # for one recipient would trickle out at (cap / poll interval) per second. So we
        # go straight round again, and the in-flight ceiling is what stops the loop.

    def _spawn(self, claim: ClaimedDelivery) -> None:
        task = asyncio.create_task(
            self._deliver(claim), name=f"deliver:{claim.delivery_id}:{claim.attempt_number}"
        )
        self._in_flight.add(task)
        task.add_done_callback(self._in_flight.discard)

    async def _deliver(self, claim: ClaimedDelivery) -> None:
        try:
            result = await self._sender.send(claim)
            await record_result(self.pool, self.worker_id, claim, result, self.settings)
            logger.info(
                "delivery %s attempt %d to %s: %s%s",
                claim.delivery_id,
                claim.attempt_number,
                claim.recipient_url,
                "delivered" if result.succeeded else f"failed ({result.error_kind})",
                f" status={result.status_code}" if result.status_code else "",
            )
        except asyncio.CancelledError:
            raise
        except Exception:
            # Recording failed (typically the database went away). The row stays
            # in_flight and its lease will expire, so the delivery is retried rather
            # than lost. Nothing to do here beyond making it loud.
            logger.exception(
                "could not record the outcome of delivery %s attempt %d; "
                "the lease will expire and it will be retried",
                claim.delivery_id,
                claim.attempt_number,
            )

    async def _wait_for_slot(self) -> None:
        if not self._in_flight:
            return
        await asyncio.wait(
            self._in_flight,
            timeout=self.settings.worker_poll_interval_seconds,
            return_when=asyncio.FIRST_COMPLETED,
        )

    async def _sleep_unless_stopped(self, seconds: float) -> None:
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(self._stop.wait(), timeout=seconds)

    async def _drain(self) -> None:
        """Stop claiming and let attempts already on the wire finish.

        Anything still running when the grace period ends is abandoned: its lease
        expires and another worker (or this one after restart) picks it up. Nothing is
        lost either way - this only decides how quickly the row becomes claimable again.
        """
        if self._in_flight:
            logger.info(
                "worker %s draining %d in-flight deliveries (grace %.0fs)",
                self.worker_id,
                len(self._in_flight),
                self.settings.worker_shutdown_grace_seconds,
            )
            done, pending = await asyncio.wait(
                self._in_flight, timeout=self.settings.worker_shutdown_grace_seconds
            )
            if pending:
                logger.warning(
                    "worker %s abandoning %d deliveries at shutdown; their leases expire "
                    "in at most %.0fs and they will be retried",
                    self.worker_id,
                    len(pending),
                    self.settings.lease_seconds,
                )
                for task in pending:
                    task.cancel()
                await asyncio.gather(*pending, return_exceptions=True)

        if self._owns_sender:
            await self._sender.aclose()
        logger.info("worker %s stopped", self.worker_id)
