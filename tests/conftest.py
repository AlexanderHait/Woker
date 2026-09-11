"""Test fixtures.

Two decisions worth knowing about:

* The tests run against a **real** PostgreSQL. SKIP LOCKED, advisory locks and lease
  expiry are the whole design here; a fake would test nothing that matters.
* The recipient stub runs as a **real** HTTP server in a background thread. Timeouts,
  silent sockets and slow responses only behave truthfully over a real socket.

Timings are compressed (retries in tenths of a second instead of seconds) so the suite
finishes quickly. The production defaults are asserted separately in test_retry.py.
"""

from __future__ import annotations

import asyncio
import os
import socket
import threading
import time
from collections.abc import Awaitable, Callable
from typing import Any
from uuid import UUID

# Test configuration must be in the environment before anything reads the settings.
os.environ.setdefault(
    "INTAKE_DATABASE_URL", "postgresql://intake:intake@127.0.0.1:55432/intake_test"
)
os.environ.update(
    {
        "INTAKE_RETRY_BASE_SECONDS": "0.1",
        "INTAKE_RETRY_FACTOR": "2",
        "INTAKE_RETRY_CAP_SECONDS": "2",
        "INTAKE_RETRY_MAX_ATTEMPTS": "5",
        "INTAKE_RETRY_JITTER_RATIO": "0",
        "INTAKE_CONNECT_TIMEOUT_SECONDS": "1",
        "INTAKE_READ_TIMEOUT_SECONDS": "2",
        "INTAKE_WRITE_TIMEOUT_SECONDS": "1",
        "INTAKE_ATTEMPT_DEADLINE_SECONDS": "3",
        "INTAKE_LEASE_SECONDS": "5",
        "INTAKE_WORKER_POLL_INTERVAL_SECONDS": "0.05",
        "INTAKE_WORKER_SHUTDOWN_GRACE_SECONDS": "5",
        "INTAKE_LOG_LEVEL": "WARNING",
    }
)

import asyncpg  # noqa: E402
import httpx  # noqa: E402
import pytest  # noqa: E402
import uvicorn  # noqa: E402

from app.config import Settings, get_settings  # noqa: E402
from app.db import apply_migrations, create_pool  # noqa: E402
from app.main import create_app  # noqa: E402
from app.worker import Worker  # noqa: E402
from stub.main import app as stub_app  # noqa: E402

TABLES = ("delivery_attempts", "deliveries", "requests")


# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------
@pytest.fixture(scope="session", autouse=True)
def _prepare_schema() -> None:
    """Create the schema once for the whole session, from scratch."""
    settings = get_settings()

    # Guard rail: the reset below drops everything. Refuse to point it at a database
    # that is not obviously a test one.
    database = settings.dsn.rsplit("/", 1)[-1].split("?")[0]
    if not database.endswith("_test"):
        raise RuntimeError(
            f"refusing to run destructive tests against database '{database}'; "
            f"the name must end with '_test'"
        )

    async def reset() -> None:
        pool = await create_pool(settings)
        try:
            async with pool.acquire() as conn:
                await conn.execute("DROP SCHEMA public CASCADE; CREATE SCHEMA public;")
            await apply_migrations(pool)
        finally:
            await pool.close()

    asyncio.run(reset())


@pytest.fixture
def settings() -> Settings:
    return get_settings()


@pytest.fixture
async def pool(settings: Settings):
    """A fresh pool per test, with the tables emptied first."""
    pool = await create_pool(settings)
    async with pool.acquire() as conn:
        await conn.execute(f"TRUNCATE {', '.join(TABLES)} RESTART IDENTITY CASCADE")
    try:
        yield pool
    finally:
        await pool.close()


# ---------------------------------------------------------------------------
# The API, in-process
# ---------------------------------------------------------------------------
@pytest.fixture
async def api(pool: asyncpg.Pool):
    """HTTP client wired straight to the ASGI app, sharing the test's pool.

    The app's own lifespan is bypassed so the test controls the pool; everything else
    (routing, validation, serialisation, error handlers) is the real thing.
    """
    app = create_app()
    app.state.pool = pool
    app.state.settings = get_settings()
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://intake.test") as client:
        yield client


# ---------------------------------------------------------------------------
# Recipient stub, on a real socket
# ---------------------------------------------------------------------------
class StubControl:
    """Thin async client for the stub's control plane."""

    def __init__(self, base_url: str) -> None:
        self.base_url = base_url

    def url(self, name: str) -> str:
        """The address a recipient would be configured with."""
        return f"{self.base_url}/hook/{name}"

    async def configure(self, name: str, **behaviour: Any) -> None:
        async with httpx.AsyncClient(timeout=5) as client:
            response = await client.put(f"{self.base_url}/control/{name}", json=behaviour)
            response.raise_for_status()

    async def received(self, name: str) -> list[dict]:
        async with httpx.AsyncClient(timeout=5) as client:
            response = await client.get(f"{self.base_url}/received/{name}")
            response.raise_for_status()
            return response.json()["items"]

    async def summary(self, name: str) -> dict:
        async with httpx.AsyncClient(timeout=5) as client:
            response = await client.get(f"{self.base_url}/received/{name}/summary")
            response.raise_for_status()
            return response.json()


@pytest.fixture(scope="session")
def stub() -> StubControl:
    """Run the recipient stub on an ephemeral port for the whole session."""
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]

    server = uvicorn.Server(
        uvicorn.Config(stub_app, host="127.0.0.1", port=port, log_level="warning")
    )
    # Hand uvicorn the socket we already reserved, so there is no window in which the
    # port could be taken by something else.
    thread = threading.Thread(target=lambda: server.run(sockets=[sock]), daemon=True)
    thread.start()

    deadline = time.monotonic() + 20
    while not server.started:
        if time.monotonic() > deadline:
            raise RuntimeError("recipient stub did not start")
        time.sleep(0.02)

    try:
        yield StubControl(f"http://127.0.0.1:{port}")
    finally:
        server.should_exit = True
        thread.join(timeout=10)


@pytest.fixture
def unique_name(request: pytest.FixtureRequest) -> Callable[[str], str]:
    """Stub endpoint names unique per test, so the session-wide stub needs no cleanup."""
    counter = {"n": 0}

    def make(label: str = "hook") -> str:
        counter["n"] += 1
        safe = request.node.name.replace("[", "-").replace("]", "").replace(" ", "")
        return f"{safe}-{label}-{counter['n']}"

    return make


# ---------------------------------------------------------------------------
# Driving workers
# ---------------------------------------------------------------------------
class WorkerHarness:
    """Runs real `Worker` instances and stops them cleanly at the end of a test."""

    def __init__(self, pool: asyncpg.Pool, settings: Settings) -> None:
        self._pool = pool
        self._settings = settings
        self._running: list[tuple[Worker, asyncio.Task]] = []

    def start(self, worker_id: str | None = None, settings: Settings | None = None) -> Worker:
        worker = Worker(self._pool, settings or self._settings, worker_id=worker_id)
        task = asyncio.create_task(worker.run(), name=f"worker:{worker.worker_id}")
        self._running.append((worker, task))
        return worker

    async def stop_all(self) -> None:
        """Ask every worker to finish what it is doing and stop."""
        for worker, _ in self._running:
            worker.request_stop()
        for _, task in self._running:
            try:
                await asyncio.wait_for(asyncio.shield(task), timeout=20)
            except (TimeoutError, asyncio.CancelledError):
                pass
        self._running.clear()


@pytest.fixture
async def workers(pool: asyncpg.Pool, settings: Settings):
    harness = WorkerHarness(pool, settings)
    try:
        yield harness
    finally:
        await harness.stop_all()


# ---------------------------------------------------------------------------
# Small helpers used across tests
# ---------------------------------------------------------------------------
async def wait_for(
    predicate: Callable[[], Awaitable[bool]], timeout: float = 15.0, interval: float = 0.05
) -> bool:
    """Poll `predicate` until it is true. Returns False on timeout rather than raising,
    so the calling test can assert with a message that explains the actual state."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if await predicate():
            return True
        await asyncio.sleep(interval)
    return False


async def delivery_rows(pool: asyncpg.Pool, request_id: UUID | str) -> list[asyncpg.Record]:
    return await pool.fetch(
        "SELECT * FROM deliveries WHERE request_id = $1 ORDER BY recipient_url",
        UUID(str(request_id)),
    )


async def statuses(pool: asyncpg.Pool, request_id: UUID | str) -> list[str]:
    return [row["status"] for row in await delivery_rows(pool, request_id)]


def lead(**overrides: Any) -> dict:
    """A valid intake body, with a unique idempotency key unless one is given."""
    body = {
        "source_id": "landing",
        "idempotency_key": f"key-{time.monotonic_ns()}",
        "payload": {"name": "Иван", "phone": "+7 900 000-00-00", "comment": "перезвоните"},
        "recipients": [],
    }
    body.update(overrides)
    return body
