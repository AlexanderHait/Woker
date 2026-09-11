"""Worker process entry point: `python -m app.run_worker`.

Handles SIGTERM/SIGINT so `docker compose stop` drains in-flight attempts instead of
leaving rows to sit until their leases expire.
"""

from __future__ import annotations

import asyncio
import logging
import signal

from app.config import get_settings
from app.db import apply_migrations, create_pool
from app.logging_config import configure_logging
from app.worker import Worker

logger = logging.getLogger(__name__)


async def main() -> None:
    settings = get_settings()
    configure_logging(settings.log_level)

    pool = await create_pool(settings)
    # The API normally wins the race to migrate; this is here so a worker can be started
    # on its own against an empty database (the advisory lock makes it safe either way).
    await apply_migrations(pool)

    worker = Worker(pool, settings)

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, worker.request_stop)

    try:
        await worker.run()
    finally:
        await pool.close()


if __name__ == "__main__":
    asyncio.run(main())
