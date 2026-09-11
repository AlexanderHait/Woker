"""Connection pool and schema migrations.

Migrations are plain ordered .sql files. Each runs once, inside its own transaction,
under a Postgres advisory lock - so starting the API and several workers at the same
time on an empty database is safe: exactly one of them applies the schema and the
rest wait and then see it.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path

import asyncpg

from app.config import Settings

logger = logging.getLogger(__name__)

MIGRATIONS_DIR = Path(__file__).resolve().parent.parent / "migrations"

# Arbitrary but fixed: every process that might migrate uses this same lock key.
MIGRATION_LOCK_KEY = 0x1D7A_4E21


async def _init_connection(conn: asyncpg.Connection) -> None:
    """Make asyncpg hand us dicts for jsonb instead of raw strings."""
    await conn.set_type_codec(
        "jsonb",
        encoder=json.dumps,
        decoder=json.loads,
        schema="pg_catalog",
    )


async def create_pool(settings: Settings) -> asyncpg.Pool:
    return await asyncpg.create_pool(
        dsn=settings.dsn,
        min_size=settings.db_pool_min_size,
        max_size=settings.db_pool_max_size,
        command_timeout=settings.db_command_timeout_seconds,
        init=_init_connection,
    )


def _migration_files() -> list[Path]:
    return sorted(MIGRATIONS_DIR.glob("*.sql"), key=lambda p: p.name)


async def apply_migrations(pool: asyncpg.Pool) -> list[str]:
    """Apply any migration files not yet recorded. Returns the names applied."""
    applied: list[str] = []

    async with pool.acquire() as conn:
        # Session-level lock held for the whole migration run; serialises concurrent starts.
        await conn.execute("SELECT pg_advisory_lock($1)", MIGRATION_LOCK_KEY)
        try:
            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS schema_migrations (
                    name       TEXT        PRIMARY KEY,
                    applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
                )
                """
            )
            done = {row["name"] for row in await conn.fetch("SELECT name FROM schema_migrations")}

            for path in _migration_files():
                if path.name in done:
                    continue
                logger.info("applying migration %s", path.name)
                async with conn.transaction():
                    await conn.execute(path.read_text(encoding="utf-8"))
                    await conn.execute(
                        "INSERT INTO schema_migrations (name) VALUES ($1)", path.name
                    )
                applied.append(path.name)
        finally:
            await conn.execute("SELECT pg_advisory_unlock($1)", MIGRATION_LOCK_KEY)

    return applied


async def reset_schema(pool: asyncpg.Pool) -> None:
    """Drop everything and re-migrate. Used by the test suite, never in production paths."""
    async with pool.acquire() as conn:
        await conn.execute("DROP SCHEMA public CASCADE; CREATE SCHEMA public;")
    await apply_migrations(pool)
