"""API application factory.

Startup order matters: the pool comes up, migrations run, and only then does the app
start serving. A container that cannot migrate refuses to accept leads rather than
accepting them into a schema that is not there.
"""

from __future__ import annotations

import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

from app.api import router
from app.config import get_settings
from app.db import apply_migrations, create_pool
from app.logging_config import configure_logging
from app.schemas import ErrorDetail, ErrorResponse

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    settings = get_settings()
    configure_logging(settings.log_level)

    app.state.settings = settings
    app.state.pool = await create_pool(settings)

    applied = await apply_migrations(app.state.pool)
    if applied:
        logger.info("applied migrations: %s", ", ".join(applied))

    try:
        yield
    finally:
        await app.state.pool.close()


def create_app() -> FastAPI:
    app = FastAPI(
        title="Intake",
        version="1.0.0",
        summary="Accepts leads instantly and keeps delivering them until they arrive.",
        lifespan=lifespan,
    )
    app.include_router(router)

    @app.exception_handler(RequestValidationError)
    async def _validation_handler(_: Request, exc: RequestValidationError) -> JSONResponse:
        """Say exactly which field is wrong and why.

        A sender whose lead was rejected has to be able to fix it without reading our
        source, so the response names the field and the reason rather than echoing a
        generic 'unprocessable entity'.
        """
        details = [
            ErrorDetail(
                field=".".join(str(part) for part in error["loc"][1:]) or "body",
                message=error["msg"],
            )
            for error in exc.errors()
        ]
        body = ErrorResponse(
            error="validation_error",
            message="The lead was not accepted: "
            + "; ".join(f"{d.field}: {d.message}" for d in details),
            details=details,
        )
        return JSONResponse(status_code=422, content=body.model_dump())

    return app


app = create_app()
