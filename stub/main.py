"""Recipient stub: a fake CRM you can break on purpose.

Every recipient is a *named endpoint*: POST /hook/{name}. A name needs no setup - an
unconfigured endpoint answers 200 - and its behaviour is changed at runtime through
PUT /control/{name}:

    {"mode": "ok"}                                 always 200
    {"mode": "error", "status_code": 500}          always fails
    {"mode": "error", "fail_first": 3}             fails 3 times, then works
    {"mode": "silent"}                             accepts the connection, never answers
    {"mode": "slow", "delay_seconds": 30}          answers, eventually

Everything it receives is recorded, so a test can assert not only that a lead arrived
but that it arrived *once*: GET /received/{name}/summary reports how many bodies came
in and how many distinct Idempotency-Keys they carried.

State is in memory on purpose - restarting the stub is the reset button.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from typing import Any, Literal

from fastapi import FastAPI, Header, HTTPException, Request
from pydantic import BaseModel, ConfigDict, Field

from app.logging_config import configure_logging

# Long enough to outlast any sane client timeout; the point is that we never answer.
SILENT_HOLD_SECONDS = 3600.0


class Behaviour(BaseModel):
    model_config = ConfigDict(extra="forbid")

    mode: Literal["ok", "error", "silent", "slow"] = "ok"
    status_code: int = Field(default=500, ge=100, le=599)
    delay_seconds: float = Field(default=30.0, ge=0)
    fail_first: int | None = Field(
        default=None,
        ge=0,
        description="Apply `mode` to the first N requests only, then answer 200. "
        "Omit to apply it to every request.",
    )


class ReceivedRecord(BaseModel):
    received_at: datetime
    idempotency_key: str | None
    request_id: str | None
    attempt: int | None
    responded_with: int | str
    body: dict[str, Any] | None


class EndpointState:
    def __init__(self) -> None:
        self.behaviour = Behaviour()
        self.received: list[ReceivedRecord] = []
        self.handled = 0


@asynccontextmanager
async def lifespan(_: FastAPI) -> AsyncIterator[None]:
    configure_logging("INFO")
    yield


app = FastAPI(
    title="Recipient stub",
    version="1.0.0",
    summary="A controllable fake recipient for exercising the intake service.",
    lifespan=lifespan,
)

_endpoints: dict[str, EndpointState] = {}


def _state(name: str) -> EndpointState:
    return _endpoints.setdefault(name, EndpointState())


# ---------------------------------------------------------------------------
# The receiving side
# ---------------------------------------------------------------------------
@app.post("/hook/{name}", summary="Receive a lead")
async def receive(
    name: str,
    request: Request,
    idempotency_key: str | None = Header(default=None, alias="Idempotency-Key"),
    intake_request_id: str | None = Header(default=None, alias="X-Intake-Request-Id"),
    intake_attempt: int | None = Header(default=None, alias="X-Intake-Attempt"),
):
    state = _state(name)
    behaviour = state.behaviour

    try:
        body = await request.json()
    except Exception:  # noqa: BLE001 - record whatever arrived, even if it is not JSON
        body = None

    # The sequence number of this request decides whether `fail_first` still applies.
    # Captured before any await so concurrent requests get distinct numbers.
    state.handled += 1
    ordinal = state.handled

    misbehaving = behaviour.fail_first is None or ordinal <= behaviour.fail_first
    mode = behaviour.mode if misbehaving else "ok"

    def record(responded_with: int | str) -> None:
        state.received.append(
            ReceivedRecord(
                received_at=datetime.now(UTC),
                idempotency_key=idempotency_key,
                request_id=intake_request_id,
                attempt=intake_attempt,
                responded_with=responded_with,
                body=body,
            )
        )

    if mode == "silent":
        # Accept the connection and hold it. The caller's read timeout has to save it.
        record("silent")
        await asyncio.sleep(SILENT_HOLD_SECONDS)
        return {"ok": True, "note": "this should never be reached"}

    if mode == "slow":
        await asyncio.sleep(behaviour.delay_seconds)
        record(200)
        return {"ok": True, "slow": True}

    if mode == "error":
        record(behaviour.status_code)
        raise HTTPException(
            status_code=behaviour.status_code,
            detail=f"stub '{name}' is failing on purpose (request #{ordinal})",
        )

    record(200)
    return {"ok": True}


# ---------------------------------------------------------------------------
# Control
# ---------------------------------------------------------------------------
@app.put("/control/{name}", summary="Set how this endpoint behaves")
async def set_behaviour(name: str, behaviour: Behaviour) -> dict:
    state = _state(name)
    state.behaviour = behaviour
    # `fail_first` counts from the moment it is set, otherwise "fail the next 3" would
    # mean something different depending on how much traffic the endpoint saw before.
    state.handled = 0
    return {"name": name, "behaviour": behaviour.model_dump()}


@app.get("/control/{name}", summary="Current behaviour and traffic")
async def get_behaviour(name: str) -> dict:
    state = _state(name)
    return {
        "name": name,
        "behaviour": state.behaviour.model_dump(),
        "handled_since_configured": state.handled,
        "received_total": len(state.received),
    }


@app.get("/received/{name}", summary="Everything this endpoint received")
async def get_received(name: str, limit: int = 200) -> dict:
    state = _state(name)
    return {
        "name": name,
        "total": len(state.received),
        "items": [r.model_dump(mode="json") for r in state.received[-limit:]],
    }


@app.get(
    "/received/{name}/summary",
    summary="Counts, including how many distinct deliveries arrived",
)
async def get_summary(name: str) -> dict:
    """`total` vs `unique_idempotency_keys` is the duplicate check.

    They are equal when every lead arrived exactly once. `total` larger than
    `unique_idempotency_keys` means at least one lead was delivered more than once -
    which the recipient could have discarded, since the key is what identifies it.
    """
    state = _state(name)
    keys = [r.idempotency_key for r in state.received if r.idempotency_key]
    request_ids = {r.request_id for r in state.received if r.request_id}
    return {
        "name": name,
        "total": len(state.received),
        "unique_idempotency_keys": len(set(keys)),
        "unique_request_ids": len(request_ids),
        "duplicates": len(keys) - len(set(keys)),
        "by_status": _count_by_status(state),
    }


def _count_by_status(state: EndpointState) -> dict[str, int]:
    counts: dict[str, int] = {}
    for record in state.received:
        key = str(record.responded_with)
        counts[key] = counts.get(key, 0) + 1
    return counts


@app.delete("/received/{name}", summary="Forget what this endpoint received")
async def clear_received(name: str) -> dict:
    state = _state(name)
    count = len(state.received)
    state.received.clear()
    state.handled = 0
    return {"name": name, "cleared": count}


@app.get("/", summary="Endpoints the stub knows about")
async def index() -> dict:
    return {
        "endpoints": {
            name: {
                "mode": state.behaviour.mode,
                "received": len(state.received),
            }
            for name, state in sorted(_endpoints.items())
        }
    }


@app.get("/healthz")
async def healthz() -> dict:
    return {"status": "ok"}
