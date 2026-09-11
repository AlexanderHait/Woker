"""HTTP request/response contracts.

One incoming format, as allowed by the brief: JSON. The intake model is deliberately
strict about the fields the *service* needs and deliberately permissive about the lead
itself - see `IntakeRequest.payload`.
"""

from __future__ import annotations

import json
from datetime import datetime
from enum import StrEnum
from typing import Any
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from app.config import get_settings
from app.recipients import InvalidRecipientUrl, normalize_url


def _contains_null_byte(value: Any) -> bool:
    """True if any string anywhere in the structure contains U+0000.

    PostgreSQL cannot store a NUL byte in a text or jsonb value, so without this check
    a lead carrying one (mis-encoded form input, a bad paste) would blow up on INSERT
    and come back as a 500 with nothing useful in it. It is a malformed lead, and the
    sender deserves to be told that in the same shape as any other validation error.
    """
    if isinstance(value, str):
        return "\x00" in value
    if isinstance(value, dict):
        return any(_contains_null_byte(k) or _contains_null_byte(v) for k, v in value.items())
    if isinstance(value, (list, tuple)):
        return any(_contains_null_byte(item) for item in value)
    return False


# ---------------------------------------------------------------------------
# Intake
# ---------------------------------------------------------------------------
class RecipientIn(BaseModel):
    model_config = ConfigDict(extra="forbid")

    url: str = Field(description="HTTP(S) endpoint the lead is POSTed to.")
    name: str | None = Field(
        default=None,
        max_length=200,
        description="Optional human label ('crm', 'sales-telegram'). Purely for the UI of "
        "whoever reads /v1/problems at 9am.",
    )

    @field_validator("url")
    @classmethod
    def _validate_url(cls, value: str) -> str:
        try:
            return normalize_url(value)
        except InvalidRecipientUrl as exc:
            raise ValueError(str(exc)) from exc

    @field_validator("name")
    @classmethod
    def _strip_name(cls, value: str | None) -> str | None:
        if value is None:
            return None
        if "\x00" in value:
            raise ValueError("must not contain a null byte")
        stripped = value.strip()
        return stripped or None


class IntakeRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    source_id: str = Field(
        min_length=1,
        max_length=200,
        description="Where the lead came from: site, quiz, partner.",
    )
    idempotency_key: str = Field(
        min_length=1,
        max_length=200,
        description="Sender-chosen key identifying this lead. Re-sending the same "
        "(source_id, idempotency_key) returns the original request instead of "
        "creating a second one.",
    )
    payload: dict[str, Any] = Field(
        description="The lead itself. No fixed schema on purpose - the set of fields "
        "differs per source and changes over time. Must be a non-empty object."
    )
    recipients: list[RecipientIn] = Field(
        default_factory=list,
        description="Where to deliver. May be empty: a lead whose address was never "
        "configured is still accepted, stored, and surfaced in /v1/problems.",
    )

    @field_validator("source_id", "idempotency_key")
    @classmethod
    def _strip_required(cls, value: str) -> str:
        stripped = value.strip()
        if not stripped:
            raise ValueError("must not be blank")
        if "\x00" in stripped:
            raise ValueError("must not contain a null byte")
        return stripped

    @field_validator("payload")
    @classmethod
    def _payload_not_empty(cls, value: dict[str, Any]) -> dict[str, Any]:
        if not value:
            raise ValueError("payload must contain at least one field")
        if _contains_null_byte(value):
            raise ValueError("payload must not contain a null byte (U+0000); it cannot be stored")
        return value

    @model_validator(mode="after")
    def _check_limits(self) -> IntakeRequest:
        settings = get_settings()

        size = len(json.dumps(self.payload, ensure_ascii=False).encode("utf-8"))
        if size > settings.max_payload_bytes:
            raise ValueError(f"payload is {size} bytes, limit is {settings.max_payload_bytes}")

        if len(self.recipients) > settings.max_recipients_per_request:
            raise ValueError(
                f"{len(self.recipients)} recipients given, limit is "
                f"{settings.max_recipients_per_request}"
            )
        return self


class IntakeAccepted(BaseModel):
    request_id: UUID
    duplicate: bool = Field(
        description="True when this key was already accepted. The response is still a "
        "success: the sender did nothing wrong and must not retry."
    )
    received_at: datetime
    recipient_count: int
    has_recipients: bool = Field(
        description="False means the lead was accepted but has nowhere to go; it is "
        "listed in /v1/problems until recipients are attached."
    )


# ---------------------------------------------------------------------------
# Status
# ---------------------------------------------------------------------------
class DeliveryState(StrEnum):
    pending = "pending"
    in_flight = "in_flight"
    delivered = "delivered"
    failed = "failed"


class RequestState(StrEnum):
    no_recipients = "no_recipients"
    in_progress = "in_progress"
    delivered = "delivered"
    partially_delivered = "partially_delivered"
    failed = "failed"


class AttemptOut(BaseModel):
    attempt_number: int = Field(
        description="Position in this delivery's whole history; never reused."
    )
    budget_attempt: int = Field(
        description="Position within the retry budget running at the time. Lower than "
        "attempt_number once someone has pressed retry, which starts the budget over."
    )
    started_at: datetime
    finished_at: datetime
    duration_ms: int
    outcome: str = Field(
        description="'unknown' means the worker died mid-attempt: the recipient may or "
        "may not have received that copy."
    )
    error_kind: str | None = None
    status_code: int | None = None
    response_excerpt: str | None = None
    scheduled_next_at: datetime | None = Field(
        default=None, description="When the following attempt was scheduled - the pause."
    )
    worker_id: str | None = None


class DeliveryOut(BaseModel):
    delivery_id: UUID
    recipient_name: str | None
    recipient_url: str
    status: DeliveryState
    attempts: int = Field(
        description="Attempts in the current budget. A manual retry resets this to 0."
    )
    total_attempts: int = Field(
        description="Attempts ever made. Never reset, so it matches the journal."
    )
    max_attempts: int
    next_attempt_at: datetime | None
    last_attempt_at: datetime | None
    last_outcome: str | None
    last_error_kind: str | None
    last_status_code: int | None
    last_error: str | None
    delivered_at: datetime | None
    failed_at: datetime | None
    attempt_log: list[AttemptOut] = Field(default_factory=list)


class RequestStatus(BaseModel):
    request_id: UUID
    source_id: str
    idempotency_key: str
    received_at: datetime
    state: RequestState
    payload: dict[str, Any]
    recipients: list[DeliveryOut]


# ---------------------------------------------------------------------------
# Problems
# ---------------------------------------------------------------------------
class ProblemReason(StrEnum):
    no_recipients = "no_recipients"
    failed = "failed"
    stalled = "stalled"


class ProblemItem(BaseModel):
    reason: ProblemReason
    request_id: UUID
    source_id: str
    received_at: datetime
    age_seconds: float
    delivery_id: UUID | None = None
    recipient_name: str | None = None
    recipient_url: str | None = None
    status: DeliveryState | None = None
    attempts: int | None = None
    next_attempt_at: datetime | None = None
    last_error_kind: str | None = None
    last_status_code: int | None = None
    last_error: str | None = None


class ProblemsResponse(BaseModel):
    generated_at: datetime
    stale_after_seconds: int
    counts: dict[str, int]
    total: int
    limit: int
    offset: int
    items: list[ProblemItem]


# ---------------------------------------------------------------------------
# Stats
# ---------------------------------------------------------------------------
class StatsResponse(BaseModel):
    period_from: datetime
    period_to: datetime
    requests_accepted: int
    requests_without_recipients: int
    deliveries_total: int
    deliveries_delivered: int
    deliveries_queued: int = Field(description="pending + in_flight")
    deliveries_failed: int
    attempts_total: int
    attempts_failed: int


# ---------------------------------------------------------------------------
# Retry
# ---------------------------------------------------------------------------
class RetryRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    recipients: list[str] | None = Field(
        default=None,
        description="Recipient URLs or names to requeue. Omit for all of them.",
    )
    include_delivered: bool = Field(
        default=False,
        description="Off by default: re-sending a lead that already arrived creates the "
        "duplicate call to the customer that we are trying to avoid.",
    )


class BulkRetryRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    status: list[DeliveryState] = Field(
        default_factory=lambda: [DeliveryState.failed],
        description="Which delivery states to requeue. Default: everything in 'failed'.",
    )
    recipient_origin: str | None = Field(
        default=None,
        description="Limit to one recipient host, e.g. 'https://crm.example.com:443'. "
        "This is the 'the CRM is back up, flush what piled up' button.",
    )
    recipient_url_contains: str | None = Field(default=None, max_length=500)
    received_after: datetime | None = None
    received_before: datetime | None = None
    limit: int = Field(default=1000, gt=0, le=100_000)


class RetryResponse(BaseModel):
    requeued: int
    skipped_in_flight: int = Field(
        description="Deliveries a worker is sending right now; left alone so we do not "
        "race with an attempt already on the wire."
    )
    skipped_delivered: int = 0
    delivery_ids: list[UUID] = Field(default_factory=list)


class AttachRecipientsRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    recipients: list[RecipientIn] = Field(min_length=1)


class AttachRecipientsResponse(BaseModel):
    request_id: UUID
    added: int
    already_present: int
    delivery_ids: list[UUID] = Field(default_factory=list)


# ---------------------------------------------------------------------------
# Errors
# ---------------------------------------------------------------------------
class ErrorDetail(BaseModel):
    field: str
    message: str


class ErrorResponse(BaseModel):
    error: str
    message: str
    details: list[ErrorDetail] = Field(default_factory=list)
