"""What counts as delivered, and what the recipient actually receives."""

from __future__ import annotations

from datetime import UTC, datetime
from uuid import uuid4

import pytest

from app.config import Settings
from app.queue import ClaimedDelivery
from app.sender import build_body, build_headers, classify_status


@pytest.fixture
def claim() -> ClaimedDelivery:
    return ClaimedDelivery(
        delivery_id=uuid4(),
        request_id=uuid4(),
        recipient_name="crm",
        recipient_url="https://crm.example.com/hook",
        recipient_origin="https://crm.example.com:443",
        budget_attempt=2,
        attempt_number=5,
        source_id="landing",
        idempotency_key="lead-42",
        received_at=datetime(2026, 1, 1, 12, 0, tzinfo=UTC),
        payload={"name": "Иван", "phone": "+7 900 000-00-00"},
    )


@pytest.mark.parametrize("status_code", [200, 201, 202, 204, 299])
def test_any_2xx_is_a_delivery(status_code: int) -> None:
    assert classify_status(status_code) is None


@pytest.mark.parametrize(
    ("status_code", "kind"),
    [
        (301, "http_redirect"),
        (302, "http_redirect"),
        (400, "http_client_error"),
        (401, "http_client_error"),
        (404, "http_client_error"),
        (408, "http_retryable"),
        (429, "http_retryable"),
        (500, "http_server_error"),
        (502, "http_server_error"),
        (503, "http_server_error"),
    ],
)
def test_everything_else_is_a_failure_with_a_useful_label(status_code: int, kind: str) -> None:
    """The label does not change the retry policy - the brief says every non-2xx is a
    failure - but it tells whoever reads /v1/problems whether to wait or to go fix an
    address."""
    assert classify_status(status_code) == kind


def test_the_recipient_gets_a_stable_key_for_dropping_duplicates(
    claim: ClaimedDelivery,
) -> None:
    """The whole at-least-once story rests on this header: it identifies the (lead,
    recipient) pair and does not change between retries."""
    headers = build_headers(claim, Settings())

    assert headers["Idempotency-Key"] == str(claim.delivery_id)
    assert headers["X-Intake-Request-Id"] == str(claim.request_id)
    # The attempt counter does change, so a recipient can tell a repeat from the first try.
    assert headers["X-Intake-Attempt"] == "5"
    assert headers["Content-Type"] == "application/json"


def test_the_key_is_the_same_on_every_attempt(claim: ClaimedDelivery) -> None:
    settings = Settings()
    later = ClaimedDelivery(**{**claim.__dict__, "attempt_number": 9, "budget_attempt": 4})

    assert (
        build_headers(claim, settings)["Idempotency-Key"]
        == build_headers(later, settings)["Idempotency-Key"]
    )


def test_the_body_carries_the_lead_untouched(claim: ClaimedDelivery) -> None:
    body = build_body(claim)

    assert body["payload"] == {"name": "Иван", "phone": "+7 900 000-00-00"}
    assert body["source_id"] == "landing"
    assert body["idempotency_key"] == "lead-42"
    assert body["delivery_id"] == str(claim.delivery_id)
    assert body["attempt"] == 5
    assert body["received_at"] == "2026-01-01T12:00:00+00:00"
