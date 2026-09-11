"""Recipient address handling.

A recipient in this service is an HTTP(S) endpoint. Two things matter about its URL:
it must be something we can actually POST to, and we need a stable *origin* for it,
because fairness and failure are properties of a host, not of a path.
"""

from __future__ import annotations

from urllib.parse import urlsplit

ALLOWED_SCHEMES = ("http", "https")
DEFAULT_PORTS = {"http": 80, "https": 443}


class InvalidRecipientUrl(ValueError):
    """The address is not a usable HTTP endpoint."""


def normalize_url(raw: str) -> str:
    """Validate a recipient URL and return it trimmed.

    Deliberately does not rewrite the URL beyond stripping whitespace: what the sender
    gave us is what the recipient expects to see in their access log.
    """
    url = (raw or "").strip()
    if not url:
        raise InvalidRecipientUrl("recipient url must not be empty")
    if "\x00" in url:
        raise InvalidRecipientUrl("recipient url must not contain a null byte")

    parts = urlsplit(url)
    if parts.scheme not in ALLOWED_SCHEMES:
        raise InvalidRecipientUrl(
            f"recipient url must use one of {', '.join(ALLOWED_SCHEMES)}; "
            f"got '{parts.scheme or url}'"
        )
    if not parts.hostname:
        raise InvalidRecipientUrl(f"recipient url has no host: '{url}'")
    try:
        parts.port  # noqa: B018 - raises ValueError on a malformed port
    except ValueError as exc:
        raise InvalidRecipientUrl(f"recipient url has an invalid port: '{url}'") from exc

    return url


def origin_of(url: str) -> str:
    """scheme://host:port, with the default port made explicit.

    Used as the fairness key: every URL on one host shares one budget of concurrent
    delivery slots, so a single dead CRM cannot starve everybody else.
    """
    parts = urlsplit(url)
    host = (parts.hostname or "").lower()
    port = parts.port or DEFAULT_PORTS[parts.scheme]
    return f"{parts.scheme}://{host}:{port}"
