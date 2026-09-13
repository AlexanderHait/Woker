"""The README has to stay executable.

Every `curl` in it is a command someone will copy verbatim, so one that no longer works is
a broken instruction rather than a typo - and this is exactly how a reader meets the
project. So the README is parsed here and each documented request is checked against the
very models the service validates with: a missing `Content-Type`, a renamed field, a mode
that no longer exists, all fail in this file in under a second.

Nothing is sent anywhere and nothing needs to be running; the bodies are validated
in-process.
"""

from __future__ import annotations

import json
import re
import shlex
from pathlib import Path

from pydantic import BaseModel, ValidationError

from app.schemas import AttachRecipientsRequest, BulkRetryRequest, IntakeRequest, RetryRequest
from stub.main import Behaviour

README = Path(__file__).resolve().parent.parent / "README.md"
JSON_HEADER = "Content-Type: application/json"

# Which model validates the body each documented command sends. The URL is matched against
# these in order, so the more specific patterns come first.
BODY_MODELS: tuple[tuple[str, type[BaseModel]], ...] = (
    (r"/control/", Behaviour),
    (r"/v1/requests/[^/\s]+/retry", RetryRequest),
    (r"/v1/requests/[^/\s]+/recipients", AttachRecipientsRequest),
    (r"/v1/deliveries/retry", BulkRetryRequest),
    (r"/v1/requests", IntakeRequest),
)


def commands() -> list[str]:
    """Every curl command in the README, with wrapped lines joined back together.

    A command continues onto the next line either because it ends in a backslash or
    because a quote is still open - a JSON body spanning several lines. Lines are joined
    with a newline, which is whitespace to the shell and to JSON alike.
    """
    found: list[str] = []
    pending = ""
    for raw in README.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not pending and not line.startswith("curl"):
            continue
        pending = f"{pending}\n{line}" if pending else line
        if pending.endswith("\\"):
            pending = pending[:-1]
            continue
        try:
            shlex.split(pending)
        except ValueError:
            continue  # an unclosed quote: the body carries on below
        found.append(pending)
        pending = ""
    assert not pending, f"a curl command in the README is never terminated: {pending!r}"
    return found


def body_and_headers(command: str) -> tuple[str | None, list[str]]:
    tokens = shlex.split(command)
    body: str | None = None
    headers: list[str] = []
    for flag, value in zip(tokens, tokens[1:], strict=False):
        if flag in ("-d", "--data", "--data-raw", "--data-binary"):
            body = value
        elif flag in ("-H", "--header"):
            headers.append(value)
    return body, headers


def model_for(command: str) -> type[BaseModel] | None:
    for pattern, model in BODY_MODELS:
        if re.search(pattern, command):
            return model
    return None


def shorten(command: str) -> str:
    flat = " ".join(command.split())
    return flat if len(flat) <= 110 else f"{flat[:107]}..."


def with_bodies() -> list[tuple[str, str]]:
    pairs = [(c, body_and_headers(c)[0]) for c in commands()]
    return [(c, b) for c, b in pairs if b is not None]


def test_every_documented_request_actually_sends_json() -> None:
    """`curl -d` without the header sends a form, and the service answers 422.

    The header has to be on the same line as the body, because that is the unit a reader
    copies - one that inherits it from the command above only works by luck.
    """
    broken: list[str] = []
    for command, body in with_bodies():
        _, headers = body_and_headers(command)
        if JSON_HEADER not in headers:
            broken.append(f"no '{JSON_HEADER}' header, so this answers 422: {shorten(command)}")
            continue
        try:
            json.loads(body)
        except json.JSONDecodeError as exc:
            broken.append(f"body is not valid JSON ({exc}): {shorten(command)}")

    assert not broken, "documented commands that do not work as written:\n" + "\n".join(broken)


def test_every_documented_body_is_accepted_by_the_model_that_validates_it() -> None:
    """Catches the README drifting away from the schemas: a renamed field, a dropped mode."""
    rejected: list[str] = []
    checked = 0
    for command, body in with_bodies():
        model = model_for(command)
        if model is None:
            continue
        checked += 1
        try:
            model.model_validate(json.loads(body))
        except ValidationError as exc:
            rejected.append(f"{model.__name__} rejects `{shorten(command)}`:\n{exc}")

    assert not rejected, "\n".join(rejected)
    # Guard rail: if the parser ever stops finding the commands, the checks above pass
    # trivially and this file becomes decoration.
    assert checked >= 7, f"only {checked} documented bodies were checked - parser broken?"


def test_the_readme_shows_how_to_ask_the_stub_for_each_behaviour() -> None:
    """The stub exists so a reviewer can reproduce the awkward cases by hand, which means
    all four behaviours have to be written down, not just supported."""
    documented = {
        json.loads(body)["mode"] for command, body in with_bodies() if "/control/" in command
    }

    assert documented >= {"ok", "error", "silent", "slow"}, (
        f"the README only shows {sorted(documented)}; a reader cannot reproduce the rest"
    )
