# One image runs all three processes (API, worker, stub); the command decides which.
# Keeping them in one image means they can never drift out of sync with each other.

FROM python:3.12-slim AS base

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /srv

# Dependencies first, so editing source does not reinstall them.
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY migrations ./migrations
COPY app ./app
COPY stub ./stub

EXPOSE 8000
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]


# Adds the test dependencies and the suite. Used by `make test`.
FROM base AS dev

COPY requirements-dev.txt ./
RUN pip install --no-cache-dir -r requirements-dev.txt

COPY pyproject.toml ./
COPY tests ./tests
# The suite checks that the commands in the README still work, so it needs the README.
COPY README.md ./

CMD ["pytest", "-v"]
