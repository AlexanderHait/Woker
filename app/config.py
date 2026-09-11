"""Runtime configuration.

Everything tunable lives here and comes from the environment, so the defaults in
this file are the documented behaviour of the service. `Settings.validate_invariants`
is called once at startup: a configuration that could silently corrupt the delivery
guarantees fails the process instead of degrading in production.
"""

from __future__ import annotations

from functools import lru_cache

from pydantic import Field, PostgresDsn, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class ConfigurationError(RuntimeError):
    """Raised at startup when settings violate an invariant of the delivery engine."""


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="INTAKE_", env_file=".env", extra="ignore")

    # --- storage -------------------------------------------------------------
    database_url: PostgresDsn = Field(
        default="postgresql://intake:intake@localhost:5432/intake",
        description="PostgreSQL DSN. Also used for the queue - there is no external broker.",
    )
    db_pool_min_size: int = Field(default=2, ge=1)
    db_pool_max_size: int = Field(default=10, ge=1)
    db_command_timeout_seconds: float = Field(default=10.0, gt=0)

    # --- intake --------------------------------------------------------------
    max_payload_bytes: int = Field(
        default=256 * 1024,
        gt=0,
        description="Upper bound on the serialised `payload` object, to keep rows sane.",
    )
    max_recipients_per_request: int = Field(default=50, gt=0)

    # --- delivery attempt timeouts -------------------------------------------
    # A recipient that accepts the connection and stays silent must never pin a worker.
    connect_timeout_seconds: float = Field(default=5.0, gt=0)
    read_timeout_seconds: float = Field(
        default=30.0,
        gt=0,
        description=(
            "Generous on purpose: a recipient answering in 20s is slow but alive. "
            "Cutting it off early would manufacture duplicate leads, which the brief "
            "treats as expensive as losing them."
        ),
    )
    write_timeout_seconds: float = Field(default=5.0, gt=0)
    # Hard wall-clock ceiling for one attempt, independent of the per-phase timeouts above.
    attempt_deadline_seconds: float = Field(default=45.0, gt=0)

    # --- retry policy --------------------------------------------------------
    # Defaults produce: 5s, 10s, 20s, 40s, 80s, 160s, 320s, 640s, 1280s, 2560s, then hourly.
    # 34 attempts span ~24.4h of recipient downtime; the first 10 land inside 85 minutes,
    # so a short blip recovers fast and a day-long outage is still survived.
    retry_base_seconds: float = Field(default=5.0, gt=0)
    retry_factor: float = Field(default=2.0, ge=1.0)
    retry_cap_seconds: float = Field(default=3600.0, gt=0)
    retry_max_attempts: int = Field(default=34, ge=1)
    retry_jitter_ratio: float = Field(
        default=0.2,
        ge=0.0,
        lt=1.0,
        description="Full-interval jitter, so recipients recovering from an outage are not "
        "hit by every queued delivery in the same millisecond.",
    )

    # --- worker --------------------------------------------------------------
    worker_concurrency: int = Field(
        default=32,
        gt=0,
        description="Deliveries in flight per worker process. A slow recipient occupies one "
        "slot; it cannot stall the others because sending is fully async.",
    )
    worker_batch_size: int = Field(default=16, gt=0)
    worker_poll_interval_seconds: float = Field(default=0.25, gt=0)
    worker_shutdown_grace_seconds: float = Field(default=20.0, ge=0)
    lease_seconds: float = Field(
        default=90.0,
        gt=0,
        description="How long a claimed delivery stays owned by a worker. After a hard kill "
        "the row becomes claimable again once the lease expires.",
    )
    max_claims_per_origin: int = Field(
        default=4,
        gt=0,
        description="Per-batch cap on deliveries claimed for one recipient host. This is what "
        "stops a single dead or slow recipient from occupying the whole worker.",
    )
    claim_scan_multiplier: int = Field(
        default=5,
        ge=1,
        description="How many rows the claim query inspects per batch before applying the "
        "per-origin cap. Higher values find work for idle origins further down the queue.",
    )

    # --- visibility ----------------------------------------------------------
    default_stale_minutes: int = Field(
        default=15,
        gt=0,
        description="Undelivered for longer than this shows up in /v1/problems.",
    )
    response_excerpt_bytes: int = Field(
        default=2000,
        gt=0,
        description="How much of a recipient's error response is kept in the journal.",
    )

    # --- misc ----------------------------------------------------------------
    user_agent: str = Field(default="intake/1.0")
    log_level: str = Field(default="INFO")

    @field_validator("log_level")
    @classmethod
    def _upper(cls, value: str) -> str:
        return value.upper()

    @property
    def dsn(self) -> str:
        return str(self.database_url)

    def validate_invariants(self) -> None:
        """Fail fast on settings that would quietly break delivery guarantees."""
        errors: list[str] = []

        # If a lease can expire while an attempt is still running, a second worker would
        # pick up the same delivery and the recipient would get two copies concurrently -
        # exactly what 2.3.8 forbids.
        if self.lease_seconds <= self.attempt_deadline_seconds:
            errors.append(
                f"lease_seconds ({self.lease_seconds}) must exceed attempt_deadline_seconds "
                f"({self.attempt_deadline_seconds}), otherwise a delivery could be claimed "
                f"twice while the first attempt is still in flight."
            )

        # The per-phase timeouts must fit inside the hard deadline, or the deadline would
        # abort attempts that the HTTP client still considers healthy.
        phase_budget = self.connect_timeout_seconds + self.read_timeout_seconds
        if phase_budget > self.attempt_deadline_seconds:
            errors.append(
                f"connect_timeout_seconds + read_timeout_seconds ({phase_budget}) must not "
                f"exceed attempt_deadline_seconds ({self.attempt_deadline_seconds})."
            )

        if self.worker_batch_size > self.worker_concurrency:
            errors.append(
                f"worker_batch_size ({self.worker_batch_size}) must not exceed "
                f"worker_concurrency ({self.worker_concurrency}); the worker would claim "
                f"more deliveries than it can start."
            )

        if self.db_pool_max_size < self.db_pool_min_size:
            errors.append("db_pool_max_size must be >= db_pool_min_size.")

        if errors:
            raise ConfigurationError(
                "Invalid configuration:\n" + "\n".join(f"  - {e}" for e in errors)
            )


@lru_cache
def get_settings() -> Settings:
    settings = Settings()
    settings.validate_invariants()
    return settings
