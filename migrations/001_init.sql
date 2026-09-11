-- Initial schema.
--
-- Three tables:
--   requests          - what we accepted, one row per incoming lead (dedup lives here)
--   deliveries        - the queue itself, one row per (request, recipient) pair
--   delivery_attempts - append-only journal, one row per HTTP attempt ever made
--
-- There is no external broker: `deliveries` is the queue, claimed with
-- SELECT ... FOR UPDATE SKIP LOCKED and protected by a lease so that a worker
-- killed mid-flight releases its work automatically.

CREATE TYPE delivery_status AS ENUM ('pending', 'in_flight', 'delivered', 'failed');

CREATE TYPE attempt_outcome AS ENUM ('success', 'failure', 'unknown');


-- ---------------------------------------------------------------------------
-- requests
-- ---------------------------------------------------------------------------
CREATE TABLE requests (
    id              UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    source_id       TEXT        NOT NULL,
    idempotency_key TEXT        NOT NULL,
    -- No fixed schema for the lead itself: the composition of fields changes per source.
    payload         JSONB       NOT NULL,
    received_at     TIMESTAMPTZ NOT NULL DEFAULT now(),

    -- Deduplication is scoped to the source: two partners may legitimately both
    -- send their own key "1" without colliding.
    CONSTRAINT requests_idempotency_unique UNIQUE (source_id, idempotency_key)
);

CREATE INDEX requests_received_at_idx ON requests (received_at DESC);


-- ---------------------------------------------------------------------------
-- deliveries  (the queue)
-- ---------------------------------------------------------------------------
CREATE TABLE deliveries (
    id               UUID            PRIMARY KEY DEFAULT gen_random_uuid(),
    request_id       UUID            NOT NULL REFERENCES requests (id) ON DELETE CASCADE,

    recipient_name   TEXT,
    recipient_url    TEXT            NOT NULL,
    -- scheme://host:port of recipient_url. Fairness and blast-radius are per host:
    -- when a CRM is down, every URL on that host is down with it.
    recipient_origin TEXT            NOT NULL,

    status           delivery_status NOT NULL DEFAULT 'pending',
    -- Attempts in the *current* budget. A manual retry resets this, which is what
    -- "try again from scratch, the CRM is fixed" means.
    attempts         INTEGER         NOT NULL DEFAULT 0,
    -- Attempts ever made. Never resets, so it can number journal entries uniquely for
    -- the whole life of the delivery even across manual retries.
    total_attempts   INTEGER         NOT NULL DEFAULT 0,

    next_attempt_at  TIMESTAMPTZ     NOT NULL DEFAULT now(),
    -- Set while status = 'in_flight'. Once it passes, another worker may take the row:
    -- this is what makes a `kill -9` mid-delivery recoverable without any cleanup daemon.
    lease_expires_at TIMESTAMPTZ,
    locked_by        TEXT,

    last_attempt_at  TIMESTAMPTZ,
    last_outcome     attempt_outcome,
    last_error_kind  TEXT,
    last_status_code INTEGER,
    last_error       TEXT,

    delivered_at     TIMESTAMPTZ,
    failed_at        TIMESTAMPTZ,

    created_at       TIMESTAMPTZ     NOT NULL DEFAULT now(),
    updated_at       TIMESTAMPTZ     NOT NULL DEFAULT now(),

    -- Derived by trigger, never written by the application. Lets the claim query use a
    -- single ordered index scan instead of OR-ing "pending and due" with "lease expired".
    claimable_at     TIMESTAMPTZ     NOT NULL DEFAULT now(),

    -- The same recipient listed twice in one request is one delivery, not two.
    CONSTRAINT deliveries_request_recipient_unique UNIQUE (request_id, recipient_url)
);

-- The claim index. Partial, so delivered/failed rows (the vast majority over time)
-- never appear in it and the queue index stays proportional to outstanding work.
CREATE INDEX deliveries_claimable_idx
    ON deliveries (claimable_at)
    WHERE status IN ('pending', 'in_flight');

-- Supports the per-origin fairness window inside the claim query.
CREATE INDEX deliveries_origin_claimable_idx
    ON deliveries (recipient_origin, claimable_at)
    WHERE status IN ('pending', 'in_flight');

CREATE INDEX deliveries_request_idx ON deliveries (request_id);
CREATE INDEX deliveries_status_created_idx ON deliveries (status, created_at DESC);
-- Drives "stuck for longer than N minutes" in /v1/problems.
CREATE INDEX deliveries_unresolved_created_idx
    ON deliveries (created_at)
    WHERE status IN ('pending', 'in_flight');


-- claimable_at is maintained by the database so it cannot drift out of sync with
-- status: a pending row becomes claimable when its backoff elapses, an in-flight row
-- when its lease expires, and a settled row is not claimable at all.
CREATE FUNCTION deliveries_sync_claimable() RETURNS TRIGGER AS $$
BEGIN
    NEW.claimable_at := CASE NEW.status
        WHEN 'pending'   THEN NEW.next_attempt_at
        WHEN 'in_flight' THEN COALESCE(NEW.lease_expires_at, now())
        ELSE NEW.next_attempt_at
    END;
    NEW.updated_at := now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER deliveries_sync_claimable_trg
    BEFORE INSERT OR UPDATE ON deliveries
    FOR EACH ROW EXECUTE FUNCTION deliveries_sync_claimable();


-- ---------------------------------------------------------------------------
-- delivery_attempts  (the journal)
-- ---------------------------------------------------------------------------
-- Append-only. This is the table we use to prove after the fact that a lead was
-- (or was not) delivered at a given time.
CREATE TABLE delivery_attempts (
    id                BIGINT          GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    delivery_id       UUID            NOT NULL REFERENCES deliveries (id) ON DELETE CASCADE,
    -- Denormalised so the journal can be read per request without a join.
    request_id        UUID            NOT NULL REFERENCES requests (id) ON DELETE CASCADE,

    -- Position in the delivery's whole history (deliveries.total_attempts at the time).
    attempt_number    INTEGER         NOT NULL,
    -- Position within the retry budget that was running then. Differs from
    -- attempt_number once someone has pressed "retry", which resets the budget.
    budget_attempt    INTEGER         NOT NULL,
    recipient_url     TEXT            NOT NULL,

    started_at        TIMESTAMPTZ     NOT NULL,
    finished_at       TIMESTAMPTZ     NOT NULL,
    duration_ms       INTEGER         NOT NULL,

    outcome           attempt_outcome NOT NULL,
    error_kind        TEXT,
    status_code       INTEGER,
    response_excerpt  TEXT,

    -- When the next attempt was scheduled for. Makes the gaps between attempts
    -- visible in the journal without recomputing the backoff policy.
    scheduled_next_at TIMESTAMPTZ,
    worker_id         TEXT,

    CONSTRAINT delivery_attempts_number_unique UNIQUE (delivery_id, attempt_number)
);

CREATE INDEX delivery_attempts_request_idx ON delivery_attempts (request_id, started_at);
CREATE INDEX delivery_attempts_started_idx ON delivery_attempts (started_at);
