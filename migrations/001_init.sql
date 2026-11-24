CREATE TABLE jobs (
    id UUID PRIMARY KEY,
    name TEXT NOT NULL,
    webhook_url TEXT NOT NULL,
    payload JSONB NOT NULL DEFAULT '{}',
    cron_expr TEXT,
    status TEXT NOT NULL,
    attempt INT NOT NULL DEFAULT 0,
    max_retries INT NOT NULL DEFAULT 3,
    scheduled_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE job_executions (
    id UUID PRIMARY KEY,
    job_id UUID NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    worker_id TEXT NOT NULL,
    attempt INT NOT NULL,
    status TEXT NOT NULL,
    response_code INT,
    error_message TEXT,
    started_at TIMESTAMPTZ NOT NULL,
    finished_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE outbox (
    id BIGSERIAL PRIMARY KEY,
    job_id UUID NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    scheduled_at BIGINT NOT NULL,
    processed BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_outbox_unprocessed ON outbox(id) WHERE processed = FALSE;
CREATE INDEX idx_jobs_status_scheduled ON jobs(status, scheduled_at);