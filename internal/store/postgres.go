package store

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Job struct {
	ID          uuid.UUID       `json:"id"`
	Name        string          `json:"name"`
	WebhookURL  string          `json:"webhook_url"`
	Payload     json.RawMessage `json:"payload"`
	CronExpr    *string         `json:"cron_expr,omitempty"`
	Status      string          `json:"status"`
	Attempt     int             `json:"attempt"`
	MaxRetries  int             `json:"max_retries"`
	ScheduledAt time.Time       `json:"scheduled_at"`
	CreatedAt   time.Time       `json:"created_at"`
}

type PostgresStore struct {
	pool *pgxpool.Pool
}

func NewPostgresStore(pool *pgxpool.Pool) *PostgresStore {
	return &PostgresStore{pool: pool}
}

func (s *PostgresStore) CreateJobWithOutbox(ctx context.Context, job *Job) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback(ctx)

	_, err = tx.Exec(ctx, `
		INSERT INTO jobs (id, name, webhook_url, payload, cron_expr, status, max_retries, scheduled_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
	`, job.ID, job.Name, job.WebhookURL, job.Payload, job.CronExpr, "PENDING", job.MaxRetries, job.ScheduledAt)
	if err != nil {
		return fmt.Errorf("insert job: %w", err)
	}

	_, err = tx.Exec(ctx, `
		INSERT INTO outbox (job_id, scheduled_at)
		VALUES ($1, $2)
	`, job.ID, job.ScheduledAt.Unix())
	if err != nil {
		return fmt.Errorf("insert outbox: %w", err)
	}

	return tx.Commit(ctx)
}

func (s *PostgresStore) GetJobByID(ctx context.Context, id uuid.UUID) (*Job, error) {
	var job Job
	err := s.pool.QueryRow(ctx, `
		SELECT id, name, webhook_url, payload, cron_expr, status, attempt, max_retries, scheduled_at, created_at
		FROM jobs WHERE id = $1
	`, id).Scan(
		&job.ID, &job.Name, &job.WebhookURL, &job.Payload, &job.CronExpr,
		&job.Status, &job.Attempt, &job.MaxRetries, &job.ScheduledAt, &job.CreatedAt,
	)
	if err != nil {
		return nil, err
	}
	return &job, nil
}

func (s *PostgresStore) MarkJobRunning(ctx context.Context, id uuid.UUID) (*Job, error) {
	var job Job
	err := s.pool.QueryRow(ctx, `
		UPDATE jobs
		SET status = 'RUNNING', updated_at = NOW()
		WHERE id = $1
		RETURNING id, name, webhook_url, payload, cron_expr, status, attempt, max_retries, scheduled_at, created_at
	`, id).Scan(
		&job.ID, &job.Name, &job.WebhookURL, &job.Payload, &job.CronExpr,
		&job.Status, &job.Attempt, &job.MaxRetries, &job.ScheduledAt, &job.CreatedAt,
	)
	if err != nil {
		return nil, err
	}
	return &job, nil
}

func (s *PostgresStore) RecordExecutionAndComplete(
	ctx context.Context,
	jobID uuid.UUID,
	workerID string,
	attempt int,
	status string,
	respCode int,
	errMsg string,
	startedAt, finishedAt time.Time,
	nextRun *time.Time,
) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	_, err = tx.Exec(ctx, `
		INSERT INTO job_executions (id, job_id, worker_id, attempt, status, response_code, error_message, started_at, finished_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
	`, uuid.New(), jobID, workerID, attempt, status, respCode, errMsg, startedAt, finishedAt)
	if err != nil {
		return err
	}

	if nextRun != nil {
		// Recurring cron job: reset to SCHEDULED with next run time
		_, err = tx.Exec(ctx, `
			UPDATE jobs
			SET status = 'SCHEDULED', attempt = 0, scheduled_at = $2, updated_at = NOW()
			WHERE id = $1
		`, jobID, *nextRun)
	} else {
		// One-off job: mark COMPLETED (or FAILED for Day 2 before Day 3 retries)
		finalJobStatus := "COMPLETED"
		if status == "FAILED" {
			finalJobStatus = "FAILED"
		}
		_, err = tx.Exec(ctx, `
			UPDATE jobs
			SET status = $2, updated_at = NOW()
			WHERE id = $1
		`, jobID, finalJobStatus)
	}
	if err != nil {
		return err
	}

	return tx.Commit(ctx)
}
