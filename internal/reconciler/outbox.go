package reconciler

import (
	"context"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
)

type OutboxEntry struct {
	ID          int64
	JobID       uuid.UUID
	ScheduledAt int64
}

type OutboxReconciler struct {
	pool *pgxpool.Pool
	rdb  *redis.Client
}

func NewOutboxReconciler(pool *pgxpool.Pool, rdb *redis.Client) *OutboxReconciler {
	return &OutboxReconciler{pool: pool, rdb: rdb}
}

func (r *OutboxReconciler) Start(ctx context.Context) {
	ticker := time.NewTicker(150 * time.Millisecond)
	defer ticker.Stop()

	slog.Info("outbox reconciler started")
	for {
		select {
		case <-ctx.Done():
			slog.Info("outbox reconciler stopped")
			return
		case <-ticker.C:
			if err := r.processBatch(ctx); err != nil {
				slog.Error("outbox reconciliation failed", "error", err)
			}
		}
	}
}

func (r *OutboxReconciler) processBatch(ctx context.Context) error {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	rows, err := tx.Query(ctx, `
		SELECT id, job_id, scheduled_at
		FROM outbox
		WHERE processed = FALSE
		ORDER BY id ASC
		LIMIT 100
		FOR UPDATE SKIP LOCKED
	`)
	if err != nil {
		return err
	}

	var entries []OutboxEntry
	for rows.Next() {
		var e OutboxEntry
		if err := rows.Scan(&e.ID, &e.JobID, &e.ScheduledAt); err != nil {
			rows.Close()
			return err
		}
		entries = append(entries, e)
	}
	rows.Close()

	if len(entries) == 0 {
		return nil
	}

	pipe := r.rdb.Pipeline()
	outboxIDs := make([]int64, len(entries))
	jobIDs := make([]uuid.UUID, len(entries))

	for i, e := range entries {
		outboxIDs[i] = e.ID
		jobIDs[i] = e.JobID
		pipe.ZAdd(ctx, "scheduled_jobs", redis.Z{
			Score:  float64(e.ScheduledAt),
			Member: e.JobID.String(),
		})
	}

	if _, err := pipe.Exec(ctx); err != nil {
		return err
	}

	if _, err := tx.Exec(ctx, `UPDATE outbox SET processed = TRUE WHERE id = ANY($1)`, outboxIDs); err != nil {
		return err
	}

	if _, err := tx.Exec(ctx, `UPDATE jobs SET status = 'SCHEDULED', updated_at = NOW() WHERE id = ANY($1)`, jobIDs); err != nil {
		return err
	}

	return tx.Commit(ctx)
}