package worker

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/nishankx/distributed-job-scheduler/internal/store"
	"github.com/redis/go-redis/v9"
	"github.com/robfig/cron/v3"
)

const (
	streamName    = "ready_jobs"
	consumerGroup = "workers_group"
)

type WorkerPool struct {
	rdb         *redis.Client
	store       *store.PostgresStore
	workerID    string
	concurrency int
	httpClient  *http.Client
	cronParser  cron.Parser
}

func NewWorkerPool(rdb *redis.Client, st *store.PostgresStore, concurrency int) *WorkerPool {
	return &WorkerPool{
		rdb:         rdb,
		store:       st,
		workerID:    "worker-" + uuid.NewString()[:8],
		concurrency: concurrency,
		httpClient:  &http.Client{Timeout: 10 * time.Second},
		cronParser:  cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow),
	}
}

func (w *WorkerPool) Start(ctx context.Context) {
	// Create consumer group (ignore BUSYGROUP error if it already exists)
	err := w.rdb.XGroupCreateMkStream(ctx, streamName, consumerGroup, "0").Err()
	if err != nil && !strings.Contains(err.Error(), "BUSYGROUP") {
		slog.Error("failed to create consumer group", "error", err)
		return
	}

	sem := make(chan struct{}, w.concurrency)
	var wg sync.WaitGroup

	slog.Info("worker pool started", "worker_id", w.workerID, "concurrency", w.concurrency)

	for {
		if ctx.Err() != nil {
			break
		}

		streams, err := w.rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    consumerGroup,
			Consumer: w.workerID,
			Streams:  []string{streamName, ">"},
			Count:    int64(w.concurrency),
			Block:    2 * time.Second,
		}).Result()

		if err != nil {
			if !errors.Is(err, redis.Nil) && ctx.Err() == nil {
				slog.Error("XREADGROUP failed", "error", err)
			}
			continue
		}

		for _, stream := range streams {
			for _, msg := range stream.Messages {
				sem <- struct{}{}
				wg.Add(1)

				go func(m redis.XMessage) {
					defer func() {
						<-sem
						wg.Done()
					}()
					w.processMessage(ctx, m)
				}(msg)
			}
		}
	}

	slog.Info("waiting for in-flight jobs to finish...")
	wg.Wait()
	slog.Info("worker pool shut down cleanly")
}

func (w *WorkerPool) processMessage(ctx context.Context, msg redis.XMessage) {
	rawID, ok := msg.Values["job_id"].(string)
	if !ok {
		w.rdb.XAck(ctx, streamName, consumerGroup, msg.ID)
		return
	}

	jobID, err := uuid.Parse(rawID)
	if err != nil {
		w.rdb.XAck(ctx, streamName, consumerGroup, msg.ID)
		return
	}

	job, err := w.store.MarkJobRunning(ctx, jobID)
	if err != nil {
		slog.Error("failed to load job", "job_id", jobID, "error", err)
		return
	}

	startedAt := time.Now().UTC()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, job.WebhookURL, bytes.NewReader(job.Payload))
	if err != nil {
		w.finalizeJob(ctx, msg.ID, job, "FAILED", 0, err.Error(), startedAt)
		return
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Job-ID", job.ID.String())

	resp, err := w.httpClient.Do(req)
	if err != nil {
		slog.Warn("webhook execution failed", "job_id", job.ID, "error", err)
		w.finalizeJob(ctx, msg.ID, job, "FAILED", 0, err.Error(), startedAt)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		slog.Info("job executed successfully", "job_id", job.ID, "status_code", resp.StatusCode)
		w.finalizeJob(ctx, msg.ID, job, "SUCCESS", resp.StatusCode, "", startedAt)
	} else {
		slog.Warn("webhook returned non-2xx", "job_id", job.ID, "status_code", resp.StatusCode)
		w.finalizeJob(ctx, msg.ID, job, "FAILED", resp.StatusCode, "non-2xx response", startedAt)
	}
}

func (w *WorkerPool) finalizeJob(
	ctx context.Context,
	msgID string,
	job *store.Job,
	execStatus string,
	respCode int,
	errMsg string,
	startedAt time.Time,
) {
	finishedAt := time.Now().UTC()
	var nextRun *time.Time

	// If job has a cron expression, compute next scheduled timestamp
	if job.CronExpr != nil && *job.CronExpr != "" {
		if sched, err := w.cronParser.Parse(*job.CronExpr); err == nil {
			t := sched.Next(finishedAt).UTC()
			nextRun = &t
		}
	}

	if err := w.store.RecordExecutionAndComplete(
		ctx, job.ID, w.workerID, job.Attempt+1,
		execStatus, respCode, errMsg, startedAt, finishedAt, nextRun,
	); err != nil {
		slog.Error("failed to record execution", "job_id", job.ID, "error", err)
		return
	}

	// If recurring cron job, push next execution back into Redis ZSET
	if nextRun != nil {
		w.rdb.ZAdd(ctx, "scheduled_jobs", redis.Z{
			Score:  float64(nextRun.Unix()),
			Member: job.ID.String(),
		})
		slog.Info("rescheduled cron job", "job_id", job.ID, "next_run", *nextRun)
	}

	// Acknowledge message in Redis Stream
	w.rdb.XAck(ctx, streamName, consumerGroup, msgID)
}
