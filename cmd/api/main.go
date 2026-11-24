package main

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/nishankx/distributed-job-scheduler/internal/reconciler"
	"github.com/nishankx/distributed-job-scheduler/internal/store"
	"github.com/redis/go-redis/v9"
)

type CreateJobRequest struct {
	Name        string          `json:"name"`
	WebhookURL  string          `json:"webhook_url"`
	Payload     json.RawMessage `json:"payload"`
	CronExpr    *string         `json:"cron_expr,omitempty"`
	MaxRetries  *int            `json:"max_retries,omitempty"`
	ScheduledAt *time.Time      `json:"scheduled_at,omitempty"`
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// 1. Connect to PostgreSQL
	dbURL := "postgres://scheduler:secret@localhost:5432/scheduler_db?sslmode=disable"
	pool, err := pgxpool.New(ctx, dbURL)
	if err != nil {
		slog.Error("failed to connect to postgres", "error", err)
		os.Exit(1)
	}
	defer pool.Close()

	// 2. Connect to Redis
	rdb := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})
	defer rdb.Close()

	if err := rdb.Ping(ctx).Err(); err != nil {
		slog.Error("failed to connect to redis", "error", err)
		os.Exit(1)
	}

	// 3. Start Outbox Reconciler in background
	jobStore := store.NewPostgresStore(pool)
	outboxRec := reconciler.NewOutboxReconciler(pool, rdb)
	go outboxRec.Start(ctx)

	// 4. Setup HTTP Routes
	mux := http.NewServeMux()

	mux.HandleFunc("POST /jobs", func(w http.ResponseWriter, r *http.Request) {
		var req CreateJobRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "invalid JSON payload", http.StatusBadRequest)
			return
		}
		if req.Name == "" || req.WebhookURL == "" {
			http.Error(w, "name and webhook_url are required", http.StatusBadRequest)
			return
		}
		if _, err := url.ParseRequestURI(req.WebhookURL); err != nil {
			http.Error(w, "invalid webhook_url", http.StatusBadRequest)
			return
		}

		scheduledAt := time.Now().UTC()
		if req.ScheduledAt != nil {
			scheduledAt = req.ScheduledAt.UTC()
		}

		maxRetries := 3
		if req.MaxRetries != nil && *req.MaxRetries >= 0 {
			maxRetries = *req.MaxRetries
		}

		payload := req.Payload
		if len(payload) == 0 {
			payload = json.RawMessage(`{}`)
		}

		job := &store.Job{
			ID:          uuid.New(),
			Name:        req.Name,
			WebhookURL:  req.WebhookURL,
			Payload:     payload,
			CronExpr:    req.CronExpr,
			MaxRetries:  maxRetries,
			ScheduledAt: scheduledAt,
		}

		if err := jobStore.CreateJobWithOutbox(r.Context(), job); err != nil {
			slog.Error("failed to persist job", "error", err)
			http.Error(w, "internal server error", http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		json.NewEncoder(w).Encode(map[string]any{
			"job_id":       job.ID,
			"status":       "PENDING",
			"scheduled_at": job.ScheduledAt,
		})
	})

	mux.HandleFunc("GET /jobs/{id}", func(w http.ResponseWriter, r *http.Request) {
		id, err := uuid.Parse(r.PathValue("id"))
		if err != nil {
			http.Error(w, "invalid job UUID", http.StatusBadRequest)
			return
		}

		job, err := jobStore.GetJobByID(r.Context(), id)
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				http.Error(w, "job not found", http.StatusNotFound)
				return
			}
			http.Error(w, "internal server error", http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(job)
	})

	srv := &http.Server{
		Addr:    ":8080",
		Handler: mux,
	}

	go func() {
		slog.Info("API server listening on :8080")
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("HTTP server error", "error", err)
		}
	}()

	<-ctx.Done()
	slog.Info("shutting down gracefully...")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	srv.Shutdown(shutdownCtx)
}