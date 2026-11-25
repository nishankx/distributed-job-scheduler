package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/nishankx/distributed-job-scheduler/internal/store"
	"github.com/nishankx/distributed-job-scheduler/internal/worker"
	"github.com/redis/go-redis/v9"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	dbURL := "postgres://scheduler:secret@localhost:5432/scheduler_db?sslmode=disable"
	pool, err := pgxpool.New(ctx, dbURL)
	if err != nil {
		slog.Error("failed to connect to postgres", "error", err)
		os.Exit(1)
	}
	defer pool.Close()

	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	defer rdb.Close()

	jobStore := store.NewPostgresStore(pool)
	workerPool := worker.NewWorkerPool(rdb, jobStore, 10)
	workerPool.Start(ctx)
}
