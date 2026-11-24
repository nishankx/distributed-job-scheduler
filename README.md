# Distributed Job Scheduler in Go

A fault-tolerant, high-throughput **Distributed Job Scheduler** built in Go. It accepts one-off delayed jobs and recurring cron jobs via an HTTP API and guarantees at-least-once execution across a cluster of worker nodes using a **Dual-Storage Architecture** (PostgreSQL + Redis).

---

## Architecture Overview

This platform separates **durable persistence** from **sub-millisecond scheduling & dispatch**:

```text
Client ──▶ Go API Server ──(Tx + Outbox)──▶ PostgreSQL (Source of Truth)
                                                 │
                                          Outbox Reconciler
                                                 ▼
Scheduler (Leader) ◀──(Lua Promotion)──▶ Redis (ZSET ──▶ Stream)
                                                 │
                                       Consumer Group (XREADGROUP)
                                                 ▼
                                         Go Worker Pool ──▶ Executor (HTTP/Webhooks)
```

### Core Components

1. **Go API Server:** Exposes REST endpoints (`POST /jobs`, `GET /jobs/{id}`) to schedule tasks and inspect execution statuses.
2. **PostgreSQL (Source of Truth):** Stores durable job definitions, execution logs, and a **Transactional Outbox** table to eliminate dual-write inconsistencies.
3. **Outbox Reconciler:** Polls unprocessed outbox records (`FOR UPDATE SKIP LOCKED`) and pushes scheduled tasks into Redis. Also serves as the disaster recovery mechanism if Redis loses state.
4. **Redis (Dispatch & Coordination):**
   - **Waiting Queue (`ZSET`):** Holds future jobs scored by their Unix execution timestamp (`scheduled_at`).
   - **Ready Queue (`Stream`):** Holds due jobs ready for immediate consumption by workers.
   - **Dead Letter Queue (`Stream`):** Captures permanently failed jobs after exhausting retries.
5. **Distributed Scheduler:** Runs with **Leader Election** (`SET NX EX` lease) so multiple instances can run for High Availability while only one active leader promotes due jobs from the `ZSET` to the `Stream` using atomic **Lua scripts**.
6. **Worker Pool & Executor:** Stateless Go workers reading via Redis Consumer Groups (`XREADGROUP`). Each job is executed inside a bounded goroutine with timeouts, exponential backoff + jitter retries, and idempotency checks.
7. **Observability:** Exposes Prometheus `/metrics` across the API, Scheduler, and Worker services.

---

## Project Structure

```text
job-scheduler/
├── cmd/
│   ├── api/main.go            # HTTP ingestion server
│   ├── scheduler/main.go      # Leader election + ZSET->Stream promoter + Reconciler
│   └── worker/main.go         # Redis Stream consumer + Executor pool
├── internal/
│   ├── models/job.go          # Domain models and status enums
│   ├── store/postgres.go      # pgx queries & transactional outbox logic
│   ├── queue/redis.go         # Redis client, Lua loaders, and stream operations
│   └── lua/
│       ├── promote.lua        # Atomic ZSET -> Ready Stream promotion
│       └── retry.lua          # Atomic exponential backoff or DLQ routing
├── migrations/
│   └── 001_init.sql           # PostgreSQL schema
├── docker-compose.yml         # PostgreSQL, Redis, and Prometheus stack
└── README.md
```

---

## Getting Started

### Prerequisites
- **Go:** 1.22+
- **Docker & Docker Compose:** For running PostgreSQL, Redis, and Prometheus

### 1. Start Infrastructure
```bash
docker-compose up -d
```

### 2. Run Database Migrations
```bash
psql "postgres://postgres:postgres@localhost:5432/scheduler?sslmode=disable" -f migrations/001_init.sql
```

### 3. Start the Services
Open three terminal windows to run each component independently (or scale workers/schedulers horizontally):

```bash
# Terminal 1: Start the API Server
go run ./cmd/api

# Terminal 2: Start the Scheduler (run multiple instances to test leader failover)
go run ./cmd/scheduler

# Terminal 3: Start a Worker Node
go run ./cmd/worker
```

### 4. Schedule a Test Job
```bash
curl -X POST http://localhost:8080/jobs \
  -H "Content-Type: application/json" \
  -d '{
    "payload": {"url": "https://httpbin.org/post", "data": {"hello": "world"}},
    "scheduled_at": "2026-11-24T19:30:00Z",
    "max_retries": 3
  }'
```

---

## 3-Day Implementation Checklist

### Day 1: Storage, Ingestion & Outbox Foundation
- [ ] **Infrastructure Setup:** Spin up PostgreSQL, Redis, and Prometheus via `docker-compose.yml`.
- [ ] **Database Migrations:** Create `jobs`, `job_executions`, and `outbox` tables with indexes on `outbox(processed)` and `jobs(status)`.
- [ ] **Go API Server:** Build `POST /jobs` (accepting payload, `scheduled_at`, optional `cron_expr`, `max_retries`) and `GET /jobs/{id}` using `net/http` and `pgx`.
- [ ] **Transactional Outbox:** Wrap `INSERT INTO jobs` and `INSERT INTO outbox` inside a single `pgx.Tx` to prevent lost writes.
- [ ] **Outbox Reconciler:** Write a background goroutine that polls unprocessed outbox rows in batches (`FOR UPDATE SKIP LOCKED`), executes `ZADD scheduled_jobs`, and marks rows processed.

### Day 2: Distributed Scheduler, Lua Scripts & Worker Pool
- [ ] **Leader Election:** Implement Redis `SET NX` lease acquisition and heartbeat renewal so multiple scheduler binaries can run safely without duplicate dispatching.
- [ ] **Atomic Lua Promotion:** Write, test, and load `promote.lua` via `SCRIPT LOAD` to move due jobs from `ZSET` to the `ready_jobs` Redis Stream.
- [ ] **Worker Pool & Consumer Group:** Build the worker binary using `XREADGROUP` with a semaphore-bounded goroutine pool.
- [ ] **Webhook Executor:** Implement HTTP job execution with `context.WithTimeout`, response status validation, and execution logging in PostgreSQL.
- [ ] **Cron Rescheduling:** Integrate `github.com/robfig/cron/v3` to compute the next `scheduled_at` timestamp upon completion of recurring jobs and re-insert into the `ZSET`.

### Day 3: Fault Tolerance, Retries, Observability & Chaos Testing
- [ ] **Retry & DLQ Lua Script:** Implement `retry.lua` to calculate exponential backoff with random jitter ($\text{base} \times 2^{\text{attempt}} + \text{jitter}$), pushing back to `scheduled_jobs` or moving to the `dlq_jobs` stream when `attempt >= max_retries`.
- [ ] **Stale Message Reclaiming:** Add an `XAUTOCLAIM` background routine in workers to reclaim unacknowledged messages from crashed workers after a visibility timeout.
- [ ] **Redis Crash Recovery:** Implement a startup/on-demand reconciliation command that scans PostgreSQL for all `PENDING`/`QUEUED` jobs and repopulates Redis.
- [ ] **Prometheus Metrics:** Expose `/metrics` on API, Scheduler, and Worker services tracking `jobs_submitted_total`, `jobs_executed_total`, `job_execution_duration_seconds`, and `dlq_size`.
- [ ] **Chaos Verification:** Run a load test (e.g., 5,000 jobs), `docker kill` the active scheduler leader and one worker mid-flight, flush Redis (`FLUSHALL`), and verify zero lost jobs.