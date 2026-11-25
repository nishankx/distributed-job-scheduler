package scheduler

import (
	"context"
	_ "embed"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

//go:embed../lua/promote.lua
var promoteLuaScript string

const leaderKey = "scheduler:leader"

type Scheduler struct {
	rdb        *redis.Client
	instanceID string
	promote    *redis.Script
}

func NewScheduler(rdb *redis.Client) *Scheduler {
	return &Scheduler{
		rdb:        rdb,
		instanceID: uuid.NewString(),
		promote:    redis.NewScript(promoteLuaScript),
	}
}

func (s *Scheduler) Start(ctx context.Context) {
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()

	slog.Info("scheduler node started", "instance_id", s.instanceID)

	for {
		select {
		case <-ctx.Done():
			slog.Info("scheduler node stopping", "instance_id", s.instanceID)
			return
		case <-ticker.C:
			if !s.acquireOrRenewLeadership(ctx) {
				continue
			}

			now := time.Now().UTC().Unix()
			res, err := s.promote.Run(ctx, s.rdb, []string{"scheduled_jobs", "ready_jobs"}, now, 100).Int()
			if err != nil {
				slog.Error("failed to promote due jobs", "error", err)
				continue
			}
			if res > 0 {
				slog.Info("promoted due jobs to stream", "count", res, "leader", s.instanceID)
			}
		}
	}
}

func (s *Scheduler) acquireOrRenewLeadership(ctx context.Context) bool {
	// 1. Attempt to claim leadership if vacant (5-second lease)
	acquired, err := s.rdb.SetNX(ctx, leaderKey, s.instanceID, 5*time.Second).Result()
	if err != nil {
		return false
	}
	if acquired {
		slog.Info("acquired scheduler leadership", "instance_id", s.instanceID)
		return true
	}

	// 2. If already held, check if WE are the leader and renew TTL
	currentLeader, err := s.rdb.Get(ctx, leaderKey).Result()
	if err == nil && currentLeader == s.instanceID {
		s.rdb.Expire(ctx, leaderKey, 5*time.Second)
		return true
	}

	return false
}
