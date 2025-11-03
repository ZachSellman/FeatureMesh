package models

import "time"

type FeatureRequest struct {
	EntityID   string   `json:"entity_id" binding:"required"`
	EntityType string   `json:"entity_type" binding:"required"` // "user" or "post"
	Features   []string `json:"features" binding:"required"`
}

type FeatureResponse struct {
	EntityID   string                 `json:"entity_id"`
	EntityType string                 `json:"entity_type"`
	Features   map[string]interface{} `json:"features"`
	Timestamp  time.Time              `json:"timestamp"`
	CacheHit   bool                   `json:"cache_hit"`
	CacheLevel string                 `json:"cache_level"` // "L1, L2, or "miss"
}

type BatchFeatureRequest struct {
	Requests []FeatureRequest `json:"requests" binding:"required"`
}

type BatchFeatureResponse struct {
	Responses []FeatureResponse `json:"responses"`
	LatencyMs int64             `json:"latency_ms"`
}

type HealthResponse struct {
	Status     string    `json:"status"`
	RedisOK    bool      `json:"redis_ok"`
	PostgresOK bool      `json:"postgres_ok"`
	Timestamp  time.Time `json:"timestamp"`
}

type StatsResponse struct {
	L1CacheSize    int     `json:"l1_cache_size"`
	L1HitRate      float64 `json:"l1_hit_rate"`
	L2HitRate      float64 `json:"l2_hit_rate"`
	TotalRequests  int64   `json:"total_requests"`
	AverageLatency float64 `json:"average_latency_ms"`
}
