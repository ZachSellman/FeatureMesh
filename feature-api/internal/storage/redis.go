package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

type RedisClient struct {
	client *redis.Client
	ctx    context.Context

	// Metrics
	hits   int64
	misses int64
}

func NewRedisClient(addr string) (*RedisClient, error) {
	client := redis.NewClient(&redis.Options{
		Addr:         addr,
		DialTimeout:  5 * time.Second,
		ReadTimeout:  3 * time.Second,
		WriteTimeout: 3 * time.Second,
		PoolSize:     100,
		MinIdleConns: 10,
	})

	ctx := context.Background()

	// Test connection
	if err := client.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("failed to connect to Redis: %w", err)
	}

	return &RedisClient{
		client: client,
		ctx:    ctx,
	}, nil
}

func (r *RedisClient) GetFeature(featureName, entityID string) (interface{}, error) {
	key := fmt.Sprintf("feature:%s:%s", featureName, entityID)

	val, err := r.client.Get(r.ctx, key).Result()
	if err == redis.Nil {
		r.misses++
		return nil, nil // Key doesn't exist
	}
	if err != nil {
		return nil, err
	}

	r.hits++

	// Try to unmarshal as JSON
	var result interface{}
	if err := json.Unmarshal([]byte(val), &result); err != nil {
		// If not JSON, return as string
		return val, nil
	}

	return result, nil
}

func (r *RedisClient) GetMultipleFeatures(features []string, entityID string) (map[string]interface{}, error) {
	// Use pipeline for efficient batch retrieval
	pipe := r.client.Pipeline()

	cmds := make([]*redis.StringCmd, len(features))
	for i, feature := range features {
		key := fmt.Sprintf("feature:%s:%s", feature, entityID)
		cmds[i] = pipe.Get(r.ctx, key)
	}

	_, err := pipe.Exec(r.ctx)
	if err != nil && err != redis.Nil {
		return nil, err
	}

	// Collect results
	results := make(map[string]interface{})
	for i, cmd := range cmds {
		val, err := cmd.Result()
		if err == redis.Nil {
			r.misses++
			results[features[i]] = nil
			continue
		}
		if err != nil {
			results[features[i]] = nil
			continue
		}

		r.hits++

		// Try to unmarshal as JSON
		var result interface{}
		if err := json.Unmarshal([]byte(val), &result); err != nil {
			results[features[i]] = val
		} else {
			results[features[i]] = result
		}
	}

	return results, nil
}

func (r *RedisClient) HitRate() float64 {
	total := r.hits + r.misses
	if total == 0 {
		return 0.0
	}
	return float64(r.hits) / float64(total)
}

func (r *RedisClient) Health() error {
	return r.client.Ping(r.ctx).Err()
}

func (r *RedisClient) Close() error {
	return r.client.Close()
}
