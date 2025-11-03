package service

import (
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	"feature-api/internal/cache"
	"feature-api/internal/models"
	"feature-api/internal/storage"
)

type FeatureService struct {
	redis      *storage.RedisClient
	postgres   *storage.PostgresClient
	localCache *cache.LocalCache
}

func NewFeatureService(redis *storage.RedisClient, postgres *storage.PostgresClient) *FeatureService {
	return &FeatureService{
		redis:      redis,
		postgres:   postgres,
		localCache: cache.NewLocalCache(5*time.Minute, 10000), // 5 min TTL, 10k items max
	}
}

func (s *FeatureService) GetFeatures(req models.FeatureRequest) (*models.FeatureResponse, error) {
	cacheKey := fmt.Sprintf("%s:%s:%v", req.EntityType, req.EntityID, req.Features)

	// Try L1 cache (local memory)
	if cached, ok := s.localCache.Get(cacheKey); ok {
		if cachedFeatures, ok := cached.(map[string]interface{}); ok {
			return &models.FeatureResponse{
				EntityID:   req.EntityID,
				EntityType: req.EntityType,
				Features:   cachedFeatures,
				Timestamp:  time.Now(),
				CacheHit:   true,
				CacheLevel: "L1",
			}, nil
		}
	}

	// L1 miss, try L2 cache (Redis)
	features, err := s.redis.GetMultipleFeatures(req.Features, req.EntityID)
	if err != nil {
		return nil, fmt.Errorf("failed to get features from Redis: %w", err)
	}

	// Check if we got all features from Redis
	allPresent := true
	for _, feature := range req.Features {
		if features[feature] == nil {
			allPresent = false
			break
		}
	}

	cacheLevel := "L2"

	// If some features missing, try PostgreSQL (offline store)
	if !allPresent {
		cacheLevel = "miss"
		for _, feature := range req.Features {
			if features[feature] == nil {
				// Fallback to PostgreSQL
				value, err := s.postgres.GetOfflineFeature(req.EntityID, req.EntityType, feature)
				if err != nil {
					log.Printf("Failed to get feature from PostgreSQL: %v", err)
					continue
				}
				if value != "" {
					// Try to parse as number
					var parsed interface{}
					if err := json.Unmarshal([]byte(value), &parsed); err == nil {
						features[feature] = parsed
					} else {
						features[feature] = value
					}
				}
			}
		}
	}

	// Update L1 cache
	s.localCache.Set(cacheKey, features)

	return &models.FeatureResponse{
		EntityID:   req.EntityID,
		EntityType: req.EntityType,
		Features:   features,
		Timestamp:  time.Now(),
		CacheHit:   cacheLevel != "miss",
		CacheLevel: cacheLevel,
	}, nil
}

func (s *FeatureService) GetMultipleFeatures(requests []models.FeatureRequest) ([]models.FeatureResponse, error) {
	responses := make([]models.FeatureResponse, len(requests))

	// Use worker pool for concurrent requests
	var wg sync.WaitGroup
	respChan := make(chan struct {
		index int
		resp  *models.FeatureResponse
		err   error
	}, len(requests))

	// Process requests concurrently
	for i, req := range requests {
		wg.Add(1)
		go func(idx int, request models.FeatureRequest) {
			defer wg.Done()
			resp, err := s.GetFeatures(request)
			respChan <- struct {
				index int
				resp  *models.FeatureResponse
				err   error
			}{index: idx, resp: resp, err: err}
		}(i, req)
	}

	// Close channel when all goroutines complete
	go func() {
		wg.Wait()
		close(respChan)
	}()

	// Collect results
	for result := range respChan {
		if result.err != nil {
			return nil, result.err
		}
		responses[result.index] = *result.resp
	}

	return responses, nil
}

func (s *FeatureService) GetStats() models.StatsResponse {
	l1HitRate := 0.0
	l2HitRate := 0.0
	l1Size := 0

	// Safely get L1 stats
	if s.localCache != nil {
		l1HitRate = s.localCache.HitRate()
		l1Size = s.localCache.Size()
	}

	// Safely get L2 stats
	if s.redis != nil {
		l2HitRate = s.redis.HitRate()
	}

	return models.StatsResponse{
		L1CacheSize:    l1Size,
		L1HitRate:      l1HitRate,
		L2HitRate:      l2HitRate,
		TotalRequests:  0,   // Future stat
		AverageLatency: 0.0, // Future stat
	}
}
