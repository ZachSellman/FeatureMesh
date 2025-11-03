package cache

import (
	"sync"
	"time"
)

type CacheItem struct {
	Value      interface{}
	Expiration int64
}

type LocalCache struct {
	items   map[string]CacheItem
	mu      sync.RWMutex
	ttl     time.Duration
	maxSize int

	// Metrics
	hitsMu sync.RWMutex
	hits   int64
	misses int64
}

func NewLocalCache(ttl time.Duration, maxSize int) *LocalCache {
	cache := &LocalCache{
		items:   make(map[string]CacheItem),
		ttl:     ttl,
		maxSize: maxSize,
	}

	// Start cleanup goroutine
	go cache.cleanup()

	return cache
}

func (c *LocalCache) Get(key string) (interface{}, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	item, exists := c.items[key]
	if !exists {
		c.incrementMisses()
		return nil, false
	}

	// Check if expired
	if time.Now().UnixNano() > item.Expiration {
		c.incrementMisses()
		return nil, false
	}

	c.incrementHits()
	return item.Value, true
}

func (c *LocalCache) Set(key string, value interface{}) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Simple eviction if cache is full
	if len(c.items) >= c.maxSize {
		// Remove oldest (random) item
		for k := range c.items {
			delete(c.items, k)
			break
		}
	}

	c.items[key] = CacheItem{
		Value:      value,
		Expiration: time.Now().Add(c.ttl).UnixNano(),
	}
}

func (c *LocalCache) Delete(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.items, key)
}

func (c *LocalCache) Size() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.items)
}

func (c *LocalCache) incrementHits() {
	c.hitsMu.Lock()
	defer c.hitsMu.Unlock()
	c.hits++
}

func (c *LocalCache) incrementMisses() {
	c.hitsMu.Lock()
	defer c.hitsMu.Unlock()
	c.misses++
}

func (c *LocalCache) HitRate() float64 {
	c.hitsMu.RLock()
	defer c.hitsMu.RUnlock()

	total := c.hits + c.misses
	if total == 0 {
		return 0.0
	}
	return float64(c.hits) / float64(total)
}

func (c *LocalCache) cleanup() {
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()

	for range ticker.C {
		c.mu.Lock()
		now := time.Now().UnixNano()
		for key, item := range c.items {
			if now > item.Expiration {
				delete(c.items, key)
			}
		}
		c.mu.Unlock()
	}
}
