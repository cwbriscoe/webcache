// Copyright 2020 - 2023 Christopher Briscoe.  All rights reserved.

package webcache

import (
	"context"
	"time"

	"github.com/cespare/xxhash/v2"
)

const defaultBuckets = 16

// WebCache is a sharded cache with a specified number of buckets.
type WebCache struct {
	cache   []*Bucket
	buckets int
}

// Config contains values to construct a NewWebCache
type Config struct {
	Capacity int64 `json:"capacity"`
	Buckets  int   `json:"buckets"`
}

// validateBuckets validates the number of buckets in the config.
// It returns the validated number of buckets.
func validateBuckets(buckets int) int {
	if buckets <= 0 || buckets > 256 {
		return defaultBuckets
	}
	return buckets
}

// NewWebCache creates a new WebCache with a maximum size of capacity bytes.
func NewWebCache(cfg *Config) *WebCache {
	if cfg == nil {
		return nil
	}

	cfg.Buckets = validateBuckets(cfg.Buckets)

	webCache := &WebCache{
		buckets: cfg.Buckets,
	}

	webCache.cache = make([]*Bucket, cfg.Buckets)

	// create the shards/buckets
	for i := 0; i < cfg.Buckets; i++ {
		webCache.cache[i] = NewBucket(cfg.Capacity / int64(cfg.Buckets))
	}

	return webCache
}

// AddGroup adds a new cache group with a getter function.
// A cache group is a set of cache entries that can be retrieved using the getter function.
func (c *WebCache) AddGroup(group string, maxAge time.Duration, getter Getter) error {
	var err error
	for i := 0; i < c.buckets; i++ {
		if addErr := c.cache[i].AddGroup(group, maxAge, getter); addErr != nil {
			err = addErr
		}
	}
	return err
}

// Delete removes the cache entry with the given key from the specified group, if it exists.
func (c *WebCache) Delete(group string, key string) {
	c.cache[c.getShard(key)].Delete(group, key)
}

// Get retrieves the cache entry with the given key from the specified group.
// It returns the cached data and its associated CacheInfo, or an error if the entry does not exist.
func (c *WebCache) Get(ctx context.Context, group, key, etag string) ([]byte, *CacheInfo, error) {
	return c.cache[c.getShard(key)].Get(ctx, group, key, etag)
}

// Set inserts some {key, value} into the WebCache.
func (c *WebCache) Set(group, key string, value []byte) *CacheInfo {
	return c.cache[c.getShard(key)].Set(group, key, value)
}

// Stats returns the total stats of all the buckets
func (c *WebCache) Stats() *CacheStats {
	stats := &CacheStats{}
	for i := 0; i < c.buckets; i++ {
		bucketStats := c.cache[i].Stats()
		stats.EtagHits.Add(bucketStats.EtagHits.Load())
		stats.CacheHits.Add(bucketStats.CacheHits.Load())
		stats.GetCalls.Add(bucketStats.GetCalls.Load())
		stats.GetDupes.Add(bucketStats.GetDupes.Load())
		stats.GetErrors.Add(bucketStats.GetErrors.Load())
		stats.GetMisses.Add(bucketStats.GetMisses.Load())
		stats.TrimEntries.Add(bucketStats.TrimEntries.Load())
		stats.TrimBytes.Add(bucketStats.TrimBytes.Load())
		stats.Capacity.Add(bucketStats.Capacity.Load())
		stats.Size.Add(bucketStats.Size.Load())
	}
	return stats
}

// BucketStats returns statistics about all buckets
func (c *WebCache) BucketStats() []*CacheStats {
	stats := make([]*CacheStats, c.buckets)
	for i := 0; i < c.buckets; i++ {
		stats[i] = c.cache[i].Stats()
	}
	return stats
}

func (c *WebCache) getShard(key string) uint64 {
	hash := xxhash.New()
	_, _ = hash.Write([]byte(key))
	return hash.Sum64() % uint64(c.buckets)
}
