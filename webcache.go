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

// NewWebCache creates a new WebCache with a maximum size of capacity bytes.
func NewWebCache(cfg *Config) *WebCache {
	// buckets must be between 1 and 256
	if cfg.Buckets <= 0 || cfg.Buckets > 256 {
		cfg.Buckets = defaultBuckets
	}

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

// AddGroup adds a new cache group with a getter function
func (c *WebCache) AddGroup(group string, maxAge time.Duration, getter Getter) error {
	for i := 0; i < c.buckets; i++ {
		err := c.cache[i].AddGroup(group, maxAge, getter)
		if err != nil {
			return err
		}
	}
	return nil
}

// Delete the value indicated by the key, if it is present.
func (c *WebCache) Delete(group string, key string) {
	c.cache[c.getShard(key)].Delete(group, key)
}

// Get retrieves a value from the WebCache and returns value and etag
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
	hash.Write([]byte(key))
	return hash.Sum64() % uint64(c.buckets)
}
