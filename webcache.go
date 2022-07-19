// Copyright 2020 - 2022 Christopher Briscoe.  All rights reserved.

package webcache

import (
	"context"

	"github.com/cespare/xxhash/v2"
)

const defaultBuckets = 16

// WebCache is a sharded cache with a specified number of buckets.
type WebCache struct {
	cache   []*Bucket
	buckets int
}

// NewWebCache creates a new WebCache with a maximum size of capacity bytes.
func NewWebCache(capacity int64, buckets int) *WebCache {
	// buckets must be between 1 and 256
	if buckets <= 0 || buckets > 256 {
		buckets = defaultBuckets
	}

	webCache := &WebCache{
		buckets: buckets,
	}

	webCache.cache = make([]*Bucket, buckets)

	// create the shards/buckets
	for i := 0; i < buckets; i++ {
		webCache.cache[i] = NewBucket(capacity / int64(buckets))
	}

	return webCache
}

// AddGroup adds a new cache group with a getter function
func (c *WebCache) AddGroup(group string, getter getter) error {
	for i := 0; i < c.buckets; i++ {
		err := c.cache[i].AddGroup(group, getter)
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
func (c *WebCache) Get(ctx context.Context, group string, key string, etag string) ([]byte, string, error) {
	return c.cache[c.getShard(key)].Get(ctx, group, key, etag)
}

// Set inserts some {key, value} into the WebCache.
func (c *WebCache) Set(group string, key string, value []byte) string {
	return c.cache[c.getShard(key)].Set(group, key, value)
}

// Stats returns the total stats of all the buckets
func (c *WebCache) Stats() *CacheStats {
	stats := &CacheStats{}
	for i := 0; i < c.buckets; i++ {
		bucketStats := c.cache[i].Stats()
		stats.EtagHits += bucketStats.EtagHits
		stats.CacheHits += bucketStats.CacheHits
		stats.GetCalls += bucketStats.GetCalls
		stats.GetDupes += bucketStats.GetDupes
		stats.GetErrors += bucketStats.GetErrors
		stats.Misses += bucketStats.Misses
		stats.Capacity += bucketStats.Capacity
		stats.Size += bucketStats.Size
	}
	return stats
}

func (c *WebCache) getShard(key string) uint64 {
	hash := xxhash.New()
	hash.Write([]byte(key))
	return hash.Sum64() % uint64(c.buckets)
}
