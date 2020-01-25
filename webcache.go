//Copyright 2020 Christopher Briscoe.  All rights reserved.

package webcache

import "hash/fnv"

const defaultBuckets = 16

// WebCache is a sharded cache with a specified number of buckets.
type WebCache struct {
	buckets  int
	capacity uint64
	cache    []*Bucket
}

// NewWebCache creates a new WebCache with a maximum size of capacity bytes.
func NewWebCache(capacity uint64, buckets int) *WebCache {
	//buckets must be between 1 and 256
	if buckets <= 0 || buckets > 256 {
		buckets = defaultBuckets
	}

	webCache := &WebCache{
		buckets:  buckets,
		capacity: capacity,
	}

	webCache.cache = make([]*Bucket, buckets)

	//create the shards/buckets
	for i := 0; i < buckets; i++ {
		webCache.cache[i] = NewBucket(capacity / uint64(buckets))
	}

	return webCache
}

// Set inserts some {key, value} into the WebCache.
func (c *WebCache) Set(key string, value []byte) string {
	return c.cache[c.getShard(key)].Set(key, value)
}

// Get retrieves a value from the WebCache and returns value and etag
func (c *WebCache) Get(key string, etag string) ([]byte, string) {
	return c.cache[c.getShard(key)].Get(key, etag)
}

// Delete the value indicated by the key, if it is present.
func (c *WebCache) Delete(key string) {
	c.cache[c.getShard(key)].Delete(key)
}

// Size reports the WebCache size as reported by the buckets.
func (c *WebCache) Size() uint64 {
	var sum uint64
	for i := 0; i < c.buckets; i++ {
		sum += c.cache[i].Size()
	}
	return sum
}

func (c *WebCache) getShard(key string) uint64 {
	hash := fnv.New64a()
	hash.Write([]byte(key))
	return hash.Sum64() % uint64(c.buckets)
}
