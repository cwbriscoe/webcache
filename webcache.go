//Copyright 2020 Christopher Briscoe.  All rights reserved.

package webcache

import "hash/fnv"

const defaultShards = 16

// WebCache is a sharded cache with a specified number of shards.
type WebCache struct {
	shards   int
	capacity uint64
	cache    []*Shard
}

// NewWebCache creates a new WebCache with a maximum size of capacity bytes.
func NewWebCache(capacity uint64, shards int) *WebCache {
	//shards must be between 1 and 256
	if shards <= 0 || shards > 256 {
		shards = defaultShards
	}

	webCache := &WebCache{
		shards:   shards,
		capacity: capacity,
	}

	webCache.cache = make([]*Shard, shards)

	//create the shards/buckets
	for i := 0; i < shards; i++ {
		webCache.cache[i] = NewShard(capacity / uint64(shards))
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

// Size reports the WebCache size as reported by the shards.
func (c *WebCache) Size() uint64 {
	var sum uint64
	for i := 0; i < c.shards; i++ {
		sum += c.cache[i].Size()
	}
	return sum
}

func (c *WebCache) getShard(key string) uint64 {
	hash := fnv.New64a()
	hash.Write([]byte(key))
	return hash.Sum64() % uint64(c.shards)
}