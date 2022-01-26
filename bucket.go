// Copyright 2020 Christopher Briscoe.  All rights reserved.

// Package webcache is A simple LRU cache for storing documents ([]byte). When the size maximum is reached,
// items are evicted starting with the least recently used. This data structure is goroutine-safe (it has
// a lock around all operations).
package webcache

import (
	"container/list"
	"context"
	"errors"
	"strconv"
	"sync"
	"sync/atomic"

	"github.com/cespare/xxhash/v2"
)

type cacheValue struct {
	key   string
	bytes []byte
}

// Just an estimate
func (v *cacheValue) size() int64 {
	return int64(len(v.key) + len(v.bytes))
}

// Cacher is an interface for either an Bucket or WebCache
type Cacher interface {
	AddGroup(string, getter) error
	Delete(string, string)
	Get(context.Context, string, string, string) ([]byte, string, error)
	Set(string, string, []byte) string
	Stats() *CacheStats
}

// Bucket is a simple LRU cache with Etag support
type Bucket struct {
	sync.Mutex
	list   *list.List
	groups map[string]*group
	table  map[string]*list.Element
	etags  map[string]string
	stats  CacheStats
}

// CacheStats keeps track of cache statistics
type CacheStats struct {
	EtagHits  int64
	CacheHits int64
	GetCalls  int64
	GetDupes  int64
	GetErrors int64
	Misses    int64
	Capacity  int64
	Size      int64
}

// NewBucket creates a new Cache with a maximum size of capacity bytes.
func NewBucket(capacity int64) *Bucket {
	bucket := &Bucket{
		stats:  CacheStats{},
		list:   list.New(),
		groups: make(map[string]*group),
		table:  make(map[string]*list.Element),
		etags:  make(map[string]string),
	}
	bucket.stats.Capacity = capacity
	return bucket
}

// AddGroup adds a new cache group with a getter function
func (c *Bucket) AddGroup(group string, getter getter) error {
	c.Lock()

	_, ok := c.groups[group]
	if ok {
		c.Unlock()
		return errors.New(group + " cache group already exists")
	}

	grp, err := newGroup(group, getter)
	if err != nil {
		return err
	}
	c.groups[group] = grp

	c.Unlock()
	return nil
}

// deleteKey will delete from the cache and etag map.
// the mutex must be locked before calling this function.
func (c *Bucket) deleteKey(key string) {
	elt, ok := c.table[key]
	if ok {
		delete(c.table, key)
		v := c.list.Remove(elt).(*cacheValue)
		atomic.AddInt64(&c.stats.Size, -v.size())
	}

	//delete the etag
	val, ok := c.etags[key]
	if ok {
		delete(c.etags, key)
		atomic.AddInt64(&c.stats.Size, -int64(len(key)+len(val)))
	}
}

// Delete the value indicated by the key, if it is present.
func (c *Bucket) Delete(group string, key string) {
	cacheKey := group + key
	c.Lock()
	c.deleteKey(cacheKey)
	c.Unlock()
}

// Get retrieves a value from the cache or nil if no value present.
func (c *Bucket) Get(ctx context.Context, group string, key string, etag string) ([]byte, string, error) {
	cacheKey := group + key
	c.Lock()

	//return if the etag matches the etag cache
	hash := c.etags[cacheKey]
	if etag != "" && etag == hash {
		c.Unlock()
		atomic.AddInt64(&c.stats.EtagHits, 1)
		return nil, etag, nil
	}

	//otherwise see if we get a cache hit
	elt, ok := c.table[cacheKey]
	if !ok {
		//no cache hit so call the do(key) function for the group
		grp, ok := c.groups[group]
		c.Unlock()
		if !ok {
			atomic.AddInt64(&c.stats.Misses, 1)
			return nil, "", nil
		}
		value, dupe, err := grp.do(ctx, key)
		if err != nil {
			atomic.AddInt64(&c.stats.GetErrors, 1)
			return nil, "", err
		}
		//now set the value from the do(key) call into the cache
		var newEtag string
		if !dupe {
			newEtag = c.Set(group, key, value)
			atomic.AddInt64(&c.stats.GetCalls, 1)
		} else {
			//try to get etag from etag cache for dupe threads on same do(key)
			c.Lock()
			newEtag = c.etags[cacheKey]
			c.Unlock()
			atomic.AddInt64(&c.stats.GetDupes, 1)
		}
		return value, newEtag, nil
	}

	value := elt.Value.(*cacheValue).bytes
	c.list.MoveToFront(elt)
	c.Unlock()

	atomic.AddInt64(&c.stats.CacheHits, 1)
	return value, hash, nil
}

// Set inserts some {key, value} into the cache.
func (c *Bucket) Set(group string, key string, value []byte) string {
	cacheKey := group + key
	c.Lock()

	c.deleteKey(cacheKey)

	v := &cacheValue{cacheKey, value}
	elt := c.list.PushFront(v)
	c.table[cacheKey] = elt
	atomic.AddInt64(&c.stats.Size, v.size())

	//save the etag
	hash := xxhash.New()
	hash.Write(value)
	hashstr := strconv.FormatUint(hash.Sum64(), 16)
	c.etags[cacheKey] = hashstr
	atomic.AddInt64(&c.stats.Size, int64(len(cacheKey)+len(hashstr)))

	c.trim()
	c.Unlock()
	return hashstr
}

// Stats returns statistics about this Bucket
func (c *Bucket) Stats() *CacheStats {
	return &CacheStats{
		EtagHits:  atomic.LoadInt64(&c.stats.EtagHits),
		CacheHits: atomic.LoadInt64(&c.stats.CacheHits),
		GetCalls:  atomic.LoadInt64(&c.stats.GetCalls),
		GetDupes:  atomic.LoadInt64(&c.stats.GetDupes),
		GetErrors: atomic.LoadInt64(&c.stats.GetErrors),
		Misses:    atomic.LoadInt64(&c.stats.Misses),
		Capacity:  atomic.LoadInt64(&c.stats.Capacity),
		Size:      atomic.LoadInt64(&c.stats.Size),
	}
}

// If the cache is over capacity, clear elements (starting at the end of the list) until it is back under
// capacity. Note that this method is not threadsafe (it should only be called from other methods which
// already hold the lock).
func (c *Bucket) trim() {
	for c.stats.Size > c.stats.Capacity {
		elt := c.list.Back()
		if elt == nil {
			break
		}
		v := c.list.Remove(elt).(*cacheValue)
		delete(c.table, v.key)
		c.stats.Size -= v.size()
	}
}
