//Copyright 2020 Christopher Briscoe.  All rights reserved.

// Package webcache is A simple LRU cache for storing documents ([]byte). When the size maximum is reached,
// items are evicted starting with the least recently used. This data structure is goroutine-safe (it has
// a lock around all operations).
package webcache

import (
	"container/list"
	"context"
	"errors"
	"hash/fnv"
	"strconv"
	"sync"
	"sync/atomic"
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
	stats  CacheStats
	list   *list.List
	groups map[string]*group
	table  map[string]*list.Element
	etags  map[string]string
}

// CacheStats keeps track of cache statistics
type CacheStats struct {
	EtagHits  int64
	CacheHits int64
	GetCalls  int64
	GetDupes  int64
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

// Delete the value indicated by the key, if it is present.
func (c *Bucket) Delete(group string, key string) {
	cacheKey := group + key
	c.Lock()

	elt, ok := c.table[cacheKey]
	if !ok {
		c.Unlock()
		return
	}
	delete(c.table, cacheKey)
	v := c.list.Remove(elt).(*cacheValue)
	atomic.AddInt64(&c.stats.Size, -v.size())

	//delete the etag
	val, ok := c.etags[cacheKey]
	if !ok {
		c.Unlock()
		return
	}

	delete(c.etags, cacheKey)
	atomic.AddInt64(&c.stats.Size, -int64(len(cacheKey)+len(val)))
	c.Unlock()
}

// Get retrieves a value from the cache or nil if no value present.
func (c *Bucket) Get(ctx context.Context, group string, key string, etag string) ([]byte, string, error) {
	cacheKey := group + key
	c.Lock()

	//return if the etag matches the etag cache
	hash, _ := c.etags[cacheKey]
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
			atomic.AddInt64(&c.stats.Misses, 1)
			return nil, "", err
		}
		//now set the value from the do(key) call into the cache
		var newEtag string
		if dupe == false {
			newEtag = c.Set(group, key, value)
			atomic.AddInt64(&c.stats.GetCalls, 1)
		} else {
			//try to get etag from etag cache for dupe threads on same do(key)
			c.Lock()
			newEtag, _ = c.etags[cacheKey]
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
	var osz int64
	cacheKey := group + key
	c.Lock()

	oelt, ok := c.table[cacheKey]
	if ok {
		ov := c.list.Remove(oelt).(*cacheValue)
		osz = ov.size()
	}

	v := &cacheValue{cacheKey, value}
	elt := c.list.PushFront(v)
	c.table[cacheKey] = elt
	atomic.AddInt64(&c.stats.Size, v.size()-osz)

	//save the etag
	_, ok = c.etags[cacheKey]
	hash := fnv.New64a()
	hash.Write(value)
	hashstr := strconv.FormatUint(hash.Sum64(), 16)
	c.etags[cacheKey] = hashstr
	if !ok {
		atomic.AddInt64(&c.stats.Size, int64(len(cacheKey)+len(hashstr)))
	}

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
		v := c.list.Remove(elt).(*cacheValue)
		delete(c.table, v.key)
		c.stats.Size -= v.size()
	}
}
