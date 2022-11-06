// Copyright 2020 - 2022 Christopher Briscoe.  All rights reserved.

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
	"time"
	"unsafe"

	"github.com/cespare/xxhash/v2"
)

// NeverExpire is used to indicate that you want data in the specified group to never expire
var NeverExpire = time.Hour*24*365*10 ^ 11 // about 100 billion years

// CacheInfo stores the etag of the cache entry, when it expires and the cost in time that
// the getter function took to create the content
type CacheInfo struct {
	Etag    string
	Expires time.Time
	Cost    time.Duration
}

type cacheEntry struct {
	elem *list.Element
	info *CacheInfo
}

type cacheValue struct {
	key   string
	bytes []byte
}

const (
	cacheInfoSize  = int(unsafe.Sizeof(CacheInfo{}))
	cacheEntrySize = int(unsafe.Sizeof(cacheEntry{}))
	cacheValueSize = int(unsafe.Sizeof(cacheValue{}))
)

// Just an estimate
func (v *cacheEntry) size() int64 {
	return int64(cacheEntrySize + cacheInfoSize + len(v.info.Etag))
}

// Just an estimate
func (v *cacheValue) size() int64 {
	return int64(cacheValueSize + len(v.key) + len(v.bytes))
}

// Cacher is an interface for either an Bucket or WebCache
type Cacher interface {
	AddGroup(string, time.Duration, getter) error
	Delete(string, string)
	Get(context.Context, string, string, string) ([]byte, *CacheInfo, error)
	Set(string, string, []byte) *CacheInfo
	Stats() *CacheStats
}

// Bucket is a simple LRU cache with Etag support
type Bucket struct {
	sync.Mutex
	list   *list.List
	groups map[string]*group
	entry  map[string]*cacheEntry
	stats  CacheStats
}

// CacheStats keeps track of cache statistics
type CacheStats struct {
	EtagHits  int64
	CacheHits int64
	GetCalls  int64
	GetDupes  int64
	GetErrors int64
	GetMisses int64
	Capacity  int64
	Size      int64
}

// NewBucket creates a new Cache with a maximum size of capacity bytes.
func NewBucket(capacity int64) *Bucket {
	bucket := &Bucket{
		stats:  CacheStats{},
		list:   list.New(),
		groups: make(map[string]*group),
		entry:  make(map[string]*cacheEntry),
	}
	bucket.stats.Capacity = capacity
	return bucket
}

// AddGroup adds a new cache group with a getter function
func (c *Bucket) AddGroup(group string, maxAge time.Duration, getter getter) error {
	c.Lock()

	_, ok := c.groups[group]
	if ok {
		c.Unlock()
		return errors.New(group + " cache group already exists")
	}

	grp, err := newGroup(group, maxAge, getter)
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
	ent, ok := c.entry[key]
	if ok {
		delete(c.entry, key)
		v := c.list.Remove(ent.elem).(*cacheValue)
		atomic.AddInt64(&c.stats.Size, -v.size()-ent.size())
	}
}

// Delete the value indicated by the key, if it is present.
func (c *Bucket) Delete(group, key string) {
	cacheKey := group + key
	c.Lock()
	c.deleteKey(cacheKey)
	c.Unlock()
}

// Get retrieves a value from the cache or nil if no value present.
func (c *Bucket) Get(ctx context.Context, group, key, etag string) ([]byte, *CacheInfo, error) {
	cacheKey := group + key
	c.Lock()

	ent, ok := c.entry[cacheKey]
	if ok {
		// first check if the entry has expired
		if time.Now().After(ent.info.Expires) {
			c.deleteKey(cacheKey)
		} else if etag == ent.info.Etag {
			// return if the etag matches the etag cache
			c.Unlock()
			atomic.AddInt64(&c.stats.EtagHits, 1)
			return nil, ent.info, nil
		} else {
			// otherwise return the cached value
			value := ent.elem.Value.(*cacheValue).bytes
			c.list.MoveToFront(ent.elem)
			c.Unlock()
			atomic.AddInt64(&c.stats.CacheHits, 1)
			return value, ent.info, nil
		}
	}

	// no cache hit so call the do(key) function for the group
	grp, ok := c.groups[group]
	c.Unlock()
	if !ok {
		atomic.AddInt64(&c.stats.GetMisses, 1)
		return nil, nil, nil
	}

	return c.singleFlight(ctx, group, key, grp)
}

func (c *Bucket) singleFlight(ctx context.Context, group, key string, grp *group) ([]byte, *CacheInfo, error) {
	start := time.Now()
	value, dupe, err := grp.do(ctx, key)
	elapsed := time.Since(start)
	if err != nil {
		if !dupe {
			grp.finish(key)
		}
		atomic.AddInt64(&c.stats.GetErrors, 1)
		return nil, nil, err
	}
	// record a miss if the getter does not return bytes
	if value == nil {
		atomic.AddInt64(&c.stats.GetMisses, 1)
	}
	// now set the value from the do(key) call into the cache
	var info *CacheInfo
	if !dupe {
		info = c.internalSet(group, key, value, elapsed)
		grp.finish(key)
		atomic.AddInt64(&c.stats.GetCalls, 1)
	} else {
		// try to get info struct from the info cache.
		c.Lock()
		elem, ok := c.entry[group+key]
		if ok {
			info = elem.info
		}
		c.Unlock()
		atomic.AddInt64(&c.stats.GetDupes, 1)
	}
	return value, info, nil
}

// Set inserts some {key, value} into the cache.
func (c *Bucket) Set(group, key string, value []byte) *CacheInfo {
	return c.internalSet(group, key, value, time.Duration(0))
}

func (c *Bucket) internalSet(group, key string, value []byte, elapsed time.Duration) *CacheInfo {
	cacheKey := group + key
	c.Lock()

	c.deleteKey(cacheKey)

	// store the cache value
	v := &cacheValue{cacheKey, value}
	elem := c.list.PushFront(v)

	// calculate the etag based of the hash sum of the data
	hashstr := strconv.FormatUint(xxhash.Sum64(value), 16)

	// get the maxAge for the given group.  if no group is found then it never expires
	maxAge := NeverExpire
	grp, ok := c.groups[group]
	if ok {
		maxAge = grp.maxAge
	}

	// store etag and link to the cache value in the key lookup map
	ent := &cacheEntry{
		elem: elem,
		info: &CacheInfo{
			Etag:    hashstr,
			Expires: time.Now().Add(maxAge),
			Cost:    elapsed,
		},
	}

	c.entry[cacheKey] = ent
	atomic.AddInt64(&c.stats.Size, v.size()+ent.size())

	c.trim()
	c.Unlock()
	return ent.info
}

// Stats returns statistics about this Bucket
func (c *Bucket) Stats() *CacheStats {
	return &CacheStats{
		EtagHits:  atomic.LoadInt64(&c.stats.EtagHits),
		CacheHits: atomic.LoadInt64(&c.stats.CacheHits),
		GetCalls:  atomic.LoadInt64(&c.stats.GetCalls),
		GetDupes:  atomic.LoadInt64(&c.stats.GetDupes),
		GetErrors: atomic.LoadInt64(&c.stats.GetErrors),
		GetMisses: atomic.LoadInt64(&c.stats.GetMisses),
		Capacity:  atomic.LoadInt64(&c.stats.Capacity),
		Size:      atomic.LoadInt64(&c.stats.Size),
	}
}

// If the cache is over capacity, clear elements (starting at the end of the list) until it is back under
// capacity. Note that this method is not threadsafe (it should only be called from other methods which
// already hold the lock).
func (c *Bucket) trim() {
	sz := atomic.LoadInt64(&c.stats.Size)
	cp := atomic.LoadInt64(&c.stats.Capacity)

	for sz > cp {
		elt := c.list.Back()
		if elt == nil {
			break
		}

		v := c.list.Remove(elt).(*cacheValue)
		elem := c.entry[v.key]
		delete(c.entry, v.key)

		eltSize := elem.size() + v.size()
		sz -= eltSize
		atomic.AddInt64(&c.stats.Size, -eltSize)
	}
}
