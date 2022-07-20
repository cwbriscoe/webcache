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

	"github.com/cespare/xxhash/v2"
)

// NeverExpire is used to indicate that you want data in the specified group to never expire
var NeverExpire = time.Hour*24*365*10 ^ 11 // about 100 billion years

type cacheEntry struct {
	elem    *list.Element
	etag    string
	expires time.Time
}

// Just an estimate
func (v *cacheEntry) size() int64 {
	return int64(8 + len(v.etag))
}

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
	AddGroup(string, time.Duration, getter) error
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

	ent, ok := c.entry[cacheKey]
	if ok {
		// first check if the entry has expired
		if time.Now().After(ent.expires) {
			c.deleteKey(cacheKey)
			goto callGetter
		}
		// return if the etag matches the etag cache
		if etag == ent.etag {
			c.Unlock()
			atomic.AddInt64(&c.stats.EtagHits, 1)
			return nil, etag, nil
		}
		// otherwise return the cached value
		value := ent.elem.Value.(*cacheValue).bytes
		c.list.MoveToFront(ent.elem)
		c.Unlock()
		atomic.AddInt64(&c.stats.CacheHits, 1)
		return value, ent.etag, nil
	}

callGetter: // this label is used when old cache entry has expired

	// no cache hit so call the do(key) function for the group
	grp, ok := c.groups[group]
	c.Unlock()
	if !ok {
		atomic.AddInt64(&c.stats.GetMisses, 1)
		return nil, "", nil
	}
	value, dupe, err := grp.do(ctx, key)
	if err != nil {
		atomic.AddInt64(&c.stats.GetErrors, 1)
		return nil, "", err
	}
	// record a miss if the getter does not return bytes
	if value == nil {
		atomic.AddInt64(&c.stats.GetMisses, 1)
	}
	// now set the value from the do(key) call into the cache
	var newEtag string
	if !dupe {
		newEtag = c.Set(group, key, value)
		atomic.AddInt64(&c.stats.GetCalls, 1)
	} else {
		// try to get etag from etag cache for dupe threads on same do(key).
		// sleep very shortly to allow the do/getter thread time to call Set
		// and update the cacheEntry with the etag.
		time.Sleep(time.Millisecond)
		c.Lock()
		elem, ok := c.entry[cacheKey]
		if ok {
			newEtag = elem.etag
		}
		c.Unlock()
		atomic.AddInt64(&c.stats.GetDupes, 1)
	}
	return value, newEtag, nil
}

// Set inserts some {key, value} into the cache.
func (c *Bucket) Set(group string, key string, value []byte) string {
	cacheKey := group + key
	c.Lock()

	c.deleteKey(cacheKey)

	// store the cache value
	v := &cacheValue{cacheKey, value}
	elem := c.list.PushFront(v)

	// calculate the etag based of the hash sum of the data
	hash := xxhash.New()
	hash.Write(value)
	hashstr := strconv.FormatUint(hash.Sum64(), 16)

	// get the maxAge for the given group.  if no group is found then it never expires
	maxAge := NeverExpire
	grp, ok := c.groups[group]
	if ok {
		maxAge = grp.maxAge
	}

	// store etag and link to the cache value in the key lookup map
	e := &cacheEntry{elem: elem, etag: hashstr, expires: time.Now().Add(maxAge)}
	c.entry[cacheKey] = e
	atomic.AddInt64(&c.stats.Size, v.size()+e.size())

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
		GetMisses: atomic.LoadInt64(&c.stats.GetMisses),
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
		elem := c.entry[v.key]
		delete(c.entry, v.key)
		c.stats.Size -= (elem.size() + v.size())
	}
}
