//Copyright 2020 Christopher Briscoe.  All rights reserved.

// Package webcache is A simple LRU cache for storing documents ([]byte). When the size maximum is reached,
// items are evicted starting with the least recently used. This data structure is goroutine-safe (it has
// a lock around all operations).
package webcache

import (
	"container/list"
	"hash/fnv"
	"strconv"
	"sync"
)

type cacheValue struct {
	key   string
	bytes []byte
}

// Just an estimate
func (v *cacheValue) size() uint64 {
	return uint64(len(v.key) + len(v.bytes))
}

// Cacher is an interface for either an Bucket or WebCache
type Cacher interface {
	Set(string, []byte) string
	Get(string, string) ([]byte, string)
	Delete(string)
	Size() uint64
}

// Bucket is a simple LRU cache with Etag support
type Bucket struct {
	sync.Mutex
	size     uint64
	capacity uint64
	list     *list.List
	table    map[string]*list.Element
	etags    map[string]string
}

// NewBucket creates a new Cache with a maximum size of capacity bytes.
func NewBucket(capacity uint64) *Bucket {
	return &Bucket{
		capacity: capacity,
		list:     list.New(),
		table:    make(map[string]*list.Element),
		etags:    make(map[string]string),
	}
}

// Set inserts some {key, value} into the cache.
func (c *Bucket) Set(key string, value []byte) string {
	var osz uint64

	c.Lock()

	oelt, ok := c.table[key]
	if ok {
		ov := c.list.Remove(oelt).(*cacheValue)
		osz = ov.size()
	}

	v := &cacheValue{key, value}
	elt := c.list.PushFront(v)
	c.table[key] = elt
	c.size += v.size() - osz

	//save the etag
	_, ok = c.etags[key]
	hash := fnv.New64a()
	hash.Write(value)
	hashstr := strconv.FormatUint(hash.Sum64(), 16)
	c.etags[key] = hashstr
	if !ok {
		c.size += uint64(len(key) + len(hashstr))
	}

	c.trim()

	c.Unlock()

	return hashstr
}

// Get retrieves a value from the cache or nil if no value present.
func (c *Bucket) Get(key string, etag string) ([]byte, string) {
	c.Lock()

	//return if the etag matches the etag cache
	hash, _ := c.etags[key]
	if etag == hash {
		c.Unlock()
		return nil, etag
	}

	//otherwise see if we get a cache hit
	elt, ok := c.table[key]
	if !ok {
		//no hit so return nil/"" since etag will be recalculated after the key is set
		c.Unlock()
		return nil, ""
	}

	value := elt.Value.(*cacheValue).bytes

	c.list.MoveToFront(elt)

	c.Unlock()

	return value, hash
}

// Delete the value indicated by the key, if it is present.
func (c *Bucket) Delete(key string) {
	c.Lock()

	elt, ok := c.table[key]
	if !ok {
		c.Unlock()
		return
	}
	delete(c.table, key)
	v := c.list.Remove(elt).(*cacheValue)
	c.size -= v.size()

	//delete the etag
	val, ok := c.etags[key]
	if !ok {
		c.Unlock()
		return
	}
	delete(c.etags, key)
	c.size -= uint64(len(key) + len(val))

	c.Unlock()
}

// Size returns an approximate size of the cache
func (c *Bucket) Size() uint64 {
	c.Lock()
	sz := c.size
	c.Unlock()
	return sz
}

// If the cache is over capacity, clear elements (starting at the end of the list) until it is back under
// capacity. Note that this method is not threadsafe (it should only be called from other methods which
// already hold the lock).
func (c *Bucket) trim() {
	for c.size > c.capacity {
		elt := c.list.Back()
		v := c.list.Remove(elt).(*cacheValue)
		delete(c.table, v.key)
		c.size -= v.size()
	}
}
