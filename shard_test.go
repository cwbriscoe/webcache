//Copyright 2020 Christopher Briscoe.  All rights reserved.
package webcache

import (
	"runtime"
	"strings"
	"sync"
	"testing"
)

func createShard(t *testing.T, capacity uint64) Cacher {
	cache := NewShard(capacity)
	if cache == nil {
		t.Errorf("NewCache() returned null")
	}
	return cache
}

func (c *Shard) getWithDeferredLock(key string) []byte {
	defer c.Unlock()
	c.Lock()

	elt, ok := c.table[key]
	if !ok {
		return nil
	}
	c.list.MoveToFront(elt)
	return elt.Value.(*cacheValue).bytes
}

func (c *Shard) getWithNoLock(key string) []byte {
	elt, ok := c.table[key]
	if !ok {
		return nil
	}
	c.list.MoveToFront(elt)
	return elt.Value.(*cacheValue).bytes
}

func (c *Shard) sizeWithDeferredLock() uint64 {
	defer c.Unlock()
	c.Lock()
	return c.size
}

func (c *Shard) sizeWithNoLock() uint64 {
	return c.size
}

func TestSet(t *testing.T) {
	cache := createShard(t, 1000)
	key := "key"
	val := "value"

	etag := cache.Set(key, []byte(val))
	getval, etag2 := cache.Get(key, "")

	if val != string(getval) {
		t.Errorf("Expected Set() value to be %s instead of %s", val, string(getval))
	}

	if etag != etag2 {
		t.Errorf("Expected Set() etag value to be %s instead of %s", etag, etag2)
	}
}

func TestGet(t *testing.T) {
	cache := createShard(t, 1000)
	key := "key"
	val := "value"

	cache.Set(key, []byte(val))
	getval, _ := cache.Get("notkey", "")

	if getval != nil {
		t.Errorf("Expected Get() value to be nil instead of %s", string(getval))
	}
}

func TestDelete(t *testing.T) {
	cache := createShard(t, 1000)
	key := "key"
	val := "value"

	cache.Set(key, []byte(val))
	cache.Delete(key)
	getval, _ := cache.Get(key, "")

	if getval != nil {
		t.Errorf("Expected Delete() value to be deleted")
	}
}

var raceCache = NewShard(1000)

func TestRace(t *testing.T) {
	var wg sync.WaitGroup

	fn := func() {
		key := "somekey"
		v := []byte(strings.Repeat("X", 900))
		for i := 0; i < 5000; i++ {
			raceCache.Set(key, v)
			raceCache.Size() //to detect race condition, nothing else
			val, _ := raceCache.Get(key, "")
			if val != nil {
				l1 := len(v)
				l2 := len(val)
				if l1 != l2 {
					t.Errorf("Race condition detected, expeced len of %d, got %d", l1, l2)
				}
			}
			raceCache.Delete(key)
		}
		wg.Done()
	}

	for i := 0; i < runtime.NumCPU(); i++ {
		wg.Add(1)
		go fn()
	}

	wg.Wait()
}

func BenchmarkDeferGet(b *testing.B) {
	cache := NewShard(1000)
	v := []byte(strings.Repeat("X", 900))
	cache.Set("key", v)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cache.getWithDeferredLock("key")
	}
}

func BenchmarkDeferEmptyGet(b *testing.B) {
	cache := NewShard(1000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cache.getWithDeferredLock("missingkey")
	}
}

func BenchmarkDeferSize(b *testing.B) {
	cache := NewShard(1000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cache.sizeWithDeferredLock()
	}
}

func BenchmarkNoDeferGet(b *testing.B) {
	cache := NewShard(1000)
	v := []byte(strings.Repeat("X", 900))
	cache.Set("key", v)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cache.Get("key", "")
	}
}

func BenchmarkNoDeferEmptyGet(b *testing.B) {
	cache := NewShard(1000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cache.Get("missingkey", "")
	}
}

func BenchmarkNoDeferSize(b *testing.B) {
	cache := NewShard(1000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cache.Size()
	}
}

func BenchmarkNoLockGet(b *testing.B) {
	cache := NewShard(1000)
	v := []byte(strings.Repeat("X", 900))
	cache.Set("key", v)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cache.getWithNoLock("key")
	}
}

func BenchmarkNoLockEmptyGet(b *testing.B) {
	cache := NewShard(1000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cache.getWithNoLock("missingkey")
	}
}

func BenchmarkNoLockSize(b *testing.B) {
	cache := NewShard(1000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cache.sizeWithNoLock()
	}
}