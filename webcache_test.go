// Copyright 2020 Christopher Briscoe.  All rights reserved.

package webcache

import (
	"context"
	"errors"
	"hash/fnv"
	"math/rand"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/cwbriscoe/testy"
)

func createWebCache(t *testing.T, capacity int64, buckets int) Cacher {
	cache := NewWebCache(capacity, buckets)
	if cache == nil {
		t.Errorf("NewWebCache() returned null")
	}
	return cache
}

func TestBucketCount(t *testing.T) {
	cache := NewWebCache(10000, 0)
	testy.Equals(t, cache.buckets, defaultBuckets)

	cache = NewWebCache(10000, 999)
	testy.Equals(t, cache.buckets, defaultBuckets)

	cache = NewWebCache(10000, 1)
	testy.Equals(t, cache.buckets, 1)

	cache = NewWebCache(10000, 2)
	testy.Equals(t, cache.buckets, 2)
}

func TestSimpleSet(t *testing.T) {
	cache := createWebCache(t, 10000, 8)
	key := "key"
	val := "TestSimpleSet"

	etag1 := cache.Set("", key, []byte(val))
	getval, etag2, err := cache.Get(nil, "", key, "")

	testy.Ok(t, err)
	testy.Equals(t, etag1, etag2)
	testy.Equals(t, val, string(getval))
}

func TestGroupSet(t *testing.T) {
	cache := createWebCache(t, 10000, 8)
	grp := "TestGroupSet"
	key := grp + "key"
	val := key + "value"

	etag1 := cache.Set(grp, key, []byte(val))
	getval, etag2, err := cache.Get(nil, grp, key, "")

	testy.Ok(t, err)
	testy.Equals(t, etag1, etag2)
	testy.Equals(t, val, string(getval))
}

func TestSimpleGetWrongKey(t *testing.T) {
	cache := createWebCache(t, 10000, 8)
	key := "key"
	val := "value"

	cache.Set("", key, []byte(val))
	getval, _, err := cache.Get(nil, "", "notkey", "")

	testy.Ok(t, err)
	testy.Nil(t, getval)
}

func TestTrim(t *testing.T) {
	cache := createWebCache(t, 64, 1)
	if cache.Stats().Capacity != 64 {
		t.Errorf("Expected capacity to be %d, but got '%d'", 64, cache.Stats().Capacity)
	}
	key := "key"
	val := "0123456789"

	etag := cache.Set("", key, []byte(val))
	esz := int64(len(key)*2 + len(val) + len(etag))
	sz := cache.Stats().Size
	testy.Equals(t, sz, esz)

	key = "abc"
	etag = cache.Set("", key, []byte(val))
	esz += int64(len(key)*2 + len(val) + len(etag))
	sz = cache.Stats().Size
	testy.Equals(t, sz, esz)

	key = "def"
	etag = cache.Set("", key, []byte(val))
	esz += int64(len(key)*2 + len(val) + len(etag))
	esz -= int64(len(key)+len(val)) * 3
	sz = cache.Stats().Size
	testy.Equals(t, sz, esz)
}

func TestTrimOverflow(t *testing.T) {
	cache := createWebCache(t, 10, 1)
	key := "key"
	val := "0123456789ABCDEF"

	cache.Set("", key, []byte(val))
}

type APITest1 struct {
}

func (a *APITest1) Get(ctx context.Context, key string) ([]byte, error) {
	value := key + key + key + key + key
	return []byte(value), nil
}

func TestGroupAdd(t *testing.T) {
	cache := createWebCache(t, 10000, 8)
	grp := "TestGroupGet"

	a := &APITest1{}
	err := cache.AddGroup(grp, a)
	testy.Ok(t, err)

	err = cache.AddGroup(grp, a)
	testy.NotOk(t, err)

	grp = "TestGroupGetNil"
	err = cache.AddGroup(grp, nil)
	testy.NotOk(t, err)
}

func TestGroupGet(t *testing.T) {
	cache := createWebCache(t, 10000, 8)
	grp := "TestGroupGet"
	key := grp + "key"

	a := &APITest1{}
	cache.AddGroup(grp, a)

	getval, etag1, err := cache.Get(nil, grp, key, "")
	testy.Ok(t, err)

	getval, etag2, err := cache.Get(nil, grp, key, "")
	testy.Ok(t, err)
	testy.Equals(t, etag1, etag2)
	testy.NotNil(t, getval)
}

type APITest2 struct {
}

func (a *APITest2) Get(ctx context.Context, key string) ([]byte, error) {
	if len(key) == 1 {
		return nil, errors.New("Invalid key: " + key)
	}

	value := key + key + key + key + key
	time.Sleep(100 * time.Millisecond)
	return []byte(value), nil
}

func TestGroupMultiGet(t *testing.T) {
	cache := createWebCache(t, 10000, 8)
	grp := "TestGroupGet"
	key := grp + "key"

	a := &APITest2{}
	cache.AddGroup(grp, a)

	f1 := func(t *testing.T) {
		t.Parallel()
		_, _, _ = cache.Get(nil, grp, key, "")
	}

	var cnt int64
	f2 := func(t *testing.T) {
		t.Parallel()
		_, _, _ = cache.Get(nil, grp, strconv.FormatInt(atomic.LoadInt64(&cnt), 10), "")
		atomic.AddInt64(&cnt, 1)
	}

	t.Run("group1", func(t *testing.T) {
		for i := 0; i < 10; i++ {
			t.Run(strconv.FormatInt(int64(i), 10), f1)
		}
	})

	t.Run("group2", func(t *testing.T) {
		for i := 0; i < 10; i++ {
			t.Run(strconv.FormatInt(int64(i), 10), f2)
		}
	})

	stats := cache.Stats()
	testy.Equals(t, stats.GetCalls, 1)
	testy.Equals(t, stats.GetDupes, 9)
	testy.Equals(t, stats.GetErrors, 10)
}

func TestGroupGetWrongKey(t *testing.T) {
	cache := createWebCache(t, 10000, 8)
	grp := "TestGroupGetWrongKey"
	key := grp + "key"
	val := key + "value"

	etag1 := cache.Set(grp, key, []byte(val))
	getval, etag2, err := cache.Get(nil, grp, "notkey", "")

	testy.Ok(t, err)
	testy.NotEquals(t, etag1, etag2)
	testy.Nil(t, getval)
}

func TestSimpleDelete(t *testing.T) {
	cache := createWebCache(t, 10000, 8)
	key := "key"
	val := "value"

	cache.Set("", key, []byte(val))
	cache.Delete("", key)
	getval, _, err := cache.Get(nil, "", key, "")

	testy.Ok(t, err)
	testy.Nil(t, getval)
}

func TestGroupDelete(t *testing.T) {
	cache := createWebCache(t, 10000, 8)
	grp := "TestGroupDelete"
	key := grp + "key"
	val := key + "value"

	cache.Set(grp, key, []byte(val))
	cache.Delete(grp, key)
	getval, etag, err := cache.Get(nil, grp, key, "")

	testy.Ok(t, err)
	testy.Equals(t, etag, "")
	testy.Nil(t, getval)
}

func TestStats(t *testing.T) {
	cache := createWebCache(t, 10000, 8)
	grp := "TestStats"
	key := grp + "key"
	val := key + "value"

	a := &APITest1{}
	cache.AddGroup(grp, a)

	cache.Set(grp, key, []byte(val))
	cache.Set(grp, key+"1", []byte(val))
	cache.Set(grp, key+"2", []byte(val))
	cache.Set(grp, key+"3", []byte(val))
	cache.Set(grp, key+"4", []byte(val))
	cache.Set(grp, key+"5", []byte(val))
	cache.Delete(grp, key)
	cache.Delete(grp, key+"1")
	cache.Delete(grp, key+"2")
	cache.Delete(grp, key+"3")
	cache.Delete(grp, key+"4")
	cache.Delete(grp, key+"5")
	size := cache.Stats().Size
	if size != 0 {
		t.Errorf("Expected Size to be zero: %d", size)
	}

	cache.Set(grp, key, []byte(val))
	cache.Set(grp, key, []byte(val+val))
	cache.Delete(grp, key)
	size = cache.Stats().Size
	testy.Equals(t, size, 0)

	etag := cache.Set(grp, key, []byte(val))
	_, _, _ = cache.Get(nil, grp, key, "")
	hits := cache.Stats().CacheHits
	testy.Equals(t, hits, 1)

	_, _, _ = cache.Get(nil, grp, key, etag)
	hits = cache.Stats().EtagHits
	testy.Equals(t, hits, 1)

	_, _, _ = cache.Get(nil, "", key+"1", etag)
	misses := cache.Stats().Misses
	testy.Equals(t, misses, 1)

	_, _, _ = cache.Get(nil, grp, key+"1", "")
	calls := cache.Stats().GetCalls
	testy.Equals(t, calls, 1)

	cache.Delete(grp, key)
	cache.Delete(grp, key+"1")
	size = cache.Stats().Size
	testy.Equals(t, size, 0)
}

var raceShardedCache = NewWebCache(10000, 8)

func TestRace(t *testing.T) {
	var wg sync.WaitGroup

	fn := func() {
		key := "somekey"
		v := []byte(strings.Repeat("X", 900))
		for i := 0; i < 5000; i++ {
			raceShardedCache.Set("", key, v)
			raceShardedCache.Stats() //to detect race condition, nothing else
			raceShardedCache.Get(nil, "", key, "")
			raceShardedCache.Delete("", key)
		}
		wg.Done()
	}

	for i := 0; i < runtime.NumCPU(); i++ {
		wg.Add(1)
		go fn()
	}

	wg.Wait()
}

func BenchmarkFnv(b *testing.B) {
	value := []byte(strings.Repeat("X", 1000))
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			hash := fnv.New64a()
			hash.Write(value)
			sum := hash.Sum64()
			hashstr := strconv.FormatUint(sum, 16)
			if len(hashstr) == 0 {
				b.Errorf("hashstring is zero")
			}
		}
	})
}

func BenchmarkXXHash(b *testing.B) {
	value := []byte(strings.Repeat("X", 1000))
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			hash := xxhash.New()
			hash.Write(value)
			sum := hash.Sum64()
			hashstr := strconv.FormatUint(sum, 16)
			if len(hashstr) == 0 {
				b.Errorf("hashstring is zero")
			}
		}
	})
}

func benchmarkRealSharded(b *testing.B, ratio int) {
	cache := NewWebCache(2000000, 24)
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			keynum := rand.Intn(1000)
			key := strconv.FormatUint(uint64(keynum), 10)
			if keynum < 100*ratio {
				sz := rand.Intn(20000) + 1
				val := []byte(strings.Repeat("X", sz))
				cache.Set("", key, val)
			} else {
				cache.Get(nil, "", key, "")
			}
		}
	})
}

func benchmarkRealNotSharded(b *testing.B, ratio int) {
	cache := NewBucket(2000000)
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			keynum := rand.Intn(1000)
			key := strconv.FormatUint(uint64(keynum), 10)
			if keynum < 100*ratio {
				sz := rand.Intn(20000) + 1
				val := []byte(strings.Repeat("X", sz))
				cache.Set("", key, val)
			} else {
				cache.Get(nil, "", key, "")
			}
		}
	})
}

func BenchmarkRealSharded1x(b *testing.B)    { benchmarkRealSharded(b, 1) }
func BenchmarkRealNotSharded1x(b *testing.B) { benchmarkRealNotSharded(b, 1) }
func BenchmarkRealSharded2x(b *testing.B)    { benchmarkRealSharded(b, 2) }
func BenchmarkRealNotSharded2x(b *testing.B) { benchmarkRealNotSharded(b, 2) }
func BenchmarkRealSharded3x(b *testing.B)    { benchmarkRealSharded(b, 3) }
func BenchmarkRealNotSharded3x(b *testing.B) { benchmarkRealNotSharded(b, 3) }
func BenchmarkRealSharded4x(b *testing.B)    { benchmarkRealSharded(b, 4) }
func BenchmarkRealNotSharded4x(b *testing.B) { benchmarkRealNotSharded(b, 4) }
func BenchmarkRealSharded5x(b *testing.B)    { benchmarkRealSharded(b, 5) }
func BenchmarkRealNotSharded5x(b *testing.B) { benchmarkRealNotSharded(b, 5) }
