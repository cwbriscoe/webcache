// Copyright 2020 - 2023 Christopher Briscoe.  All rights reserved.

package webcache

import (
	"context"
	"errors"
	"fmt"
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

func createWebCache(t *testing.T, config *Config) (CacheManager, error) {
	cache := NewWebCache(config)
	if cache == nil {
		t.Errorf("NewWebCache() returned null")
		return nil, errors.New("NewWebCache returned null")
	}
	return cache, nil
}

func TestBucketCount(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		capacity int64
		buckets  int
		expected int
	}{
		{10000, 0, defaultBuckets},
		{10000, 999, defaultBuckets},
		{10000, 1, 1},
		{10000, 2, 2},
	}

	for _, tc := range testCases {
		cache := NewWebCache(&Config{Capacity: tc.capacity, Buckets: tc.buckets})
		testy.NotNil(t, cache)
		testy.Equals(t, cache.buckets, tc.expected)
	}
}

func TestSimpleSet(t *testing.T) {
	t.Parallel()

	cache, err := createWebCache(t, &Config{Capacity: 10000, Buckets: 8})
	testy.Ok(t, err)
	key := "key"
	val := "TestSimpleSet"

	info1 := cache.Set("", key, []byte(val))
	getVal, info2, err := cache.Get(context.TODO(), "", key, "")

	testy.Ok(t, err)
	testy.Equals(t, info1.Etag, info2.Etag)
	testy.Equals(t, val, string(getVal))
}

func TestGroupSet(t *testing.T) {
	t.Parallel()

	cache, err := createWebCache(t, &Config{Capacity: 10000, Buckets: 8})
	testy.Ok(t, err)
	grp := "TestGroupSet"
	key := grp + "key"
	val := key + "value"

	info1 := cache.Set(grp, key, []byte(val))
	getVal, info2, err := cache.Get(context.TODO(), grp, key, "")

	testy.Ok(t, err)
	testy.Equals(t, info1.Etag, info2.Etag)
	testy.Equals(t, val, string(getVal))
}

func TestSimpleGetWrongKey(t *testing.T) {
	t.Parallel()

	cache, err := createWebCache(t, &Config{Capacity: 10000, Buckets: 8})
	testy.Ok(t, err)
	key := "key"
	val := "value"

	cache.Set("", key, []byte(val))
	getVal, _, err := cache.Get(context.TODO(), "", "not key", "")

	testy.Ok(t, err)
	testy.Nil(t, getVal)
}

func TestTrim(t *testing.T) {
	t.Parallel()

	cache, err := createWebCache(t, &Config{Capacity: 400, Buckets: 1})
	testy.Ok(t, err)
	if cache.Stats().Capacity.Load() != 400 {
		t.Errorf("Expected capacity to be %d, but got '%d'", 400, cache.Stats().Capacity.Load())
	}
	key := "key"
	val := "0123456789"

	baseSize := cacheValueSize + cacheEntrySize + cacheInfoSize + len(key) + len(val)
	baseSize += 1 // size of ":" in key:value cache key

	info := cache.Set("", key, []byte(val))
	esz := int64(baseSize + len(info.Etag))
	sz := cache.Stats().Size.Load()
	testy.Equals(t, sz, esz)

	key = "abc"
	cache.Set("", key, []byte(val))
	esz *= 2
	sz = cache.Stats().Size.Load()
	testy.Equals(t, sz, esz)

	key = "def"
	cache.Set("", key, []byte(val))
	stats := cache.Stats()
	testy.Equals(t, stats.Size.Load(), esz)
	testy.Equals(t, stats.TrimEntries.Load(), 1)
	testy.Equals(t, stats.TrimBytes.Load(), esz/2)
}

func TestTrimOverflow(t *testing.T) {
	t.Parallel()

	cache, err := createWebCache(t, &Config{Capacity: 10, Buckets: 1})
	testy.Ok(t, err)
	key := "key"
	val := "0123456789"

	cache.Set("", key, []byte(val))
}

type APITest1 struct{}

func (*APITest1) Get(_ context.Context, key string) ([]byte, error) {
	value := key + key + key + key + key
	return []byte(value), nil
}

func TestGroupAdd(t *testing.T) {
	t.Parallel()

	cache, err := createWebCache(t, &Config{Capacity: 10000, Buckets: 8})
	testy.Ok(t, err)
	grp := "TestGroupGet"

	a := &APITest1{}
	err = cache.AddGroup(grp, time.Hour, a)
	testy.Ok(t, err)

	err = cache.AddGroup(grp, time.Hour, a)
	testy.NotOk(t, err)

	grp = "TestGroupGetNil"
	err = cache.AddGroup(grp, time.Hour, nil)
	testy.NotOk(t, err)
}

func TestGroupGet(t *testing.T) {
	t.Parallel()

	cache, err := createWebCache(t, &Config{Capacity: 10000, Buckets: 8})
	testy.Ok(t, err)
	grp := "TestGroupGet"
	key := grp + "key"

	a := &APITest1{}
	err = cache.AddGroup(grp, time.Hour, a)
	testy.Ok(t, err)

	_, info1, err := cache.Get(context.TODO(), grp, key, "")
	testy.Ok(t, err)

	getVal, info2, err := cache.Get(context.TODO(), grp, key, "")

	testy.Ok(t, err)
	testy.Equals(t, info1.Etag, info2.Etag)
	testy.NotNil(t, getVal)
}

type APITest2 struct{}

func (*APITest2) Get(_ context.Context, key string) ([]byte, error) {
	if len(key) == 1 {
		return nil, errors.New("Invalid key: " + key)
	}

	value := key + key + key + key + key
	time.Sleep(100 * time.Millisecond)
	return []byte(value), nil
}

func TestGroupMultiGet(t *testing.T) {
	cache, err := createWebCache(t, &Config{Capacity: 10000, Buckets: 8})
	testy.Ok(t, err)
	grp := "TestGroupGet"
	key := grp + "key"

	a := &APITest2{}
	err = cache.AddGroup(grp, time.Hour, a)
	testy.Ok(t, err)

	f1 := func(t *testing.T) {
		t.Parallel()
		_, _, _ = cache.Get(context.TODO(), grp, key, "")
	}

	var cnt int64
	f2 := func(t *testing.T) {
		t.Parallel()
		_, _, _ = cache.Get(context.TODO(), grp, strconv.FormatInt(atomic.LoadInt64(&cnt), 10), "")
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
	testy.Equals(t, stats.GetCalls.Load(), 1)
	testy.Equals(t, stats.GetDupes.Load(), 9)
	testy.Equals(t, stats.GetErrors.Load(), 10)
}

type PanicTest struct{}

func (*PanicTest) Get(_ context.Context, _ string) ([]byte, error) {
	time.Sleep(100 * time.Millisecond)
	panic("oh no, something bad happened")
}

func TestPanicInGetter(t *testing.T) {
	t.Parallel()

	cache, err := createWebCache(t, &Config{Capacity: 10000, Buckets: 8})
	testy.Ok(t, err)
	grp := "TestPanicInGetter"
	key := grp + "key"

	a := &PanicTest{}
	err = cache.AddGroup(grp, time.Hour, a)
	testy.Ok(t, err)

	f1 := func(t *testing.T) {
		t.Parallel()
		_, _, _ = cache.Get(context.TODO(), grp, key, "")
	}

	t.Run("group1", func(t *testing.T) {
		for i := 0; i < 10; i++ {
			t.Run(strconv.FormatInt(int64(i), 10), f1)
		}
	})

	stats := cache.Stats()
	testy.Equals(t, stats.GetErrors.Load(), 10)
}

func TestGroupGetWrongKey(t *testing.T) {
	t.Parallel()

	cache, err := createWebCache(t, &Config{Capacity: 10000, Buckets: 8})
	testy.Ok(t, err)
	grp := "TestGroupGetWrongKey"
	key := grp + "key"
	val := key + "value"

	info1 := cache.Set(grp, key, []byte(val))
	getVal, info2, err := cache.Get(context.TODO(), grp, "not key", "")

	testy.Ok(t, err)
	testy.Assert(t, info1 != nil, "expected info2 to not be nil")
	testy.Assert(t, info2 == nil, "expected info2 to be nil")
	testy.Nil(t, getVal)
}

func TestSimpleDelete(t *testing.T) {
	t.Parallel()

	cache, err := createWebCache(t, &Config{Capacity: 10000, Buckets: 8})
	testy.Ok(t, err)
	key := "key"
	val := "value"

	cache.Set("", key, []byte(val))
	cache.Delete("", key)
	getVal, _, err := cache.Get(context.TODO(), "", key, "")

	testy.Ok(t, err)
	testy.Nil(t, getVal)
}

func TestGroupDelete(t *testing.T) {
	t.Parallel()

	cache, err := createWebCache(t, &Config{Capacity: 10000, Buckets: 8})
	testy.Ok(t, err)
	grp := "TestGroupDelete"
	key := grp + "key"
	val := key + "value"

	cache.Set(grp, key, []byte(val))
	cache.Delete(grp, key)
	getVal, info, err := cache.Get(context.TODO(), grp, key, "")

	t.Log(info)

	testy.Ok(t, err)
	testy.Assert(t, info == nil, "info should be nil")
	testy.Nil(t, getVal)
}

func TestStats(t *testing.T) {
	t.Parallel()

	cache, err := createWebCache(t, &Config{Capacity: 10000, Buckets: 8})
	testy.Ok(t, err)
	grp := "TestStats"
	key := grp + "key"
	val := key + "value"

	a := &APITest1{}
	err = cache.AddGroup(grp, time.Hour, a)
	testy.Ok(t, err)

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
	size := cache.Stats().Size.Load()
	if size != 0 {
		t.Errorf("Expected Size to be zero: %d", size)
	}

	cache.Set(grp, key, []byte(val))
	cache.Set(grp, key, []byte(val+val))
	cache.Delete(grp, key)
	size = cache.Stats().Size.Load()
	testy.Equals(t, size, 0)

	info := cache.Set(grp, key, []byte(val))
	_, _, _ = cache.Get(context.TODO(), grp, key, "")
	hits := cache.Stats().CacheHits.Load()
	testy.Equals(t, hits, 1)

	_, _, _ = cache.Get(context.TODO(), grp, key, info.Etag)
	hits = cache.Stats().EtagHits.Load()
	testy.Equals(t, hits, 1)

	_, _, _ = cache.Get(context.TODO(), "", key+"1", info.Etag)
	misses := cache.Stats().GetMisses.Load()
	testy.Equals(t, misses, 1)

	_, _, _ = cache.Get(context.TODO(), grp, key+"1", "")
	calls := cache.Stats().GetCalls.Load()
	testy.Equals(t, calls, 1)

	cache.Delete(grp, key)
	cache.Delete(grp, key+"1")
	size = cache.Stats().Size.Load()
	testy.Equals(t, size, 0)
}

func TestExpires1(t *testing.T) {
	t.Parallel()

	cache, err := createWebCache(t, &Config{Capacity: 10000, Buckets: 8})
	testy.Ok(t, err)
	grp := "TestExpires1"
	key := grp + "key"

	a := &APITest1{}
	err = cache.AddGroup(grp, time.Nanosecond, a)
	testy.Ok(t, err)

	_, _, _ = cache.Get(context.TODO(), grp, key, "")
	time.Sleep(time.Nanosecond)
	_, _, _ = cache.Get(context.TODO(), grp, key, "")

	gets := cache.Stats().GetCalls.Load()
	testy.Equals(t, gets, 2)
}

func TestExpires2(t *testing.T) {
	t.Parallel()

	cache, err := createWebCache(t, &Config{Capacity: 10000, Buckets: 8})
	testy.Ok(t, err)
	grp := "TestExpires2"
	key := grp + "key"

	a := &APITest1{}
	err = cache.AddGroup(grp, NeverExpire, a)
	testy.Ok(t, err)

	_, _, _ = cache.Get(context.TODO(), grp, key, "")
	time.Sleep(time.Nanosecond)
	_, _, _ = cache.Get(context.TODO(), grp, key, "")

	gets := cache.Stats().GetCalls.Load()
	testy.Equals(t, gets, 1)

	hits := cache.Stats().CacheHits.Load()
	testy.Equals(t, hits, 1)
}

func TestExpiresEtag(t *testing.T) {
	t.Parallel()

	cache, err := createWebCache(t, &Config{Capacity: 10000, Buckets: 8})
	testy.Ok(t, err)
	grp := "TestExpiresEtag"
	key := grp + "key"

	a := &APITest1{}
	err = cache.AddGroup(grp, time.Nanosecond, a)
	testy.Ok(t, err)

	_, info1, _ := cache.Get(context.TODO(), grp, key, "")
	time.Sleep(time.Nanosecond)
	_, info2, _ := cache.Get(context.TODO(), grp, key, "")

	gets := cache.Stats().GetCalls.Load()

	testy.NotEquals(t, info1.Etag, "")
	testy.NotEquals(t, info2.Etag, "")
	testy.Equals(t, info1.Etag, info2.Etag)
	testy.Equals(t, gets, 2)
}

var raceShardedCache = NewWebCache(&Config{Capacity: 10000, Buckets: 8})

func TestRace(t *testing.T) {
	t.Parallel()

	var wg sync.WaitGroup

	fn := func() {
		key := "some key"
		v := []byte(strings.Repeat("X", 900))
		for i := 0; i < 5000; i++ {
			raceShardedCache.Set("", key, v)
			raceShardedCache.Stats() // to detect race condition, nothing else
			_, _, err := raceShardedCache.Get(context.TODO(), "", key, "")
			testy.Ok(t, err)
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

// Test a race that happened when accessing CacheInfo during single flight
type APIRaceTestCacheInfo struct{}

func (*APIRaceTestCacheInfo) Get(_ context.Context, key string) ([]byte, error) {
	if len(key) == 0 {
		return nil, errors.New("Invalid key: " + key)
	}

	value := key + key + key + key + key
	return []byte(value), nil
}

func TestRaceCacheInfo(t *testing.T) {
	t.Parallel()

	cache, err := createWebCache(t, &Config{Capacity: 10000, Buckets: 8})
	testy.Ok(t, err)
	grp := "TestRaceTestCacheInfo"
	key := grp + "key"

	a := &APIRaceTestCacheInfo{}
	err = cache.AddGroup(grp, time.Hour, a)
	testy.Ok(t, err)

	f1 := func(t *testing.T) {
		t.Parallel()
		_, info, err := cache.Get(context.TODO(), grp, key, "")
		testy.NotNil(t, info)
		testy.Ok(t, err)
		val := info.Cost.String()
		testy.NotNil(t, val)
	}

	t.Run("RaceTestCacheInfo", func(t *testing.T) {
		for i := 0; i < 10; i++ {
			t.Run(strconv.FormatInt(int64(i), 10), f1)
		}
	})
}

// TestRaceExpires will ensure that Get does not return a nil data or info
// value when expires is set extremely low and there are several concurrent
// single flight requests

type APITestRaceExpires struct{}

func (*APITestRaceExpires) Get(_ context.Context, _ string) ([]byte, error) {
	time.Sleep(1 * time.Nanosecond)
	return []byte("data"), nil
}

func TestRaceExpires(t *testing.T) {
	t.Parallel()

	cache, err := createWebCache(t, &Config{Capacity: 10000, Buckets: 8})
	testy.Ok(t, err)
	grp := "TestRaceExpires"
	key := grp + "key"

	a := &APITestRaceExpires{}
	err = cache.AddGroup(grp, time.Nanosecond, a)
	testy.Ok(t, err)

	f1 := func(t *testing.T) {
		t.Parallel()
		data, info, err := cache.Get(context.TODO(), grp, key, "")
		testy.Ok(t, err)
		testy.NotNil(t, info)
		testy.NotNil(t, data)
		testy.Equals(t, string(data), "data")
	}

	t.Run("TestRaceExpires", func(t *testing.T) {
		for i := 0; i < 10000; i++ {
			t.Run(strconv.FormatInt(int64(i), 10), f1)
		}
	})
}

func BenchmarkTimeNow(b *testing.B) {
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_ = time.Now()
		}
	})
}

func BenchmarkFnv(b *testing.B) {
	value := []byte(strings.Repeat("X", 1000))
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			hash := fnv.New64a()
			_, _ = hash.Write(value)
			sum := hash.Sum64()
			hashStr := strconv.FormatUint(sum, 16)
			if len(hashStr) == 0 {
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
			_, _ = hash.Write(value)
			sum := hash.Sum64()
			hashStr := strconv.FormatUint(sum, 16)
			if len(hashStr) == 0 {
				b.Errorf("hashstring is zero")
			}
		}
	})
}

func benchmarkRealSharded(b *testing.B, ratio int) {
	cache := NewWebCache(&Config{Capacity: 10000, Buckets: 24})
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			keyNum := rand.Intn(1000)
			key := strconv.FormatUint(uint64(keyNum), 10)
			if keyNum < 100*ratio {
				sz := rand.Intn(20000) + 1
				val := []byte(strings.Repeat("X", sz))
				cache.Set("", key, val)
			} else {
				_, _, _ = cache.Get(context.TODO(), "", key, "")
			}
		}
	})
}

func benchmarkRealNotSharded(b *testing.B, ratio int) {
	cache := NewBucket(2000000)
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			keyNum := rand.Intn(1000)
			key := strconv.FormatUint(uint64(keyNum), 10)
			if keyNum < 100*ratio {
				sz := rand.Intn(20000) + 1
				val := []byte(strings.Repeat("X", sz))
				cache.Set("", key, val)
			} else {
				_, _, _ = cache.Get(context.TODO(), "", key, "")
			}
		}
	})
}

type profileGetter struct {
	data [10][]byte
	once sync.Once
}

func (g *profileGetter) Get(_ context.Context, _ string) ([]byte, error) {
	g.once.Do(func() {
		for i := 0; i < 10; i++ {
			g.data[i] = []byte(strings.Repeat("X", 5000*i))
		}
	})
	random := rand.Intn(10)
	return g.data[random], nil
}

func BenchmarkForCPUProfileWebCache(b *testing.B) {
	cache := NewWebCache(&Config{Capacity: 10000, Buckets: 1})
	_ = cache.AddGroup("group", time.Hour, &profileGetter{})
	var keys [1000]string
	for i := 0; i < 1000; i++ {
		keys[i] = strconv.FormatUint(uint64(i), 10)
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			random := rand.Intn(1000)
			_, _, _ = cache.Get(context.TODO(), "group", keys[random], "")
			// not necessary to add a call to Set() since Get calls it often
		}
	})
}

func BenchmarkForCPUProfileBucket(b *testing.B) {
	cache := NewBucket(100000)
	_ = cache.AddGroup("group", time.Hour, &profileGetter{})
	var keys [1000]string
	for i := 0; i < 1000; i++ {
		keys[i] = strconv.FormatUint(uint64(i), 10)
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			random := rand.Intn(1000)
			_, _, _ = cache.Get(context.TODO(), "group", keys[random], "")
			// not necessary to add a call to Set() since Get calls it often
			random = rand.Intn(1000)
			cache.Delete("group", keys[random])
		}
	})
}

func BenchmarkReal(b *testing.B) {
	for _, multiplier := range []int{1, 2, 3, 4, 5} {
		b.Run(fmt.Sprintf("Sharded%dX", multiplier), func(b *testing.B) {
			benchmarkRealSharded(b, multiplier)
		})
		b.Run(fmt.Sprintf("NotSharded%dX", multiplier), func(b *testing.B) {
			benchmarkRealNotSharded(b, multiplier)
		})
	}
}
