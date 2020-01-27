//Copyright 2020 Christopher Briscoe.  All rights reserved.

package webcache

import (
	"context"
	"crypto/sha1"
	"hash/fnv"
	"math/rand"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
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
	if cache.buckets != defaultBuckets {
		t.Errorf("Expected defaultBucket when setting buckets to 0")
	}

	cache = NewWebCache(10000, 999)
	if cache.buckets != defaultBuckets {
		t.Errorf("Expected defaultBucket when setting buckets to > 256")
	}

	cache = NewWebCache(10000, 1)
	if cache.buckets != 1 {
		t.Errorf("Expected buckets to be 1 when setting buckets to 1")
	}

	cache = NewWebCache(10000, 2)
	if cache.buckets != 2 {
		t.Errorf("Expected buckets to be 2 when setting buckets to 2")
	}
}

func TestSimpleSet(t *testing.T) {
	cache := createWebCache(t, 10000, 8)
	key := "key"
	val := "TestSimpleSet"

	etag1 := cache.Set("", key, []byte(val))
	getval, etag2, err := cache.Get(nil, "", key, "")

	if err != nil {
		t.Errorf("Did not expect Get() to return an error: '%s'", err)
	}

	if etag1 != etag2 {
		t.Errorf("Expected etag1 to equal etag2: '%s' '%s'", etag1, etag2)
	}

	if val != string(getval) {
		t.Errorf("Expected Set() value to be '%s' instead of '%s'", val, string(getval))
	}
}

func TestGroupSet(t *testing.T) {
	cache := createWebCache(t, 10000, 8)
	grp := "TestGroupSet"
	key := grp + "key"
	val := key + "value"

	etag1 := cache.Set(grp, key, []byte(val))
	getval, etag2, err := cache.Get(nil, grp, key, "")

	if err != nil {
		t.Errorf("Did not expect Get() to return an error: '%s'", err)
	}

	if etag1 != etag2 {
		t.Errorf("Expected etag1 to equal etag2: '%s' '%s'", etag1, etag2)
	}

	if val != string(getval) {
		t.Errorf("Expected Set() value to be '%s' instead of '%s'", val, string(getval))
	}
}

func TestSimpleGetWrongKey(t *testing.T) {
	cache := createWebCache(t, 10000, 8)
	key := "key"
	val := "value"

	cache.Set("", key, []byte(val))
	getval, _, err := cache.Get(nil, "", "notkey", "")

	if err != nil {
		t.Errorf("Did not expect Get() to return an error: '%s'", err)
	}

	if getval != nil {
		t.Errorf("Expected Get() value to be nil instead of '%s'", string(getval))
	}
}

type APITest1 struct {
}

func (a *APITest1) get(ctx context.Context, key string) ([]byte, error) {
	value := key + key + key + key + key
	return []byte(value), nil
}

func TestGroupGet(t *testing.T) {
	cache := createWebCache(t, 10000, 8)
	grp := "TestGroupGet"
	key := grp + "key"

	a := &APITest1{}
	cache.AddGroup(grp, a)

	getval, etag1, err := cache.Get(nil, grp, key, "")
	if err != nil {
		t.Errorf("Did not expect Get() to return an error: '%s'", err)
	}

	getval, etag2, err := cache.Get(nil, grp, key, "")
	if err != nil {
		t.Errorf("Did not expect Get() to return an error: '%s'", err)
	}

	if etag1 != etag2 {
		t.Errorf("Expected etag1 to equal etag2: '%s' '%s'", etag1, etag2)
	}

	if getval == nil {
		t.Errorf("Expected Get() value to not be nil")
	}
}

type APITest2 struct {
}

func (a *APITest2) get(ctx context.Context, key string) ([]byte, error) {
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

	f := func(t *testing.T) {
		t.Parallel()
		_, _, _ = cache.Get(nil, grp, key, "")
		//fmt.Println(etag, string(getval))
	}

	t.Run("group", func(t *testing.T) {
		for i := 0; i < 10; i++ {
			//fmt.Println("Submitting %n", i)
			t.Run(strconv.FormatInt(int64(i), 10), f)
		}
	})
}

func TestGroupGetWrongKey(t *testing.T) {
	cache := createWebCache(t, 10000, 8)
	grp := "TestGroupGetWrongKey"
	key := grp + "key"
	val := key + "value"

	etag1 := cache.Set(grp, key, []byte(val))
	getval, etag2, err := cache.Get(nil, grp, "notkey", "")

	if err != nil {
		t.Errorf("Did not expect Get() to return an error: '%s'", err)
	}

	if etag1 == etag2 {
		t.Errorf("Expected etag1 to not equal etag2: '%s' '%s'", etag1, etag2)
	}

	if getval != nil {
		t.Errorf("Expected Get() value to be nil instead of '%s'", string(getval))
	}
}

func TestSimpleDelete(t *testing.T) {
	cache := createWebCache(t, 10000, 8)
	key := "key"
	val := "value"

	cache.Set("", key, []byte(val))
	cache.Delete("", key)
	getval, _, err := cache.Get(nil, "", key, "")

	if err != nil {
		t.Errorf("Did not expect Get() to return an error: %s", err)
	}

	if getval != nil {
		t.Errorf("Expected Delete() value to be deleted")
	}
}

func TestGroupDelete(t *testing.T) {
	cache := createWebCache(t, 10000, 8)
	grp := "TestGroupDelete"
	key := grp + "key"
	val := key + "value"

	cache.Set(grp, key, []byte(val))
	cache.Delete(grp, key)
	getval, etag, err := cache.Get(nil, grp, key, "")

	if err != nil {
		t.Errorf("Did not expect Get() to return an error: %s", err)
	}

	if etag != "" {
		t.Errorf("Did not expect etag to have a value: '%s'", etag)
	}

	if getval != nil {
		t.Errorf("Expected Delete() value to be deleted")
	}
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
	if size != 0 {
		t.Errorf("Expected Size to be zero: %d", size)
	}

	etag := cache.Set(grp, key, []byte(val))
	_, _, _ = cache.Get(nil, grp, key, "")
	hits := cache.Stats().CacheHits
	if hits != 1 {
		t.Errorf("Expected CacheHits to be one: %d", hits)
	}

	_, _, _ = cache.Get(nil, grp, key, etag)
	hits = cache.Stats().EtagHits
	if hits != 1 {
		t.Errorf("Expected EtagHits to be one: %d", hits)
	}

	_, _, _ = cache.Get(nil, "", key+"1", etag)
	misses := cache.Stats().Misses
	if misses != 1 {
		t.Errorf("Expected Misses to be one: %d", misses)
	}

	_, _, _ = cache.Get(nil, grp, key+"1", "")
	calls := cache.Stats().GetCalls
	if calls != 1 {
		t.Errorf("Expected GetCalls to be one: %d", calls)
	}

	cache.Delete(grp, key)
	cache.Delete(grp, key+"1")
	size = cache.Stats().Size
	if size != 0 {
		t.Errorf("Expected Size to be zero: %d", size)
	}
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

func (c *WebCache) getShardSha1(key string) int {
	return int(sha1.Sum([]byte(key))[0]) % c.buckets
}

func (c *WebCache) getShardTest(key string) uint64 {
	hash := fnv.New64a()
	hash.Write([]byte(key))
	return hash.Sum64() % uint64(c.buckets)
}

func benchmarkRealSharded(b *testing.B, ratio int) {
	cache := NewWebCache(200000, 16)
	b.ResetTimer()

	b.SetParallelism(16)
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			keynum := rand.Intn(1000)
			key := strconv.FormatUint(uint64(keynum), 10)
			if keynum < 100*ratio {
				sz := rand.Intn(2000) + 1
				val := []byte(strings.Repeat("X", sz))
				cache.Set("", key, val)
			} else {
				cache.Get(nil, "", key, "")
			}
		}
	})
}

func benchmarkRealNotSharded(b *testing.B, ratio int) {
	cache := NewBucket(200000)
	b.ResetTimer()

	b.SetParallelism(16)
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			keynum := rand.Intn(1000)
			key := strconv.FormatUint(uint64(keynum), 10)
			if keynum < 100*ratio {
				sz := rand.Intn(2000) + 1
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
