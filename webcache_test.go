//Copyright 2020 Christopher Briscoe.  All rights reserved.
package webcache

import (
	"crypto/sha1"
	"hash/fnv"
	"math/rand"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
)

func createWebCache(t *testing.T, capacity uint64, shards int) Cacher {
	cache := NewWebCache(capacity, shards)
	if cache == nil {
		t.Errorf("NewWebCache() returned null")
	}
	return cache
}

func TestWebCount(t *testing.T) {
	cache := NewWebCache(10000, 0)
	if cache.shards != defaultShards {
		t.Errorf("Expected defaultShards when setting shards to 0")
	}

	cache = NewWebCache(10000, 999)
	if cache.shards != defaultShards {
		t.Errorf("Expected defaultShards when setting shards to > 256")
	}

	cache = NewWebCache(10000, 1)
	if cache.shards != 1 {
		t.Errorf("Expected shards to be 1 when setting shards to 1")
	}

	cache = NewWebCache(10000, 2)
	if cache.shards != 2 {
		t.Errorf("Expected shards to be 2 when setting shards to 2")
	}
}

func TestShardedSet(t *testing.T) {
	cache := createWebCache(t, 10000, 8)
	key := "key"
	val := "value"

	cache.Set(key, []byte(val))
	getval, _ := cache.Get(key, "")

	if val != string(getval) {
		t.Errorf("Expected Set() value to be %s instead of %s", val, string(getval))
	}
}

func TestShardedGet(t *testing.T) {
	cache := createWebCache(t, 10000, 8)
	key := "key"
	val := "value"

	cache.Set(key, []byte(val))
	getval, _ := cache.Get("notkey", "")

	if getval != nil {
		t.Errorf("Expected Get() value to be nil instead of %s", string(getval))
	}
}

func TestShardedDelete(t *testing.T) {
	cache := createWebCache(t, 10000, 8)
	key := "key"
	val := "value"

	cache.Set(key, []byte(val))
	cache.Delete(key)
	getval, _ := cache.Get(key, "")

	if getval != nil {
		t.Errorf("Expected Delete() value to be deleted")
	}
}

var raceShardedCache = NewWebCache(10000, 8)

func TestShardedRace(t *testing.T) {
	var wg sync.WaitGroup

	fn := func() {
		key := "somekey"
		v := []byte(strings.Repeat("X", 900))
		for i := 0; i < 5000; i++ {
			raceShardedCache.Set(key, v)
			raceShardedCache.Size() //to detect race condition, nothing else
			raceShardedCache.Get(key, "")
			raceShardedCache.Delete(key)
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
	return int(sha1.Sum([]byte(key))[0]) % c.shards
}

func (c *WebCache) getShardTest(key string) uint64 {
	hash := fnv.New64a()
	hash.Write([]byte(key))
	return hash.Sum64() % uint64(c.shards)
}

func BenchmarkHashMd5(b *testing.B) {
	cache := NewWebCache(1000, 1)
	key := strings.Repeat("X", 50)
	b.ResetTimer()

	b.SetParallelism(8)
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			cache.getShard(key)
		}
	})
}

func BenchmarkHashSha1(b *testing.B) {
	cache := NewWebCache(1000, 1)
	key := strings.Repeat("X", 50)
	b.ResetTimer()

	b.SetParallelism(8)
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			cache.getShardSha1(key)
		}
	})
}

func BenchmarkHashTest(b *testing.B) {
	cache := NewWebCache(1000, 1)
	key := strings.Repeat("X", 50)
	b.ResetTimer()

	b.SetParallelism(8)
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			cache.getShardTest(key)
		}
	})
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
				cache.Set(key, val)
			} else {
				cache.Get(key, "")
			}
		}
	})
}

func benchmarkRealNotSharded(b *testing.B, ratio int) {
	cache := NewShard(200000)
	b.ResetTimer()

	b.SetParallelism(16)
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			keynum := rand.Intn(1000)
			key := strconv.FormatUint(uint64(keynum), 10)
			if keynum < 100*ratio {
				sz := rand.Intn(2000) + 1
				val := []byte(strings.Repeat("X", sz))
				cache.Set(key, val)
			} else {
				cache.Get(key, "")
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