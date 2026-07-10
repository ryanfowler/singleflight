package singleflight

import (
	"strconv"
	"sync/atomic"
	"testing"

	xsingleflight "golang.org/x/sync/singleflight"
)

// These benchmarks mirror the non-context-sensitive workloads in
// singleflight_bench_test.go. x/sync/singleflight has no context-aware waiter
// API, so BenchmarkCanceledDuplicate intentionally has no x/sync counterpart.
func BenchmarkXSyncDoUncontended(b *testing.B) {
	var g xsingleflight.Group

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := strconv.Itoa(i)
		v, err, shared := g.Do(key, func() (any, error) {
			return i, nil
		})
		if err != nil || shared || v.(int) != i {
			b.Fatalf("Do() = %v, %v, %v; want %d, nil, false", v, err, shared, i)
		}
	}
}

func BenchmarkXSyncDoSameKeySequential(b *testing.B) {
	var g xsingleflight.Group

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		v, err, shared := g.Do("key", func() (any, error) {
			return 1, nil
		})
		if err != nil || shared || v.(int) != 1 {
			b.Fatalf("Do() = %v, %v, %v; want 1, nil, false", v, err, shared)
		}
	}
}

func BenchmarkXSyncDoHotKeyParallel(b *testing.B) {
	var g xsingleflight.Group

	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			v, err, _ := g.Do("key", func() (any, error) {
				return 1, nil
			})
			if err != nil || v.(int) != 1 {
				b.Fatalf("Do() = %v, %v; want 1, nil", v, err)
			}
		}
	})
}

func BenchmarkXSyncDoManyKeysParallel(b *testing.B) {
	var g xsingleflight.Group
	var next atomic.Int64

	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			i := int(next.Add(1))
			key := strconv.Itoa(i)
			v, err, shared := g.Do(key, func() (any, error) {
				return i, nil
			})
			if err != nil || shared || v.(int) != i {
				b.Fatalf("Do() = %v, %v, %v; want %d, nil, false", v, err, shared, i)
			}
		}
	})
}
