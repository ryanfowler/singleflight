package singleflight

import (
	"context"
	"strconv"
	"sync/atomic"
	"testing"

	xsingleflight "golang.org/x/sync/singleflight"
)

// Each worker owns a key. Only worker setup uses the shared counter, so this
// measures cross-key contention without per-call key allocation or atomics.
func BenchmarkDoWorkerLocalKeysParallel(b *testing.B) {
	ctx := context.Background()
	b.Run("impl=Group", func(b *testing.B) {
		var g Group[string, int]
		var next atomic.Int64
		b.ReportAllocs()
		b.RunParallel(func(pb *testing.PB) {
			key := strconv.FormatInt(next.Add(1), 10)
			for pb.Next() {
				v, err, shared := g.Do(ctx, key, func(context.Context) (int, error) { return 1, nil })
				if v != 1 || err != nil || shared {
					b.Errorf("Do() = %d, %v, %v", v, err, shared)
					return
				}
			}
		})
	})
	b.Run("impl=ShardedGroup32", func(b *testing.B) {
		g := NewShardedGroup[string, int](defaultShardCount)
		var next atomic.Int64
		b.ReportAllocs()
		b.RunParallel(func(pb *testing.PB) {
			key := strconv.FormatInt(next.Add(1), 10)
			for pb.Next() {
				v, err, shared := g.Do(ctx, key, func(context.Context) (int, error) { return 1, nil })
				if v != 1 || err != nil || shared {
					b.Errorf("Do() = %d, %v, %v", v, err, shared)
					return
				}
			}
		})
	})
}

func BenchmarkXSyncDoWorkerLocalKeysParallel(b *testing.B) {
	var g xsingleflight.Group
	var next atomic.Int64
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		key := strconv.FormatInt(next.Add(1), 10)
		for pb.Next() {
			v, err, shared := g.Do(key, func() (any, error) { return 1, nil })
			if v != 1 || err != nil || shared {
				b.Errorf("Do() = %v, %v, %v", v, err, shared)
				return
			}
		}
	})
}

func BenchmarkDoLargeValue(b *testing.B) {
	b.Run("size=8B", benchmarkValueSize[uint64])
	b.Run("size=4KiB", benchmarkValueSize[[4096]byte])
	b.Run("size=64KiB", benchmarkValueSize[[65536]byte])
}

func benchmarkValueSize[V any](b *testing.B) {
	ctx := context.Background()
	fn := func(context.Context) (V, error) { var v V; return v, nil }
	b.Run("impl=Group", func(b *testing.B) {
		var g Group[string, V]
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_, err, shared := g.Do(ctx, "key", fn)
			if err != nil || shared {
				b.Fatalf("Do() err, shared = %v, %v", err, shared)
			}
		}
	})
	b.Run("impl=ShardedGroup32", func(b *testing.B) {
		g := NewShardedGroup[string, V](defaultShardCount)
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_, err, shared := g.Do(ctx, "key", fn)
			if err != nil || shared {
				b.Fatalf("Do() err, shared = %v, %v", err, shared)
			}
		}
	})
}

// One benchmark operation is a batch of callers sharing a blocked leader.
// This includes goroutine creation and result delivery, not just Do overhead.
func BenchmarkDoFanOut(b *testing.B) {
	ctx := context.Background()
	for _, callers := range []int{8, 64} {
		b.Run("callers="+strconv.Itoa(callers), func(b *testing.B) {
			for name, g := range testGroups[string, int]() {
				b.Run("impl="+name, func(b *testing.B) {
					b.ReportAllocs()
					b.ReportMetric(float64(callers), "callers/op")
					for i := 0; i < b.N; i++ {
						started := make(chan struct{})
						release := make(chan struct{})
						results := make(chan doResult[int], callers)
						go func() {
							v, err, shared := g.Do(ctx, "key", func(context.Context) (int, error) {
								close(started)
								<-release
								return 42, nil
							})
							results <- doResult[int]{v, err, shared}
						}()
						<-started
						for j := 1; j < callers; j++ {
							go func() {
								v, err, shared := g.Do(ctx, "key", func(context.Context) (int, error) { return 99, nil })
								results <- doResult[int]{v, err, shared}
							}()
						}
						waitForWaiters(b, underlyingTestGroup(g, "key"), "key", callers-1)
						close(release)
						for j := 0; j < callers; j++ {
							if got := <-results; got.val != 42 || got.err != nil || !got.shared {
								b.Fatalf("shared result = %#v", got)
							}
						}
					}
				})
			}
		})
	}
}

// Unlike the pre-canceled benchmark, this includes context and goroutine
// creation, waiter registration, cancellation, and result delivery.
func BenchmarkWaitingDuplicateCancellation(b *testing.B) {
	ctx := context.Background()
	for name, g := range testGroups[string, int]() {
		b.Run("impl="+name, func(b *testing.B) {
			started := make(chan struct{})
			release := make(chan struct{})
			leaderDone := make(chan struct{})
			go func() {
				defer close(leaderDone)
				g.Do(ctx, "key", func(context.Context) (int, error) {
					close(started)
					<-release
					return 42, nil
				})
			}()
			<-started
			defer func() { close(release); <-leaderDone }()
			shard := underlyingTestGroup(g, "key")
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				waiting, cancel := context.WithCancel(ctx)
				result := make(chan doResult[int], 1)
				go func() {
					v, err, shared := g.Do(waiting, "key", func(context.Context) (int, error) { return 99, nil })
					result <- doResult[int]{v, err, shared}
				}()
				waitForWaiters(b, shard, "key", 1)
				cancel()
				if got := <-result; got.val != 0 || got.err != context.Canceled || !got.shared {
					b.Fatalf("canceled result = %#v", got)
				}
			}
			b.StopTimer()
		})
	}
}
