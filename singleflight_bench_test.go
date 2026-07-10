package singleflight

import (
	"context"
	"strconv"
	"sync/atomic"
	"testing"
)

func BenchmarkDoUncontended(b *testing.B) {
	ctx := context.Background()

	b.Run("impl=Group", func(b *testing.B) {
		var g Group[string, int]

		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			key := strconv.Itoa(i)
			v, err, shared := g.Do(ctx, key, func(context.Context) (int, error) {
				return i, nil
			})
			if err != nil || shared || v != i {
				b.Fatalf("Do() = %d, %v, %v; want %d, nil, false", v, err, shared, i)
			}
		}
	})

	b.Run("impl=ShardedGroup32", func(b *testing.B) {
		g := NewShardedGroup[string, int](defaultShardCount)

		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			key := strconv.Itoa(i)
			v, err, shared := g.Do(ctx, key, func(context.Context) (int, error) {
				return i, nil
			})
			if err != nil || shared || v != i {
				b.Fatalf("Do() = %d, %v, %v; want %d, nil, false", v, err, shared, i)
			}
		}
	})
}

func BenchmarkDoSameKeySequential(b *testing.B) {
	ctx := context.Background()

	b.Run("impl=Group", func(b *testing.B) {
		var g Group[string, int]

		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			v, err, shared := g.Do(ctx, "key", func(context.Context) (int, error) {
				return 1, nil
			})
			if err != nil || shared || v != 1 {
				b.Fatalf("Do() = %d, %v, %v; want 1, nil, false", v, err, shared)
			}
		}
	})

	b.Run("impl=ShardedGroup32", func(b *testing.B) {
		g := NewShardedGroup[string, int](defaultShardCount)

		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			v, err, shared := g.Do(ctx, "key", func(context.Context) (int, error) {
				return 1, nil
			})
			if err != nil || shared || v != 1 {
				b.Fatalf("Do() = %d, %v, %v; want 1, nil, false", v, err, shared)
			}
		}
	})
}

func BenchmarkDoHotKeyParallel(b *testing.B) {
	ctx := context.Background()

	b.Run("impl=Group", func(b *testing.B) {
		var g Group[string, int]

		b.ReportAllocs()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				v, err, shared := g.Do(ctx, "key", func(context.Context) (int, error) {
					return 1, nil
				})
				if err != nil || v != 1 {
					b.Fatalf("Do() = %d, %v, %v; want 1, nil, any", v, err, shared)
				}
			}
		})
	})

	b.Run("impl=ShardedGroup32", func(b *testing.B) {
		g := NewShardedGroup[string, int](defaultShardCount)

		b.ReportAllocs()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				v, err, shared := g.Do(ctx, "key", func(context.Context) (int, error) {
					return 1, nil
				})
				if err != nil || v != 1 {
					b.Fatalf("Do() = %d, %v, %v; want 1, nil, any", v, err, shared)
				}
			}
		})
	})
}

func BenchmarkDoManyKeysParallel(b *testing.B) {
	ctx := context.Background()

	b.Run("impl=Group", func(b *testing.B) {
		var g Group[string, int]
		var next atomic.Int64

		b.ReportAllocs()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				i := int(next.Add(1))
				key := strconv.Itoa(i)
				v, err, shared := g.Do(ctx, key, func(context.Context) (int, error) {
					return i, nil
				})
				if err != nil || shared || v != i {
					b.Fatalf("Do() = %d, %v, %v; want %d, nil, false", v, err, shared, i)
				}
			}
		})
	})

	b.Run("impl=ShardedGroup32", func(b *testing.B) {
		g := NewShardedGroup[string, int](defaultShardCount)
		var next atomic.Int64

		b.ReportAllocs()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				i := int(next.Add(1))
				key := strconv.Itoa(i)
				v, err, shared := g.Do(ctx, key, func(context.Context) (int, error) {
					return i, nil
				})
				if err != nil || shared || v != i {
					b.Fatalf("Do() = %d, %v, %v; want %d, nil, false", v, err, shared, i)
				}
			}
		})
	})
}

func BenchmarkCanceledDuplicate(b *testing.B) {
	ctx := context.Background()

	b.Run("impl=Group", func(b *testing.B) {
		var g Group[string, int]
		release := make(chan struct{})
		leaderStarted := make(chan struct{})
		leaderDone := make(chan struct{})

		go func() {
			defer close(leaderDone)
			_, _, _ = g.Do(ctx, "key", func(context.Context) (int, error) {
				close(leaderStarted)
				<-release
				return 1, nil
			})
		}()
		<-leaderStarted

		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			canceled, cancel := context.WithCancel(ctx)
			cancel()
			_, err, shared := g.Do(canceled, "key", func(context.Context) (int, error) {
				return 2, nil
			})
			if err == nil || !shared {
				b.Fatalf("Do() err, shared = %v, %v; want canceled error, true", err, shared)
			}
		}
		b.StopTimer()
		close(release)
		<-leaderDone
	})

	b.Run("impl=ShardedGroup32", func(b *testing.B) {
		g := NewShardedGroup[string, int](defaultShardCount)
		release := make(chan struct{})
		leaderStarted := make(chan struct{})
		leaderDone := make(chan struct{})

		go func() {
			defer close(leaderDone)
			_, _, _ = g.Do(ctx, "key", func(context.Context) (int, error) {
				close(leaderStarted)
				<-release
				return 1, nil
			})
		}()
		<-leaderStarted

		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			canceled, cancel := context.WithCancel(ctx)
			cancel()
			_, err, shared := g.Do(canceled, "key", func(context.Context) (int, error) {
				return 2, nil
			})
			if err == nil || !shared {
				b.Fatalf("Do() err, shared = %v, %v; want canceled error, true", err, shared)
			}
		}
		b.StopTimer()
		close(release)
		<-leaderDone
	})
}
