package singleflight

import (
	"context"
	"sync/atomic"
	"testing"
)

func BenchmarkDoUncontended(b *testing.B) {
	var g Group[int, int]
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		v, err, shared := g.Do(ctx, i, func(context.Context) (int, error) {
			return i, nil
		})
		if err != nil || shared || v != i {
			b.Fatalf("Do() = %d, %v, %v; want %d, nil, false", v, err, shared, i)
		}
	}
}

func BenchmarkDoHotKeyParallel(b *testing.B) {
	var g Group[string, int]
	ctx := context.Background()

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
}

func BenchmarkDoManyKeysParallel(b *testing.B) {
	var g Group[int, int]
	ctx := context.Background()
	var next atomic.Int64

	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			key := int(next.Add(1))
			v, err, shared := g.Do(ctx, key, func(context.Context) (int, error) {
				return key, nil
			})
			if err != nil || shared || v != key {
				b.Fatalf("Do() = %d, %v, %v; want %d, nil, false", v, err, shared, key)
			}
		}
	})
}

func BenchmarkCanceledDuplicate(b *testing.B) {
	var g Group[string, int]
	ctx := context.Background()
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
}

func BenchmarkForget(b *testing.B) {
	var g Group[int, int]
	ctx := context.Background()

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, _, _ = g.Do(ctx, i, func(context.Context) (int, error) {
			return i, nil
		})
		g.Forget(i)
	}
}

func BenchmarkForgetParallel(b *testing.B) {
	var g Group[int, int]
	ctx := context.Background()
	var next atomic.Int64

	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			key := int(next.Add(1))
			_, _, _ = g.Do(ctx, key, func(context.Context) (int, error) {
				return key, nil
			})
			g.Forget(key)
		}
	})
}
