package singleflight

import (
	"context"
	"errors"
	"math"
	"os"
	"os/exec"
	"sync"
	"testing"
	"time"
)

type testGroup[K comparable, V any] interface {
	Do(context.Context, K, func(context.Context) (V, error)) (V, error, bool)
}

func testGroups[K comparable, V any]() map[string]testGroup[K, V] {
	return map[string]testGroup[K, V]{
		"Group":            new(Group[K, V]),
		"ShardedGroup":     NewShardedGroup[K, V](8),
		"ZeroShardedGroup": new(ShardedGroup[K, V]),
	}
}

func TestInvalidKeysDoNotPoisonGroup(t *testing.T) {
	keys := map[string]any{
		"NaN":         math.NaN(),
		"StructNaN":   struct{ F float64 }{math.NaN()},
		"ArrayNaN":    [1]float64{math.NaN()},
		"ComplexNaN":  complex(math.NaN(), 0),
		"Slice":       []int{1},
		"StructSlice": struct{ Value any }{[]int{1}},
		"Map":         map[string]int{"a": 1},
		"Function":    func() {},
	}
	for keyName, key := range keys {
		for name, g := range testGroups[any, int]() {
			t.Run(name+"/"+keyName, func(t *testing.T) {
				var panicked any
				called := false
				func() {
					defer func() { panicked = recover() }()
					g.Do(context.Background(), key, func(context.Context) (int, error) {
						called = true
						return 1, nil
					})
				}()
				if panicked == nil || called {
					t.Fatalf("invalid key: panic=%v, fn called=%v", panicked, called)
				}
				results := make(chan doResult[int], 1)
				go func() {
					v, err, shared := g.Do(context.Background(), "valid", func(context.Context) (int, error) {
						return 42, nil
					})
					results <- doResult[int]{v, err, shared}
				}()
				if got := receiveResult(t, results); got.val != 42 || got.err != nil || got.shared {
					t.Fatalf("call after invalid key = %#v", got)
				}
			})
		}
	}
}

func TestNaNKeyDoesNotCreateMapEntry(t *testing.T) {
	var g Group[float64, int]
	for i := 0; i < 100; i++ {
		func() {
			defer func() {
				if recover() == nil {
					t.Fatal("NaN key did not panic")
				}
			}()
			g.Do(context.Background(), math.NaN(), func(context.Context) (int, error) {
				t.Fatal("fn ran for NaN key")
				return 0, nil
			})
		}()
	}
	if len(g.m) != 0 {
		t.Fatalf("NaN calls retained %d map entries", len(g.m))
	}
	v, err, shared := g.Do(context.Background(), 1, func(context.Context) (int, error) { return 42, nil })
	if v != 42 || err != nil || shared {
		t.Fatalf("valid call = %d, %v, %v", v, err, shared)
	}
}

func TestPreCanceledDuplicateDoesNotJoin(t *testing.T) {
	for name, g := range testGroups[string, int]() {
		t.Run(name, func(t *testing.T) {
			started := make(chan struct{})
			release := make(chan struct{})
			leaderDone := make(chan doResult[int], 1)
			go func() {
				v, err, shared := g.Do(context.Background(), "key", func(context.Context) (int, error) {
					close(started)
					<-release
					return 42, nil
				})
				leaderDone <- doResult[int]{v, err, shared}
			}()
			<-started
			defer func() {
				close(release)
				if got := receiveResult(t, leaderDone); got.val != 42 || got.err != nil || got.shared {
					t.Errorf("unshared leader = %#v", got)
				}
			}()
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			v, err, shared := g.Do(ctx, "key", func(context.Context) (int, error) {
				t.Error("pre-canceled duplicate ran fn")
				return 99, nil
			})
			if v != 0 || err != context.Canceled || !shared {
				t.Fatalf("pre-canceled duplicate = %d, %v, %v", v, err, shared)
			}
			shard := underlyingTestGroup(g, "key")
			shard.mu.Lock()
			c, ok := shard.m["key"]
			shard.mu.Unlock()
			if !ok || c != nil {
				t.Fatal("pre-canceled duplicate allocated waiter state")
			}
		})
	}
}

func TestLeaderCancellationIsShared(t *testing.T) {
	for name, g := range testGroups[string, int]() {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			started := make(chan struct{})
			results := make(chan doResult[int], 4)
			go func() {
				v, err, shared := g.Do(ctx, "key", func(ctx context.Context) (int, error) {
					close(started)
					<-ctx.Done()
					return 0, ctx.Err()
				})
				results <- doResult[int]{v, err, shared}
			}()
			<-started
			for i := 0; i < 3; i++ {
				go func() {
					v, err, shared := g.Do(context.Background(), "key", func(context.Context) (int, error) {
						return 99, nil
					})
					results <- doResult[int]{v, err, shared}
				}()
			}
			waitForWaiters(t, underlyingTestGroup(g, "key"), "key", 3)
			cancel()
			for i := 0; i < 4; i++ {
				got := receiveResult(t, results)
				if got.val != 0 || !errors.Is(got.err, context.Canceled) || !got.shared {
					t.Fatalf("shared cancellation = %#v", got)
				}
			}
		})
	}
}

// Tests use the internal waiter count to release leaders only after all
// duplicates have joined. Timing alone cannot establish this ordering.
func underlyingTestGroup[K comparable, V any](g testGroup[K, V], key K) *Group[K, V] {
	if g, ok := g.(*Group[K, V]); ok {
		return g
	}
	return g.(*ShardedGroup[K, V]).groupFor(key)
}

func TestCancellationRacesCompletion(t *testing.T) {
	for name, g := range testGroups[string, int]() {
		t.Run(name, func(t *testing.T) {
			for i := 0; i < 100; i++ {
				ctx, cancel := context.WithCancel(context.Background())
				started := make(chan struct{})
				release := make(chan struct{})
				leaderDone := make(chan doResult[int], 1)
				duplicateDone := make(chan doResult[int], 1)
				go func() {
					v, err, shared := g.Do(context.Background(), "key", func(context.Context) (int, error) {
						close(started)
						<-release
						return 42, nil
					})
					leaderDone <- doResult[int]{v, err, shared}
				}()
				<-started
				go func() {
					v, err, shared := g.Do(ctx, "key", func(context.Context) (int, error) { return 99, nil })
					duplicateDone <- doResult[int]{v, err, shared}
				}()
				waitForWaiters(t, underlyingTestGroup(g, "key"), "key", 1)
				var wg sync.WaitGroup
				wg.Add(2)
				go func() { defer wg.Done(); cancel() }()
				go func() { defer wg.Done(); close(release) }()
				wg.Wait()
				leader := receiveResult(t, leaderDone)
				duplicate := receiveResult(t, duplicateDone)
				if leader.val != 42 || leader.err != nil {
					t.Fatalf("leader = %#v", leader)
				}
				if !duplicate.shared {
					t.Fatal("duplicate returned shared=false")
				}
				if duplicate.err == nil {
					if duplicate.val != 42 || !leader.shared {
						t.Fatalf("completed results: leader=%#v duplicate=%#v", leader, duplicate)
					}
				} else if !errors.Is(duplicate.err, context.Canceled) || duplicate.val != 0 {
					t.Fatalf("canceled duplicate = %#v", duplicate)
				}
				if shard := underlyingTestGroup(g, "key"); len(shard.m) != 0 {
					t.Fatal("completed call retained its key")
				}
			}
		})
	}
}

func TestPanicCleanupAndErrorAccessors(t *testing.T) {
	for name, g := range testGroups[string, int]() {
		t.Run(name, func(t *testing.T) {
			want := errors.New("boom")
			var p *PanicError
			func() {
				defer func() { p, _ = recover().(*PanicError) }()
				g.Do(context.Background(), "key", func(context.Context) (int, error) { panic(want) })
			}()
			if p == nil || p.Value() != want || !errors.Is(p, want) {
				t.Fatalf("panic error = %v", p)
			}
			stack := p.Stack()
			if len(stack) == 0 {
				t.Fatal("empty panic stack")
			}
			original := stack[0]
			stack[0] ^= 0xff
			if p.Stack()[0] != original {
				t.Fatal("Stack exposed internal storage")
			}
			results := make(chan doResult[int], 1)
			go func() {
				v, err, shared := g.Do(context.Background(), "key", func(context.Context) (int, error) { return 42, nil })
				results <- doResult[int]{v, err, shared}
			}()
			if got := receiveResult(t, results); got.val != 42 || got.err != nil || got.shared {
				t.Fatalf("call after panic = %#v", got)
			}
		})
	}
	if got := newPanicError("not an error").Unwrap(); got != nil {
		t.Fatalf("Unwrap of string panic = %v", got)
	}
}

func TestCompletedCallWinsCancellation(t *testing.T) {
	var g Group[string, int]
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	wantErr := errors.New("shared error")
	c := &call[int]{val: 42, err: wantErr, waiters: 1, done: make(chan struct{})}
	close(c.done)
	for i := 0; i < 100; i++ {
		v, err, shared := g.wait(ctx, c, c.done)
		if v != 42 || err != wantErr || !shared || c.waiters != 1 {
			t.Fatalf("completed result = %d, %v, %v; waiters=%d", v, err, shared, c.waiters)
		}
	}
}

func TestLargeValueIsShared(t *testing.T) {
	type value [4096]byte
	var want value
	for i := range want {
		want[i] = byte(i)
	}
	wantErr := errors.New("shared error")
	for name, g := range testGroups[string, value]() {
		t.Run(name, func(t *testing.T) {
			started := make(chan struct{})
			release := make(chan struct{})
			results := make(chan doResult[value], 2)
			go func() {
				v, err, shared := g.Do(context.Background(), "key", func(context.Context) (value, error) {
					close(started)
					<-release
					return want, wantErr
				})
				results <- doResult[value]{v, err, shared}
			}()
			<-started
			go func() {
				v, err, shared := g.Do(context.Background(), "key", func(context.Context) (value, error) { return value{}, nil })
				results <- doResult[value]{v, err, shared}
			}()
			waitForWaiters(t, underlyingTestGroup(g, "key"), "key", 1)
			close(release)
			for i := 0; i < 2; i++ {
				got := receiveResult(t, results)
				if got.val != want || got.err != wantErr || !got.shared {
					t.Fatalf("large shared result: err=%v, shared=%v, equal=%v", got.err, got.shared, got.val == want)
				}
			}
			v, err, shared := g.Do(context.Background(), "key", func(context.Context) (value, error) { return want, nil })
			if v != want || err != nil || shared {
				t.Fatalf("large unshared result: err=%v, shared=%v, equal=%v", err, shared, v == want)
			}
		})
	}
}

func TestLegacyPanicNilReplay(t *testing.T) {
	if os.Getenv("SINGLEFLIGHT_TEST_PANICNIL") != "1" {
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestLegacyPanicNilReplay$", "-test.v")
		cmd.Env = append(os.Environ(), "SINGLEFLIGHT_TEST_PANICNIL=1", "GODEBUG="+os.Getenv("GODEBUG")+",panicnil=1")
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("legacy panic(nil) subprocess: %v\n%s", err, out)
		}
		return
	}
	for name, g := range testGroups[string, int]() {
		t.Run(name, func(t *testing.T) {
			started := make(chan struct{})
			release := make(chan struct{})
			panics := make(chan any, 2)
			go func() {
				defer func() { panics <- recover() }()
				g.Do(context.Background(), "key", func(context.Context) (int, error) {
					close(started)
					<-release
					panic(nil)
				})
			}()
			<-started
			go func() {
				defer func() { panics <- recover() }()
				g.Do(context.Background(), "key", func(context.Context) (int, error) { return 99, nil })
			}()
			waitForWaiters(t, underlyingTestGroup(g, "key"), "key", 1)
			close(release)
			var first *PanicError
			for i := 0; i < 2; i++ {
				p, ok := receiveAny(t, panics).(*PanicError)
				if !ok || p.Value() != nil || len(p.Stack()) == 0 {
					t.Fatalf("legacy panic(nil) was not replayed: %v", p)
				}
				if first != nil && first != p {
					t.Fatal("callers received different PanicError values")
				}
				first = p
			}
			if v, err, shared := g.Do(context.Background(), "key", func(context.Context) (int, error) { return 42, nil }); v != 42 || err != nil || shared {
				t.Fatalf("call after panic(nil) = %d, %v, %v", v, err, shared)
			}
		})
	}
}
