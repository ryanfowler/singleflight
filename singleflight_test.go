package singleflight

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

type doResult[V any] struct {
	val    V
	err    error
	shared bool
}

func TestZeroValueGroup(t *testing.T) {
	var g Group[string, int]
	var calls int

	v, err, shared := g.Do(context.Background(), "key", func(context.Context) (int, error) {
		calls++
		return 42, nil
	})
	if err != nil {
		t.Fatalf("Do returned error: %v", err)
	}
	if v != 42 {
		t.Fatalf("Do returned value %d, want 42", v)
	}
	if shared {
		t.Fatal("first call returned shared=true, want false")
	}
	if calls != 1 {
		t.Fatalf("fn called %d times, want 1", calls)
	}

	v, err, shared = g.Do(context.Background(), "key", func(context.Context) (int, error) {
		calls++
		return 43, nil
	})
	if err != nil {
		t.Fatalf("second Do returned error: %v", err)
	}
	if v != 43 {
		t.Fatalf("second Do returned value %d, want 43", v)
	}
	if shared {
		t.Fatal("second sequential call returned shared=true, want false")
	}
	if calls != 2 {
		t.Fatalf("fn called %d times, want 2", calls)
	}
}

func TestGenericKeyAndValue(t *testing.T) {
	type key struct {
		ID   int
		Name string
	}
	type value struct {
		Message string
	}

	var g Group[key, value]
	v, err, shared := g.Do(context.Background(), key{ID: 1, Name: "a"}, func(context.Context) (value, error) {
		return value{Message: "typed"}, nil
	})
	if err != nil {
		t.Fatalf("Do returned error: %v", err)
	}
	if v.Message != "typed" {
		t.Fatalf("Do returned value %#v, want typed message", v)
	}
	if shared {
		t.Fatal("Do returned shared=true, want false")
	}
}

func TestDuplicateSuppression(t *testing.T) {
	var g Group[string, string]
	var calls atomic.Int32
	started := make(chan struct{})
	release := make(chan struct{})
	leaderDone := make(chan doResult[string], 1)
	duplicateDone := make(chan doResult[string], 1)

	go func() {
		v, err, shared := g.Do(context.Background(), "key", func(context.Context) (string, error) {
			calls.Add(1)
			close(started)
			<-release
			return "first", nil
		})
		leaderDone <- doResult[string]{val: v, err: err, shared: shared}
	}()

	<-started

	go func() {
		v, err, shared := g.Do(context.Background(), "key", func(context.Context) (string, error) {
			calls.Add(1)
			return "second", nil
		})
		duplicateDone <- doResult[string]{val: v, err: err, shared: shared}
	}()

	waitForWaiters(t, &g, "key", 1)
	close(release)

	leader := receiveResult(t, leaderDone)
	duplicate := receiveResult(t, duplicateDone)

	if calls.Load() != 1 {
		t.Fatalf("fn called %d times, want 1", calls.Load())
	}
	if leader.err != nil {
		t.Fatalf("leader returned error: %v", leader.err)
	}
	if duplicate.err != nil {
		t.Fatalf("duplicate returned error: %v", duplicate.err)
	}
	if leader.val != "first" || duplicate.val != "first" {
		t.Fatalf("values = %q, %q; want first, first", leader.val, duplicate.val)
	}
	if !leader.shared {
		t.Fatal("leader returned shared=false, want true")
	}
	if !duplicate.shared {
		t.Fatal("duplicate returned shared=false, want true")
	}
}

func TestCallCreatedOnSecondCaller(t *testing.T) {
	var g Group[string, int]
	started := make(chan struct{})
	release := make(chan struct{})
	leaderDone := make(chan doResult[int], 1)
	duplicateDone := make(chan doResult[int], 1)

	go func() {
		v, err, shared := g.Do(context.Background(), "key", func(context.Context) (int, error) {
			close(started)
			<-release
			return 1, nil
		})
		leaderDone <- doResult[int]{val: v, err: err, shared: shared}
	}()
	<-started

	g.mu.Lock()
	c, ok := g.m["key"]
	if !ok {
		t.Fatal("in-flight call was not registered")
	}
	if c != nil {
		t.Fatal("call was created before a duplicate caller joined")
	}
	g.mu.Unlock()

	go func() {
		v, err, shared := g.Do(context.Background(), "key", func(context.Context) (int, error) {
			return 2, nil
		})
		duplicateDone <- doResult[int]{val: v, err: err, shared: shared}
	}()

	waitForWaiters(t, &g, "key", 1)
	g.mu.Lock()
	c = g.m["key"]
	if c == nil {
		t.Fatal("call was not created after a duplicate caller joined")
	}
	g.mu.Unlock()

	close(release)

	leader := receiveResult(t, leaderDone)
	duplicate := receiveResult(t, duplicateDone)
	if leader.val != 1 || leader.err != nil || !leader.shared {
		t.Fatalf("leader result = %#v, want 1 nil true", leader)
	}
	if duplicate.val != 1 || duplicate.err != nil || !duplicate.shared {
		t.Fatalf("duplicate result = %#v, want 1 nil true", duplicate)
	}
}

func TestDistinctKeysDoNotBlockEachOther(t *testing.T) {
	var g Group[string, string]
	started := make(chan struct{})
	release := make(chan struct{})
	done := make(chan doResult[string], 1)

	go func() {
		v, err, shared := g.Do(context.Background(), "a", func(context.Context) (string, error) {
			close(started)
			<-release
			return "a", nil
		})
		done <- doResult[string]{val: v, err: err, shared: shared}
	}()
	<-started

	v, err, shared := g.Do(context.Background(), "b", func(context.Context) (string, error) {
		return "b", nil
	})
	if err != nil {
		t.Fatalf("Do for distinct key returned error: %v", err)
	}
	if v != "b" {
		t.Fatalf("Do for distinct key returned %q, want b", v)
	}
	if shared {
		t.Fatal("Do for distinct key returned shared=true, want false")
	}

	close(release)
	if got := receiveResult(t, done); got.val != "a" || got.err != nil || got.shared {
		t.Fatalf("blocked call result = %#v, want a nil false", got)
	}
}

func TestDuplicateContextCancellation(t *testing.T) {
	var g Group[string, int]
	started := make(chan struct{})
	release := make(chan struct{})
	leaderDone := make(chan doResult[int], 1)
	duplicateDone := make(chan doResult[int], 1)

	go func() {
		v, err, shared := g.Do(context.Background(), "key", func(context.Context) (int, error) {
			close(started)
			<-release
			return 10, nil
		})
		leaderDone <- doResult[int]{val: v, err: err, shared: shared}
	}()
	<-started

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		v, err, shared := g.Do(ctx, "key", func(context.Context) (int, error) {
			return 20, nil
		})
		duplicateDone <- doResult[int]{val: v, err: err, shared: shared}
	}()

	waitForWaiters(t, &g, "key", 1)
	cancel()

	duplicate := receiveResult(t, duplicateDone)
	if !errors.Is(duplicate.err, context.Canceled) {
		t.Fatalf("duplicate error = %v, want context.Canceled", duplicate.err)
	}
	if duplicate.val != 0 {
		t.Fatalf("duplicate value = %d, want zero", duplicate.val)
	}
	if !duplicate.shared {
		t.Fatal("duplicate returned shared=false, want true")
	}

	waitForWaiters(t, &g, "key", 0)
	close(release)

	leader := receiveResult(t, leaderDone)
	if leader.err != nil {
		t.Fatalf("leader returned error: %v", leader.err)
	}
	if leader.val != 10 {
		t.Fatalf("leader value = %d, want 10", leader.val)
	}
	if leader.shared {
		t.Fatal("leader returned shared=true after duplicate canceled, want false")
	}
}

func TestCooperativeLeaderCancellation(t *testing.T) {
	var g Group[string, int]
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	var called bool
	v, err, shared := g.Do(ctx, "key", func(ctx context.Context) (int, error) {
		called = true
		<-ctx.Done()
		return 0, ctx.Err()
	})
	if !called {
		t.Fatal("fn was not called")
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want context.Canceled", err)
	}
	if v != 0 {
		t.Fatalf("value = %d, want zero", v)
	}
	if shared {
		t.Fatal("shared = true, want false")
	}
}

func TestPanicReplay(t *testing.T) {
	var g Group[string, int]
	started := make(chan struct{})
	release := make(chan struct{})
	leaderPanic := make(chan any, 1)
	duplicatePanic := make(chan any, 1)

	go func() {
		defer func() {
			leaderPanic <- recover()
		}()
		_, _, _ = g.Do(context.Background(), "key", func(context.Context) (int, error) {
			close(started)
			<-release
			panic("boom")
		})
	}()
	<-started

	go func() {
		defer func() {
			duplicatePanic <- recover()
		}()
		_, _, _ = g.Do(context.Background(), "key", func(context.Context) (int, error) {
			return 2, nil
		})
	}()

	waitForWaiters(t, &g, "key", 1)
	close(release)

	assertPanicError(t, receiveAny(t, leaderPanic), "boom")
	assertPanicError(t, receiveAny(t, duplicatePanic), "boom")
}

func TestGoexitReplayAndCleanup(t *testing.T) {
	var g Group[string, int]
	started := make(chan struct{})
	release := make(chan struct{})
	leaderDone := make(chan struct{})
	duplicateDone := make(chan struct{})

	go func() {
		defer close(leaderDone)
		_, _, _ = g.Do(context.Background(), "key", func(context.Context) (int, error) {
			close(started)
			<-release
			runtime.Goexit()
			return 0, nil
		})
		t.Error("leader Do returned after runtime.Goexit")
	}()
	<-started

	go func() {
		defer close(duplicateDone)
		_, _, _ = g.Do(context.Background(), "key", func(context.Context) (int, error) {
			return 2, nil
		})
		t.Error("duplicate Do returned after runtime.Goexit")
	}()

	waitForWaiters(t, &g, "key", 1)
	close(release)
	receiveClosed(t, leaderDone, "leader")
	receiveClosed(t, duplicateDone, "duplicate")

	v, err, shared := g.Do(context.Background(), "key", func(context.Context) (int, error) {
		return 3, nil
	})
	if err != nil {
		t.Fatalf("Do after Goexit returned error: %v", err)
	}
	if v != 3 {
		t.Fatalf("Do after Goexit returned %d, want 3", v)
	}
	if shared {
		t.Fatal("Do after Goexit returned shared=true, want false")
	}
}

func TestManyCallersOneKey(t *testing.T) {
	var g Group[string, int]
	var calls atomic.Int32
	const callers = 64

	started := make(chan struct{})
	release := make(chan struct{})
	results := make(chan doResult[int], callers)

	go func() {
		v, err, shared := g.Do(context.Background(), "key", func(context.Context) (int, error) {
			calls.Add(1)
			close(started)
			<-release
			return 1, nil
		})
		results <- doResult[int]{val: v, err: err, shared: shared}
	}()
	<-started

	for i := 1; i < callers; i++ {
		go func() {
			v, err, shared := g.Do(context.Background(), "key", func(context.Context) (int, error) {
				calls.Add(1)
				return 2, nil
			})
			results <- doResult[int]{val: v, err: err, shared: shared}
		}()
	}

	waitForWaiters(t, &g, "key", callers-1)
	close(release)

	for i := 0; i < callers; i++ {
		result := receiveResult(t, results)
		if result.err != nil {
			t.Fatalf("result %d returned error: %v", i, result.err)
		}
		if result.val != 1 {
			t.Fatalf("result %d value = %d, want 1", i, result.val)
		}
		if !result.shared {
			t.Fatalf("result %d shared=false, want true", i)
		}
	}
	if calls.Load() != 1 {
		t.Fatalf("fn called %d times, want 1", calls.Load())
	}
}

func TestManyIndependentKeys(t *testing.T) {
	var g Group[int, int]
	const keys = 128
	results := make(chan doResult[int], keys)
	var calls atomic.Int32

	for i := 0; i < keys; i++ {
		key := i
		go func() {
			v, err, shared := g.Do(context.Background(), key, func(context.Context) (int, error) {
				calls.Add(1)
				return key, nil
			})
			results <- doResult[int]{val: v, err: err, shared: shared}
		}()
	}

	seen := make(map[int]bool, keys)
	for i := 0; i < keys; i++ {
		result := receiveResult(t, results)
		if result.err != nil {
			t.Fatalf("result %d returned error: %v", i, result.err)
		}
		if result.shared {
			t.Fatalf("result %d shared=true, want false", i)
		}
		seen[result.val] = true
	}
	if len(seen) != keys {
		t.Fatalf("saw %d keys, want %d", len(seen), keys)
	}
	if calls.Load() != keys {
		t.Fatalf("fn called %d times, want %d", calls.Load(), keys)
	}
}

func waitForWaiters[K comparable, V any](t *testing.T, g *Group[K, V], key K, want int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		g.mu.Lock()
		got := 0
		if g.m != nil {
			if c := g.m[key]; c != nil {
				got = c.waiters
			}
		}
		g.mu.Unlock()
		if got == want {
			return
		}
		runtime.Gosched()
	}

	g.mu.Lock()
	got := 0
	if g.m != nil {
		if c := g.m[key]; c != nil {
			got = c.waiters
		}
	}
	g.mu.Unlock()
	t.Fatalf("waiters for %v = %d, want %d", key, got, want)
}

func receiveResult[V any](t *testing.T, ch <-chan doResult[V]) doResult[V] {
	t.Helper()
	select {
	case result := <-ch:
		return result
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for result")
		var zero doResult[V]
		return zero
	}
}

func receiveAny(t *testing.T, ch <-chan any) any {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for value")
		return nil
	}
}

func receiveClosed(t *testing.T, ch <-chan struct{}, name string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for %s goroutine", name)
	}
}

func assertPanicError(t *testing.T, recovered any, want string) {
	t.Helper()
	if recovered == nil {
		t.Fatal("expected panic, got nil")
	}
	if _, ok := recovered.(*panicError); !ok {
		t.Fatalf("panic type = %T, want *panicError", recovered)
	}
	if !strings.Contains(fmt.Sprint(recovered), want) {
		t.Fatalf("panic = %v, want substring %q", recovered, want)
	}
}
