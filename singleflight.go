// Package singleflight provides duplicate function call suppression.
//
// Use Group for most workloads. Use ShardedGroup when a Group is shared by many
// goroutines that concurrently call Do with many distinct keys and profiling
// shows contention on Group's internal bookkeeping lock.
package singleflight

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"hash/maphash"
	"runtime"
	"runtime/debug"
	"sync"
)

// Group represents a class of work and forms a namespace in which units of work
// can be executed with duplicate suppression.
//
// Group is the right default for most callers. It has the least per-call
// overhead and suppresses duplicate work for each key with a single internal
// map and lock.
//
// The zero value of Group is ready to use.
// A Group must not be copied after first use.
type Group[K comparable, V any] struct {
	mu sync.Mutex
	m  map[K]*call[V]
}

type call[V any] struct {
	val     V
	err     error
	waiters int
	done    chan struct{}
}

// Do executes and returns the results of the given function, making sure that
// only one execution is in-flight for a given key at a time.
//
// If a duplicate call comes in, the duplicate caller waits for the original to
// complete and receives the same result. A duplicate caller may return early if
// its context is canceled before the original call completes.
//
// fn must not synchronously call Do on the same Group with the same key. Such
// a recursive call waits for fn to return, while fn waits for the recursive
// call, causing a deadlock. A cancelable context can release the recursive
// call, but callers should avoid this pattern.
//
// The first caller for a key runs fn synchronously in the caller's goroutine,
// and its context is passed to fn. Thus, that context governs the shared
// operation: if fn returns its cancellation error, all callers still waiting
// receive that result. Its cancellation is cooperative: fn must observe ctx
// and return for Do to return due to cancellation.
//
// The returned shared value reports whether this call joined or served shared
// work. Duplicate callers return shared=true. The caller running fn returns
// shared=true only when at least one duplicate caller is still waiting when fn
// completes.
func (g *Group[K, V]) Do(ctx context.Context, key K, fn func(context.Context) (V, error)) (v V, err error, shared bool) {
	g.mu.Lock()
	if g.m == nil {
		g.m = make(map[K]*call[V])
	}
	if c, ok := g.m[key]; ok {
		if err := ctx.Err(); err != nil {
			g.mu.Unlock()
			var zero V
			return zero, err, true
		}
		if c == nil {
			c = &call[V]{
				done:    make(chan struct{}),
				waiters: 1,
			}
			g.m[key] = c
		} else {
			c.waiters++
		}
		done := c.done
		g.mu.Unlock()
		return g.wait(ctx, c, done)
	}
	// A nil call marks an in-flight leader with no duplicate waiters yet.
	g.m[key] = nil
	g.mu.Unlock()

	return g.doCall(ctx, key, fn)
}

func (g *Group[K, V]) wait(ctx context.Context, c *call[V], done <-chan struct{}) (v V, err error, shared bool) {
	select {
	case <-done:
		replay(c.err)
		return c.val, c.err, true
	case <-ctx.Done():
		g.mu.Lock()
		select {
		case <-done:
			g.mu.Unlock()
			replay(c.err)
			return c.val, c.err, true
		default:
		}
		c.waiters--
		g.mu.Unlock()

		var zero V
		return zero, ctx.Err(), true
	}
}

func (g *Group[K, V]) doCall(ctx context.Context, key K, fn func(context.Context) (V, error)) (v V, err error, shared bool) {
	normalReturn := false

	defer func() {
		if !normalReturn {
			if r := recover(); r != nil {
				err = newPanicError(r)
			} else {
				err = errGoexit
			}
		}

		shared = g.finish(key, v, err)

		replay(err)
	}()

	v, err = fn(ctx)
	normalReturn = true

	return v, err, false
}

func (g *Group[K, V]) finish(key K, v V, err error) bool {
	g.mu.Lock()
	var c *call[V]
	if g.m != nil {
		c = g.m[key]
		delete(g.m, key)
	}

	shared := false
	if c != nil {
		c.val = v
		c.err = err
		shared = c.waiters > 0
		close(c.done)
	}
	g.mu.Unlock()
	return shared
}

// ShardedGroup represents a class of work split across multiple internal
// Groups. Keys are hashed with hash/maphash and routed to one shard before
// duplicate suppression is applied.
//
// ShardedGroup is useful when many goroutines concurrently call Do with many
// distinct keys. In that workload, sharding spreads the internal map and lock
// traffic across multiple Groups and can reduce contention. It does not improve
// duplicate suppression for a single hot key, because equal keys always route
// to the same shard, and it adds hash/routing overhead to each call.
//
// The zero value of ShardedGroup is ready to use with 32 shards. Use
// NewShardedGroup to create a group with a specific shard count.
//
// A ShardedGroup must not be copied. NewShardedGroup returns a pointer, which
// should be retained and used by all callers.
type ShardedGroup[K comparable, V any] struct {
	state *shardedGroupState[K, V]

	// zeroState provides storage for the zero value without requiring a heap
	// allocation until the ShardedGroup itself escapes. Constructed groups use
	// state so that the seed and shards remain shared if the value is
	// accidentally copied before first use.
	zeroState shardedGroupState[K, V]
}

type shardedGroupState[K comparable, V any] struct {
	initOnce sync.Once
	seed     maphash.Seed
	shards   []Group[K, V]
}

// NewShardedGroup returns a ShardedGroup with shards internal Groups.
//
// Pick a shard count high enough to spread expected distinct-key concurrency,
// but not so high that mostly idle shards waste memory. The zero value uses 32
// shards, which is a reasonable default for highly concurrent servers. It
// panics if shards is not positive.
func NewShardedGroup[K comparable, V any](shards int) *ShardedGroup[K, V] {
	if shards <= 0 {
		panic("singleflight: shard count must be positive")
	}
	return &ShardedGroup[K, V]{
		state: &shardedGroupState[K, V]{
			shards: make([]Group[K, V], shards),
		},
	}
}

// Do executes and returns the results of the given function, making sure that
// only one execution is in-flight for a given key at a time within the selected
// shard.
//
// ShardedGroup.Do has the same duplicate suppression and cancellation behavior
// as Group.Do.
func (g *ShardedGroup[K, V]) Do(ctx context.Context, key K, fn func(context.Context) (V, error)) (v V, err error, shared bool) {
	return g.groupFor(key).Do(ctx, key, fn)
}

func (g *ShardedGroup[K, V]) groupFor(key K) *Group[K, V] {
	state := g.state
	if state == nil {
		state = &g.zeroState
	}
	state.initOnce.Do(func() {
		state.seed = maphash.MakeSeed()
		if state.shards == nil {
			state.shards = make([]Group[K, V], defaultShardCount)
		}
	})
	return &state.shards[maphash.Comparable(state.seed, key)%uint64(len(state.shards))]
}

func replay(err error) {
	if p, ok := err.(*panicError); ok {
		panic(p)
	}
	if err == errGoexit {
		runtime.Goexit()
	}
}

var errGoexit = errors.New("runtime.Goexit was called")

const defaultShardCount = 32

type panicError struct {
	value any
	stack []byte
}

func newPanicError(v any) *panicError {
	stack := debug.Stack()
	if line := bytes.IndexByte(stack, '\n'); line >= 0 {
		stack = stack[line+1:]
	}
	return &panicError{value: v, stack: stack}
}

func (p *panicError) Error() string {
	return fmt.Sprintf("%v\n\n%s", p.value, p.stack)
}

func (p *panicError) Unwrap() error {
	err, ok := p.value.(error)
	if !ok {
		return nil
	}
	return err
}
