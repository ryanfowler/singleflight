// Package singleflight provides duplicate function call suppression.
package singleflight

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"runtime"
	"runtime/debug"
	"sync"
)

var errGoexit = errors.New("runtime.Goexit was called")

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

type call[V any] struct {
	done chan struct{}

	val V
	err error

	waiters int
}

// Group represents a class of work and forms a namespace in which units of work
// can be executed with duplicate suppression.
//
// The zero value of Group is ready to use.
// A Group must not be copied after first use.
type Group[K comparable, V any] struct {
	mu sync.Mutex
	m  map[K]*call[V]
}

// Do executes and returns the results of the given function, making sure that
// only one execution is in-flight for a given key at a time.
//
// If a duplicate call comes in, the duplicate caller waits for the original to
// complete and receives the same result. A duplicate caller may return early if
// its context is canceled before the original call completes.
//
// The first caller for a key runs fn synchronously in the caller's goroutine.
// Its context cancellation is therefore cooperative: fn must observe ctx and
// return for Do to return due to cancellation.
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
		c.waiters++
		if c.done == nil {
			c.done = make(chan struct{})
		}
		done := c.done
		g.mu.Unlock()
		return g.wait(ctx, c, done)
	}
	c := new(call[V])
	g.m[key] = c
	g.mu.Unlock()

	return g.doCall(ctx, key, c, fn)
}

// Forget tells the singleflight group to forget about a key. Future calls to Do
// for this key will call the function rather than waiting for an earlier call to
// complete.
func (g *Group[K, V]) Forget(key K) {
	g.mu.Lock()
	delete(g.m, key)
	g.mu.Unlock()
}

func (g *Group[K, V]) wait(ctx context.Context, c *call[V], done <-chan struct{}) (v V, err error, shared bool) {
	select {
	case <-done:
		c.replay()
		return c.val, c.err, true
	case <-ctx.Done():
		g.mu.Lock()
		select {
		case <-done:
			g.mu.Unlock()
			c.replay()
			return c.val, c.err, true
		default:
		}
		c.waiters--
		g.mu.Unlock()

		var zero V
		return zero, ctx.Err(), true
	}
}

func (g *Group[K, V]) doCall(ctx context.Context, key K, c *call[V], fn func(context.Context) (V, error)) (v V, err error, shared bool) {
	normalReturn := false
	recovered := false

	defer func() {
		if !normalReturn && !recovered {
			c.err = errGoexit
		}

		v, err = c.val, c.err
		shared = g.finish(key, c)

		c.replay()
	}()

	func() {
		defer func() {
			if !normalReturn {
				if r := recover(); r != nil {
					c.err = newPanicError(r)
				}
			}
		}()

		c.val, c.err = fn(ctx)
		normalReturn = true
	}()

	if !normalReturn {
		recovered = true
	}

	return c.val, c.err, false
}

func (g *Group[K, V]) finish(key K, c *call[V]) bool {
	g.mu.Lock()
	shared := c.waiters > 0
	if g.m != nil && g.m[key] == c {
		delete(g.m, key)
	}
	if c.done != nil {
		close(c.done)
	}
	g.mu.Unlock()
	return shared
}

func (c *call[V]) replay() {
	if p, ok := c.err.(*panicError); ok {
		panic(p)
	}
	if c.err == errGoexit {
		runtime.Goexit()
	}
}
