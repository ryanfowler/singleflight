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

func replay(err error) {
	if p, ok := err.(*panicError); ok {
		panic(p)
	}
	if err == errGoexit {
		runtime.Goexit()
	}
}
