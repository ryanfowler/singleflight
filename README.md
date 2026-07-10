# singleflight

[![Go Reference](https://pkg.go.dev/badge/github.com/ryanfowler/singleflight.svg)](https://pkg.go.dev/github.com/ryanfowler/singleflight)

A faster, generic, context-aware singleflight for Go.

`singleflight` is a small Go package for duplicate function call suppression.
It lets concurrent callers for the same key share one in-flight operation
instead of all doing the same work.

Use it around expensive, cache-fill, lookup, or refresh paths where many
goroutines may request the same thing at the same time:

- collapse duplicate database, API, or filesystem reads;
- prevent cache stampedes while a value is being rebuilt;
- keep request-scoped cancellation behavior for callers that stop waiting;
- use concrete generic key and value types without type assertions.

This package is not a cache. It only shares work while a call is currently
running. After that call finishes, the key is forgotten and a later call will
run the function again.

## Why This Package?

This module mirrors the core behavior of
[`golang.org/x/sync/singleflight`](https://pkg.go.dev/golang.org/x/sync/singleflight)
with a smaller, typed API:

- generic key and value types: `Group[K, V]`;
- one context-aware `Do` method;
- a lower-overhead synchronous path for callers that only need `Do`;
- no result wrapper or `any` casts;
- no runtime dependencies;
- a `ShardedGroup` option for high-concurrency, many-key workloads.

The first caller for a key runs the function in that caller's goroutine.
Duplicate callers wait for the shared result, or return early if their own
context is canceled before the original call completes.

For the common uncontended path, `Group` avoids allocating duplicate-waiter
state unless a second caller actually joins an in-flight call. The narrower API
also avoids the public `Result` wrapper and `any` result plumbing used by
`x/sync/singleflight`. The repository includes equivalent `x/sync/singleflight`
benchmarks for uncontended calls, sequential hot-key calls, hot-key parallel
calls, and many-key parallel calls, as well as a canceled-duplicate benchmark
for this package's context-aware waiting.

## Install

```sh
go get github.com/ryanfowler/singleflight
```

`singleflight` requires Go 1.24 or newer.

## Quick Start

```go
package users

import (
	"context"

	"github.com/ryanfowler/singleflight"
)

type User struct {
	ID   string
	Name string
}

var userLoads singleflight.Group[string, User]

func LoadUser(ctx context.Context, userID string) (User, error) {
	user, err, _ := userLoads.Do(ctx, userID, func(ctx context.Context) (User, error) {
		return loadUserFromDatabase(ctx, userID)
	})
	if err != nil {
		return User{}, err
	}
	return user, nil
}
```

If 100 goroutines call `LoadUser(ctx, "42")` while the first database lookup is
still running, `loadUserFromDatabase` runs once and the duplicate callers receive
the same `User` and `error`.

## API

```go
type Group[K comparable, V any] struct {
	// unexported fields
}

func (g *Group[K, V]) Do(
	ctx context.Context,
	key K,
	fn func(context.Context) (V, error),
) (v V, err error, shared bool)
```

The zero value of `Group` is ready to use:

```go
var group singleflight.Group[string, User]
```

`Group` is safe for concurrent use, but must not be copied after first use.

## Understanding `Do`

`Do` guarantees that only one `fn` is in flight for a given key at a time.
Different keys may run independently.

```go
value, err, shared := group.Do(ctx, key, func(ctx context.Context) (Value, error) {
	return load(ctx, key)
})
```

The `shared` return value is usually useful for metrics and logging:

- duplicate callers return `shared=true`;
- the caller that ran `fn` returns `shared=true` only if at least one duplicate
  caller was still waiting when `fn` completed;
- purely sequential calls usually return `shared=false`.

Most application code can ignore it:

```go
value, err, _ := group.Do(ctx, key, fn)
```

### Recursive Calls

`fn` must not synchronously call `Do` on the same group with the same key. The
nested call waits for the outer `fn` to return, while the outer `fn` waits for
the nested call, causing a deadlock. A cancelable context can release the
nested call, but code should avoid this pattern.

## Context Cancellation

Duplicate callers can cancel independently while waiting.

If a duplicate caller's context is canceled while it is waiting, that caller
returns the zero value, `ctx.Err()`, and `shared=true`. The original operation
continues for any remaining callers.

The first caller for a key runs `fn` synchronously, and its context is the
context passed to `fn`. Therefore, the first caller's context governs the
shared operation: if it is canceled and `fn` returns a cancellation error, all
callers still waiting receive that same error, even when their own contexts are
active. `Do` cannot forcibly stop `fn`; leader cancellation is cooperative, so
`fn` must observe `ctx` and return.

For cache fills or other work that should outlive an individual request, pass
`Do` a context with a lifetime appropriate for the shared operation (for
example, a service context with a bounded timeout), rather than a request
context.

```go
value, err, _ := group.Do(ctx, key, func(ctx context.Context) (Value, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return Value{}, err
	}
	return fetch(req)
})
```

## Sharded Groups

Use `Group` by default. It has the least per-call overhead and is the right
choice for most services, mostly sequential calls, low-concurrency callers, or
workloads dominated by one hot key.

For workloads with many goroutines calling one shared group with many distinct
keys at the same time, `ShardedGroup` can reduce lock contention by routing keys
across multiple internal `Group` values.

```go
var userLoads = singleflight.NewShardedGroup[string, User](64)

func LoadUser(ctx context.Context, userID string) (User, error) {
	user, err, _ := userLoads.Do(ctx, userID, func(ctx context.Context) (User, error) {
		return loadUserFromDatabase(ctx, userID)
	})
	return user, err
}
```

```go
type ShardedGroup[K comparable, V any] struct {
	// unexported fields
}

func NewShardedGroup[K comparable, V any](shards int) *ShardedGroup[K, V]

func (g *ShardedGroup[K, V]) Do(
	ctx context.Context,
	key K,
	fn func(context.Context) (V, error),
) (v V, err error, shared bool)
```

The zero value of `ShardedGroup` is ready to use with 32 shards. Use
`NewShardedGroup` when you want a specific shard count. The shard count must be
positive. A `ShardedGroup` must not be copied; retain and use the pointer
returned by `NewShardedGroup`.

`ShardedGroup` does not help a workload dominated by a single hot key. Equal
keys always route to the same shard so duplicate suppression still happens in
one internal `Group`, and sharding adds a small hash/routing cost to every call.

## Panics and `runtime.Goexit`

If `fn` panics or calls `runtime.Goexit`, the group cleans up its internal state
and propagates that behavior to every participating caller. This keeps future
calls for the same key from getting stuck behind a failed in-flight operation.

Panics are **not** replayed as their original values. `Do` recovers the panic,
captures its stack, and panics with a `*singleflight.PanicError` for both the
caller that ran `fn` and all waiting duplicate callers. Use `Value` to inspect
the original panic value and `Stack` to inspect the captured stack. If `fn`
calls `runtime.Goexit`, `Do` calls `runtime.Goexit` in every participating
caller.

## Comparison With `x/sync/singleflight`

Choose this package when you want typed results and context-aware waiting with a
minimal, faster `Do`-only API. It is designed to reduce overhead in the common
synchronous path by avoiding per-call waiter allocation when there are no
duplicates, avoiding result boxing, and exposing concrete typed values directly.

Choose `golang.org/x/sync/singleflight` when you need its broader API surface,
such as `DoChan`, `Forget`, or the public `Result` type.

### Benchmarks

The repository benchmarks equivalent `Do` workloads against
`golang.org/x/sync/singleflight` v0.16.0. Representative medians from three
runs on an Apple M2 Pro with Go 1.26.5 (`GOMAXPROCS=10`) are:

| Workload | Group | ShardedGroup (32) | x/sync Group |
| --- | ---: | ---: | ---: |
| Uncontended distinct keys | 57.7 ns/op, 0 allocs/op | 75.9 ns/op, 0 allocs/op | 90.4 ns/op, 2 allocs/op |
| Sequential hot key | 45.8 ns/op, 0 allocs/op | 52.3 ns/op, 0 allocs/op | 68.4 ns/op, 1 alloc/op |
| Parallel hot key | 291.6 ns/op, 0 allocs/op | 320.7 ns/op, 0 allocs/op | 379.9 ns/op, 0 allocs/op |
| Parallel distinct keys | 315.5 ns/op, 0 allocs/op | 114.7 ns/op, 1 alloc/op | 392.7 ns/op, 2 allocs/op |

These are microbenchmarks, not a substitute for measuring an application's
workload. `x/sync/singleflight` has no context-aware waiter API, so the
canceled-duplicate benchmark has no equivalent comparison. Reproduce the
comparison with:

```sh
GOMAXPROCS=10 go test -run='^$' \
  -bench='(DoUncontended|DoSameKeySequential|DoHotKeyParallel|DoManyKeysParallel)$' \
  -benchmem -count=3
```

Within this package, `Group` is usually fastest when there is little cross-key
lock contention. `ShardedGroup` can win when many goroutines concurrently use
many distinct keys, but it adds hashing overhead and is usually not faster for
mostly sequential workloads or a single hot key.

## License

Apache-2.0. See [LICENSE](LICENSE).
