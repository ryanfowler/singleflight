# singleflight

A faster, generic, context-aware singleflight for Go.

This module mirrors the core behavior of `golang.org/x/sync/singleflight`
while using generic key and value types, a single context-aware `Do` method,
and fewer allocations. The first caller for a key runs the function in the
caller's goroutine; duplicate callers wait for the shared result or return
early when their own context is canceled.

```go
var group singleflight.Group[string, User]

user, err, _ := group.Do(ctx, userID, func(ctx context.Context) (User, error) {
	return loadUser(ctx, userID)
})
if err != nil {
	return User{}, err
}
```

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

The zero value of `Group` is ready to use. There is no constructor for `Group`,
no `DoChan`, no `Forget`, and no public result wrapper type.

## Choosing `Group` or `ShardedGroup`

Use `Group` by default. It has the least per-call overhead and is usually the
fastest choice for low-concurrency callers, mostly sequential calls, or
workloads dominated by one hot key.

`ShardedGroup` uses `hash/maphash` to route each key to one of N internal
`Group` values, which can reduce lock contention when many independent keys are
active concurrently. The zero value is ready to use with 32 shards; use
`NewShardedGroup` to choose a different shard count.

Use `ShardedGroup` when a single shared group is called by many goroutines with
many distinct keys at the same time, especially if profiling shows contention
on `Group`'s internal mutex. In that workload, sharding spreads the internal
in-flight maps and locks across multiple `Group` values.

Do not expect `ShardedGroup` to help a workload dominated by one hot key. Equal
keys always hash to the same shard so duplicate suppression still happens in
one internal `Group`. Sharding also adds a small hash/routing cost to every
call, so it can be slower when there is little cross-key contention.

```go
var group = singleflight.NewShardedGroup[string, User](64)

user, err, _ := group.Do(ctx, userID, func(ctx context.Context) (User, error) {
	return loadUser(ctx, userID)
})
if err != nil {
	return User{}, err
}
```

Most callers can ignore the third return value. It mirrors
`golang.org/x/sync/singleflight`: `shared` reports whether the call joined or
served shared work, which is mainly useful for observability.

```go
user, err, shared := group.Do(ctx, userID, func(ctx context.Context) (User, error) {
	return loadUser(ctx, userID)
})
if shared {
	recordSharedUserLoad()
}
```

Leader cancellation is cooperative: because `fn` runs synchronously in the
leader caller's goroutine, `fn` must observe `ctx` and return for the leader's
`Do` call to return due to cancellation.
