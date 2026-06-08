# singleflight

Generic, context-aware duplicate call suppression for Go.

This module mirrors the core behavior of `golang.org/x/sync/singleflight`
while using generic key and value types and a single context-aware `Do` method.
The first caller for a key runs the function in the caller's goroutine; duplicate
callers wait for the shared result or return early when their own context is
canceled.

```go
var group singleflight.Group[string, User]

user, err, shared := group.Do(ctx, userID, func(ctx context.Context) (User, error) {
	return loadUser(ctx, userID)
})
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

func (g *Group[K, V]) Forget(key K)
```

The zero value of `Group` is ready to use. There is no constructor, no
`DoChan`, and no public result wrapper type.

Leader cancellation is cooperative: because `fn` runs synchronously in the
leader caller's goroutine, `fn` must observe `ctx` and return for the leader's
`Do` call to return due to cancellation.
