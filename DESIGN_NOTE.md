# Issue #1263: server-initiated listen cancellation

Provisional local prototype for issue discussion, based on the supplied
2026-07-28 requirement to send `notifications/cancelled` when the server ends a listen.
This is not a stable API commitment or a whole-protocol compliance claim.
Previously, `ServerSession.Close` only cancelled listens locally by ID.

## Proposed contract

- Add one exported field: `RequestExtra.CancelListen func(context.Context, string) error`.
- Populate it only for `subscriptions/listen`, before receiving middleware runs.
  Access it through `Server.AddReceivingMiddleware` and `req.GetExtra()`.
  Forward the same extra to `SubscribeHandler`'s `req.Extra` for resources
  subscribed through that listen; legacy resource subscribe requests have no callback.
- The closure binds one request generation, its original listen ID, and its
  request-scoped protocol/transport routing. No exported request ID is needed.
  Even `context.Background()` as the caller context preserves the original routing.
  A stale closure cannot cancel a later request that reuses the same ID.
- Calling it attempts `notifications/cancelled` with that original ID and the
  supplied reason, then cancels the local handler context even if sending fails.
  Keep the existing final listen response for explicit per-listen cancellation:
  it completes outstanding calls without closing the session.
- `sync.Once` permits at most one send attempt across concurrent explicit calls
  and shutdown. Cache the first attempt's result, including any error, for repeat
  calls; a failed send is not retried. The first attempt chooses the context and reason.
- After client cancellation, disconnect, or handler completion, an unused
  callback is a no-op returning nil. If an earlier explicit attempt occurred,
  return its cached result instead. Passive teardown must not initiate a send.
- The caller context bounds the write, with an internal five-second ceiling.
  The callback waits only for the send attempt and local context cancellation,
  not handler completion; context cancellation still happens on timeout/error.
- `ServerSession.Close` invokes this same callback path for active listens with
  reason `server session closed`, before closing the connection. Automatic sends
  run concurrently, are best effort, and log send failures; `Close` drains handlers.
  Existing connection shutdown may suppress the final response: require the
  cancellation notification before EOF, not a final result during session close.
  New listens are rejected once shutdown starts, so none can escape the snapshot.
  Call `Close` from an owner goroutine, never synchronously inside a handler
  whose completion it must await.

## Options considered

- **Generic `ServerSession.NotifyCancelled` + RequestID accessor:** two exports;
  flexible, but correct IDs/generations, routing, and deduplication burden callers.
- **Context-based cancel method:** implicit request scope; retaining and supplying
  the right context becomes part of correctness and is less discoverable.
- **Chosen callback:** one exported field, bound lifetime and routing; access
  requires receiving middleware or a resource subscription handler.
- **Automatic close only:** zero exports, but cannot stop one listen without
  closing the entire session.

## Example: retain one listen's callback

Proposed API usage; register before serving. This example captures the first
listen only. The owner closes `stop` later to stop that listen, not the session.

```go
package example

import (
	"context"
	"log"
	"sync"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

func cancelFirstListenLater(server *mcp.Server, stop <-chan struct{}) {
	ready := make(chan func(context.Context, string) error, 1)
	var capture sync.Once
	server.AddReceivingMiddleware(func(next mcp.MethodHandler) mcp.MethodHandler {
		return func(ctx context.Context, method string, req mcp.Request) (mcp.Result, error) {
			if extra := req.GetExtra(); method == "subscriptions/listen" && extra != nil && extra.CancelListen != nil {
				capture.Do(func() { ready <- extra.CancelListen })
			}
			return next(ctx, method, req)
		}
	})
	go func() {
		cancelListen := <-ready
		<-stop
		if err := cancelListen(context.Background(), "owner stopped listen"); err != nil {
			log.Printf("cancel listen: %v", err)
		}
	}()
}
```

## Boundaries and verification

- No blanket notification on `ctx.Done()`: client cancellation/disconnect must
  not echo cancellation. Broken transports provide no automatic wire-delivery guarantee.
- Wider spec follow-up: confirm retained final-response semantics and directional
  cancellation. The current client's cancellation path targets incoming handlers,
  not its outgoing listen call; notification alone must not replace completion.
  Overlapping incoming/outgoing IDs need client-side follow-up before adoption.
- Is middleware/resource-handler access sufficient for owners of individual listens?
- Tests in `mcp/listen_cancel_test.go` cover numeric/string IDs over in-memory and
  HTTP/SSE, explicit/automatic teardown, duplicates, retained callbacks after close,
  client disconnect, concurrent teardown, ID reuse, failed/timed-out sends, and
  rejection of a late listen during shutdown. No separate stdio process test.
- Passed: `go test ./mcp -run '^TestCancelListen' -count=1 -timeout=90s`;
  `go test -race ./mcp -run '^TestCancelListen' -count=10 -timeout=120s`;
  `go test -race ./...`. No external spec lookup or conformance-suite run.
