// Copyright 2025 The Go MCP SDK Authors. All rights reserved.
// Use of this source code is governed by the license
// that can be found in the LICENSE file.

package mcp

import (
	"context"
	"sync"

	"github.com/modelcontextprotocol/go-sdk/jsonrpc"
)

type listenState struct {
	session *ServerSession
	id      jsonrpc.ID
	ctx     context.Context
	cancel  context.CancelFunc
	once    sync.Once
	err     error
}

func (l *listenState) stop(ctx context.Context, reason string) error {
	l.once.Do(func() {
		defer l.cancel()
		// A peer cancellation or disconnect is not a server-initiated teardown.
		if l.ctx.Err() != nil {
			return
		}
		ctx, cancel := context.WithTimeout(ctx, notifyCancellationTimeout)
		defer cancel()
		// The caller may use an unrelated context. Route to this listen stream,
		// not a standalone SSE stream or another request's stream.
		ctx = context.WithValue(ctx, idContextKey{}, l.id)
		ctx = context.WithValue(ctx, protocolVersionContextKey{}, protocolVersionFromContext(l.ctx))
		l.err = handleNotify(ctx, notificationCancelled, newServerRequest(l.session, &CancelledParams{
			RequestID: l.id.Raw(),
			Reason:    reason,
		}))
	})
	return l.err
}

func (l *listenState) finish() {
	// Wait for an in-progress send before allowing the terminal response to
	// close the stream, or retire the callback without sending anything.
	l.once.Do(l.cancel)
}
