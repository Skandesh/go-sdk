// Copyright 2025 The Go MCP SDK Authors. All rights reserved.
// Use of this source code is governed by the license
// that can be found in the LICENSE file.

package mcp

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/internal/jsonrpc2"
	"github.com/modelcontextprotocol/go-sdk/jsonrpc"
)

type listenCancelCall struct {
	ctx     context.Context
	extra   *RequestExtra
	session *ServerSession
	done    chan struct{}
}

type listenCancelSent struct {
	params CancelledParams
	route  jsonrpc.ID
}

type listenCancelHarness struct {
	server       *Server
	calls        chan *listenCancelCall
	subscribed   chan *RequestExtra
	unsubscribed chan string
	sent         chan listenCancelSent
	onCancel     MethodHandler
}

func newListenCancelHarness(t *testing.T) *listenCancelHarness {
	t.Helper()
	h := &listenCancelHarness{
		calls:        make(chan *listenCancelCall, 16),
		subscribed:   make(chan *RequestExtra, 16),
		unsubscribed: make(chan string, 16),
		sent:         make(chan listenCancelSent, 64),
	}
	h.server = NewServer(testImpl, &ServerOptions{
		SubscribeHandler: func(_ context.Context, req *SubscribeRequest) error {
			h.subscribed <- req.Extra
			return nil
		},
		UnsubscribeHandler: func(_ context.Context, req *UnsubscribeRequest) error {
			h.unsubscribed <- req.Params.URI
			return nil
		},
	})
	h.server.AddResource(&Resource{Name: "r1", URI: "file:///r1"}, nil)
	h.server.AddReceivingMiddleware(func(next MethodHandler) MethodHandler {
		return func(ctx context.Context, method string, req Request) (Result, error) {
			if method == methodSubscriptionsListen {
				call := &listenCancelCall{ctx, req.GetExtra(), req.GetSession().(*ServerSession), make(chan struct{})}
				h.calls <- call
				defer close(call.done)
			}
			return next(ctx, method, req)
		}
	})
	h.server.AddSendingMiddleware(func(next MethodHandler) MethodHandler {
		return func(ctx context.Context, method string, req Request) (Result, error) {
			if method == notificationCancelled {
				params, ok := req.GetParams().(*CancelledParams)
				if !ok {
					return nil, fmt.Errorf("cancel params have type %T", req.GetParams())
				}
				route, _ := ctx.Value(idContextKey{}).(jsonrpc.ID)
				h.sent <- listenCancelSent{*params, route}
				if h.onCancel != nil {
					return h.onCancel(ctx, method, req)
				}
			}
			return next(ctx, method, req)
		}
	})
	return h
}

func listenCancelWait[T any](t *testing.T, ch <-chan T, what string) T {
	t.Helper()
	select {
	case value := <-ch:
		return value
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for %s", what)
		var zero T
		return zero
	}
}

func listenCancelAsync(f func() error) <-chan error {
	done := make(chan error, 1)
	go func() { done <- f() }()
	return done
}

func listenCancelOK(t *testing.T, f func() error, what string) {
	t.Helper()
	if err := listenCancelWait(t, listenCancelAsync(f), what); err != nil {
		t.Fatalf("%s: %v", what, err)
	}
}

func listenCancelID(t *testing.T, value any) jsonrpc.ID {
	t.Helper()
	id, err := jsonrpc.MakeID(value)
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func listenCancelRequest(t *testing.T, id jsonrpc.ID, active bool) *jsonrpc.Request {
	t.Helper()
	notifications := &NotificationSubscriptions{}
	if active {
		notifications.ResourceSubscriptions = []string{"file:///r1"}
	}
	params, err := json.Marshal(&SubscriptionsListenParams{
		Meta: Meta{
			MetaKeyProtocolVersion:    protocolVersion20260728,
			MetaKeyClientInfo:         testImpl,
			MetaKeyClientCapabilities: map[string]any{},
		},
		Notifications: notifications,
	})
	if err != nil {
		t.Fatal(err)
	}
	return &jsonrpc.Request{ID: id, Method: methodSubscriptionsListen, Params: params}
}

type listenCancelRead struct {
	message jsonrpc.Message
	err     error
}

type listenCancelWire <-chan listenCancelRead

func (wire listenCancelWire) next(t *testing.T) jsonrpc.Message {
	t.Helper()
	got := listenCancelWait(t, wire, "wire message")
	if got.err != nil || got.message == nil {
		t.Fatalf("reading wire message: %v (message %v)", got.err, got.message)
	}
	return got.message
}

func (wire listenCancelWire) end(t *testing.T) {
	t.Helper()
	got := listenCancelWait(t, wire, "wire EOF")
	if got.message != nil || !errors.Is(got.err, io.EOF) {
		t.Fatalf("wire after final result = %+v, want EOF", got)
	}
}

func (wire listenCancelWire) shutdown(t *testing.T, want map[jsonrpc.ID]string) {
	t.Helper()
	cancelled, completed := make(map[jsonrpc.ID]bool), make(map[jsonrpc.ID]bool)
	for {
		got := listenCancelWait(t, wire, "session shutdown wire message")
		if got.message == nil {
			if !errors.Is(got.err, io.EOF) {
				t.Fatalf("shutdown wire error = %v, want EOF", got.err)
			}
			break
		}
		if got.err != nil {
			t.Fatalf("reading shutdown wire message: %v", got.err)
		}
		switch msg := got.message.(type) {
		case *jsonrpc.Request:
			var params CancelledParams
			if err := json.Unmarshal(msg.Params, &params); err != nil {
				t.Fatal(err)
			}
			id := listenCancelID(t, params.RequestID)
			reason, ok := want[id]
			if !ok || cancelled[id] {
				t.Fatalf("unexpected or duplicate cancellation for %v", id.Raw())
			}
			listenCancelNotification(t, msg, id, reason)
			cancelled[id] = true
		case *jsonrpc.Response:
			if !cancelled[msg.ID] || completed[msg.ID] {
				t.Fatalf("result for %v arrived without cancellation, or twice", msg.ID.Raw())
			}
			listenCancelResult(t, msg, msg.ID)
			completed[msg.ID] = true
		}
	}
	// Connection.Close may suppress a final response once shutdown starts.
	for id := range want {
		if !cancelled[id] {
			t.Errorf("missing cancellation before EOF for %v", id.Raw())
		}
	}
}

type listenCancelTransport struct{ Connection }

func (transport listenCancelTransport) Connect(context.Context) (Connection, error) {
	return transport.Connection, nil
}

func (h *listenCancelHarness) memory(t *testing.T, wrap func(Connection) Connection) (*ServerSession, Connection, listenCancelWire) {
	t.Helper()
	ct, st := NewInMemoryTransports()
	var transport Transport = st
	if wrap != nil {
		conn, err := st.Connect(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		transport = listenCancelTransport{wrap(conn)}
	}
	ss, err := h.server.Connect(t.Context(), transport, nil)
	if err != nil {
		t.Fatal(err)
	}
	peer, err := ct.Connect(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = peer.Close()
		listenCancelWait(t, listenCancelAsync(ss.Close), "session cleanup")
	})
	wire := make(chan listenCancelRead, 64)
	go func() {
		defer close(wire)
		for {
			msg, err := peer.Read(t.Context())
			wire <- listenCancelRead{msg, err}
			if err != nil {
				return
			}
		}
	}()
	return ss, peer, wire
}

func (h *listenCancelHarness) http(t *testing.T, request *jsonrpc.Request) (listenCancelWire, context.CancelFunc) {
	t.Helper()
	handler := NewStreamableHTTPHandler(func(*http.Request) *Server { return h.server },
		&StreamableHTTPOptions{Stateless: true, StreamKeepAlive: -1})
	server := httptest.NewServer(handler)
	t.Cleanup(func() {
		server.CloseClientConnections()
		listenCancelOK(t, func() error { server.Close(); return nil }, "HTTP server cleanup")
	})
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	t.Cleanup(cancel)
	body, err := jsonrpc.EncodeMessage(request)
	if err != nil {
		t.Fatal(err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, server.URL, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json, text/event-stream")
	req.Header.Set(protocolVersionHeader, protocolVersion20260728)
	req.Header.Set(methodHeader, methodSubscriptionsListen)
	var resp *http.Response
	listenCancelOK(t, func() error {
		var err error
		resp, err = server.Client().Do(req)
		return err
	}, "HTTP listen response")
	t.Cleanup(func() { resp.Body.Close() })
	if resp.StatusCode != http.StatusOK || baseMediaType(resp.Header.Get("Content-Type")) != "text/event-stream" {
		t.Fatalf("HTTP response = %s, Content-Type %q, want 200 SSE", resp.Status, resp.Header.Get("Content-Type"))
	}
	wire := make(chan listenCancelRead, 64)
	go func() {
		defer close(wire)
		for event, err := range scanEventsLimited(resp.Body, DefaultMaxEventSize) {
			if err != nil {
				wire <- listenCancelRead{err: err}
				return
			}
			if len(event.Data) == 0 {
				continue
			}
			msg, err := jsonrpc.DecodeMessage(event.Data)
			wire <- listenCancelRead{msg, err}
			if err != nil {
				return
			}
		}
		wire <- listenCancelRead{err: io.EOF}
	}()
	return wire, cancel
}

func (h *listenCancelHarness) started(t *testing.T, wire listenCancelWire, id jsonrpc.ID, active bool) (*listenCancelCall, *RequestExtra) {
	t.Helper()
	call := listenCancelWait(t, h.calls, "receiving middleware")
	if call.extra == nil || call.extra.CancelListen == nil {
		t.Fatal("receiving middleware did not receive RequestExtra.CancelListen")
	}
	var subExtra *RequestExtra
	if active {
		subExtra = listenCancelWait(t, h.subscribed, "SubscribeHandler")
		if subExtra == nil || subExtra.CancelListen == nil {
			t.Fatal("SubscribeHandler did not receive RequestExtra.CancelListen")
		}
	}
	msg := wire.next(t)
	ack, ok := msg.(*jsonrpc.Request)
	if !ok || ack.Method != notificationSubscriptionsAck || ack.IsCall() {
		t.Fatalf("first listen message = %+v, want subscriptions/acknowledged notification", msg)
	}
	var params SubscriptionsAcknowledgedParams
	if err := json.Unmarshal(ack.Params, &params); err != nil {
		t.Fatal(err)
	}
	if got := listenCancelID(t, params.Meta[MetaKeySubscriptionID]); got != id {
		t.Fatalf("acknowledged subscription ID = %v, want %v", got.Raw(), id.Raw())
	}
	return call, subExtra
}

func listenCancelNotification(t *testing.T, msg jsonrpc.Message, id jsonrpc.ID, reason string) {
	t.Helper()
	req, ok := msg.(*jsonrpc.Request)
	if !ok || req.Method != notificationCancelled || req.IsCall() {
		t.Fatalf("message = %+v, want notifications/cancelled without an ID", msg)
	}
	var params CancelledParams
	if err := json.Unmarshal(req.Params, &params); err != nil {
		t.Fatal(err)
	}
	if got := listenCancelID(t, params.RequestID); got != id || params.Reason != reason {
		t.Fatalf("cancellation = %+v, want ID %v (%T), reason %q", params, id.Raw(), id.Raw(), reason)
	}
}

func listenCancelResult(t *testing.T, msg jsonrpc.Message, id jsonrpc.ID) {
	t.Helper()
	resp, ok := msg.(*jsonrpc.Response)
	if !ok || resp.ID != id || resp.Error != nil {
		t.Fatalf("message = %+v, want successful final result for %v", msg, id.Raw())
	}
	var result SubscriptionsListenResult
	if err := json.Unmarshal(resp.Result, &result); err != nil {
		t.Fatal(err)
	}
	if got := listenCancelID(t, result.Meta[MetaKeySubscriptionID]); got != id {
		t.Fatalf("final subscription ID = %v, want %v", got.Raw(), id.Raw())
	}
}

func (h *listenCancelHarness) stopped(t *testing.T, call *listenCancelCall) {
	t.Helper()
	if uri := listenCancelWait(t, h.unsubscribed, "UnsubscribeHandler"); uri != "file:///r1" {
		t.Errorf("unsubscribed URI = %q", uri)
	}
	listenCancelWait(t, call.done, "listen handler completion")
}

func (h *listenCancelHarness) sentOnce(t *testing.T, id jsonrpc.ID, reason string) {
	t.Helper()
	got := listenCancelWait(t, h.sent, "sending middleware cancellation")
	if got.params.RequestID != id.Raw() || got.params.Reason != reason || got.route != id {
		t.Errorf("sending middleware saw %+v, want original ID %v (%T), route %v, reason %q", got, id.Raw(), id.Raw(), id, reason)
	}
	h.noSend(t)
}

func (h *listenCancelHarness) noSend(t *testing.T) {
	t.Helper()
	select {
	case got := <-h.sent:
		t.Fatalf("unexpected cancellation send attempt: %+v", got)
	default:
	}
}

func TestCancelListenExplicit(t *testing.T) {
	for _, transport := range []string{"memory", "http"} {
		for _, rawID := range []any{float64(73), "listen-73"} {
			t.Run(fmt.Sprintf("%s/%T", transport, rawID), func(t *testing.T) {
				h := newListenCancelHarness(t)
				id := listenCancelID(t, rawID)
				request := listenCancelRequest(t, id, true)
				var wire listenCancelWire
				if transport == "http" {
					wire, _ = h.http(t, request)
				} else {
					_, peer, w := h.memory(t, nil)
					wire = w
					listenCancelOK(t, func() error { return peer.Write(t.Context(), request) }, "listen request")
				}
				call, subExtra := h.started(t, wire, id, true)
				first, second := call.extra.CancelListen, subExtra.CancelListen
				if _, numeric := rawID.(float64); numeric {
					first, second = second, first
				}
				// A retained callback must route to its own listen even when called
				// from a background task or a different request's context.
				ctx := context.WithValue(context.Background(), idContextKey{}, listenCancelID(t, "unrelated-request"))
				listenCancelOK(t, func() error { return first(ctx, "resource retired") }, "explicit cancellation")
				listenCancelOK(t, func() error { return second(context.Background(), "duplicate") }, "duplicate cancellation")
				listenCancelNotification(t, wire.next(t), id, "resource retired")
				listenCancelResult(t, wire.next(t), id)
				h.stopped(t, call)
				listenCancelOK(t, call.session.Close, "session close")
				listenCancelOK(t, func() error { return first(context.Background(), "after close") }, "retained callback after close")
				wire.end(t)
				h.sentOnce(t, id, "resource retired")
			})
		}
	}
}

func TestCancelListenBeforeLocalTeardown(t *testing.T) {
	h := newListenCancelHarness(t)
	entered, release := make(chan struct{}), make(chan struct{})
	t.Cleanup(func() {
		select {
		case <-release:
		default:
			close(release)
		}
	})
	h.onCancel = func(ctx context.Context, method string, req Request) (Result, error) {
		close(entered)
		select {
		case <-release:
		case <-t.Context().Done():
			return nil, t.Context().Err()
		}
		return defaultSendingMethodHandler(ctx, method, req)
	}
	ss, peer, wire := h.memory(t, nil)
	id := listenCancelID(t, "ordered")
	request := listenCancelRequest(t, id, true)
	listenCancelOK(t, func() error { return peer.Write(t.Context(), request) }, "listen request")
	call, _ := h.started(t, wire, id, true)
	done := listenCancelAsync(func() error { return call.extra.CancelListen(context.Background(), "ordered teardown") })
	listenCancelWait(t, entered, "sending middleware entry")
	if err := call.ctx.Err(); err != nil {
		t.Errorf("listen context canceled before notification was sent: %v", err)
	}
	select {
	case <-call.done:
		t.Error("listen handler returned before notification was sent")
	default:
	}
	close(release)
	if err := listenCancelWait(t, done, "cancellation callback"); err != nil {
		t.Fatal(err)
	}
	listenCancelNotification(t, wire.next(t), id, "ordered teardown")
	listenCancelResult(t, wire.next(t), id)
	h.stopped(t, call)
	listenCancelOK(t, ss.Close, "session close")
	wire.end(t)
	h.sentOnce(t, id, "ordered teardown")
}

func TestCancelListenSessionClose(t *testing.T) {
	for _, transport := range []string{"memory", "http"} {
		t.Run(transport, func(t *testing.T) {
			h := newListenCancelHarness(t)
			ids := []jsonrpc.ID{listenCancelID(t, float64(41)), listenCancelID(t, "listen-42")}
			var wire listenCancelWire
			var calls []*listenCancelCall
			if transport == "http" {
				ids = ids[1:]
				wire, _ = h.http(t, listenCancelRequest(t, ids[0], true))
				call, _ := h.started(t, wire, ids[0], true)
				calls = append(calls, call)
			} else {
				_, peer, w := h.memory(t, nil)
				wire = w
				for _, id := range ids {
					request := listenCancelRequest(t, id, true)
					listenCancelOK(t, func() error { return peer.Write(t.Context(), request) }, "listen request")
					call, _ := h.started(t, wire, id, true)
					calls = append(calls, call)
				}
			}
			ss := calls[0].session
			listenCancelOK(t, ss.Close, "automatic cancellation on session close")
			listenCancelOK(t, ss.Close, "second session close")
			want := make(map[jsonrpc.ID]string)
			for _, id := range ids {
				want[id] = "server session closed"
			}
			wire.shutdown(t, want)
			for _, call := range calls {
				h.stopped(t, call)
				listenCancelOK(t, func() error { return call.extra.CancelListen(context.Background(), "retained") }, "callback after session close")
			}
			seen := make(map[jsonrpc.ID]bool)
			for range ids {
				got := listenCancelWait(t, h.sent, "automatic cancellation through middleware")
				if want[got.route] != "server session closed" || seen[got.route] || got.params.RequestID != got.route.Raw() || got.params.Reason != "server session closed" {
					t.Errorf("unexpected automatic cancellation: %+v", got)
				}
				seen[got.route] = true
			}
			h.noSend(t)
		})
	}
}

func TestCancelListenAdmissionDuringClose(t *testing.T) {
	h := newListenCancelHarness(t)
	entered, release := make(chan struct{}, 16), make(chan struct{})
	h.onCancel = func(ctx context.Context, method string, req Request) (Result, error) {
		entered <- struct{}{}
		select {
		case <-release:
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-t.Context().Done():
			return nil, t.Context().Err()
		}
		return defaultSendingMethodHandler(ctx, method, req)
	}
	ss, peer, wire := h.memory(t, nil)
	t.Cleanup(func() {
		select {
		case <-release:
		default:
			close(release)
		}
	})
	id := listenCancelID(t, "active-before-close")
	request := listenCancelRequest(t, id, true)
	listenCancelOK(t, func() error { return peer.Write(t.Context(), request) }, "first listen request")
	call, _ := h.started(t, wire, id, true)
	closed := listenCancelAsync(ss.Close)
	listenCancelWait(t, entered, "session close cancellation entering middleware")
	if err := call.ctx.Err(); err != nil {
		t.Fatalf("active listen ended before its cancellation was sent: %v", err)
	}

	lateID := listenCancelID(t, "arrived-during-close")
	lateRequest := listenCancelRequest(t, lateID, true)
	listenCancelOK(t, func() error { return peer.Write(t.Context(), lateRequest) }, "late listen request")
	msg := wire.next(t)
	response, ok := msg.(*jsonrpc.Response)
	if !ok || response.ID != lateID || !errors.Is(response.Error, jsonrpc2.ErrServerClosing) {
		t.Fatalf("late listen response = %+v, want server-closing error for %v", msg, lateID.Raw())
	}
	select {
	case call := <-h.calls:
		t.Fatalf("late listen reached receiving middleware: %+v", call)
	default:
	}
	select {
	case err := <-closed:
		t.Fatalf("Close returned before the blocked cancellation was released: %v", err)
	default:
	}
	close(release)
	if err := listenCancelWait(t, closed, "session close after rejecting late listen"); err != nil {
		t.Fatal(err)
	}
	wire.shutdown(t, map[jsonrpc.ID]string{id: "server session closed"})
	h.stopped(t, call)
	if err := listenCancelWait(t, listenCancelAsync(ss.Wait), "session fully drained"); err != nil && !errors.Is(err, jsonrpc2.ErrServerClosing) {
		t.Fatalf("session shutdown: %v", err)
	}
	listenCancelOK(t, func() error { return call.extra.CancelListen(context.Background(), "retained") }, "callback after session drained")
	h.sentOnce(t, id, "server session closed")
	select {
	case call := <-h.calls:
		t.Fatalf("late listen entered receiving middleware during shutdown: %+v", call)
	default:
	}
	select {
	case <-h.subscribed:
		t.Fatal("late listen reached SubscribeHandler")
	default:
	}
	select {
	case uri := <-h.unsubscribed:
		t.Fatalf("unexpected additional listen teardown for %q", uri)
	default:
	}
}

func listenCancelPeerCancel(t *testing.T, peer Connection, id jsonrpc.ID) {
	t.Helper()
	params, err := json.Marshal(&CancelledParams{RequestID: id.Raw(), Reason: "peer finished"})
	if err != nil {
		t.Fatal(err)
	}
	listenCancelOK(t, func() error {
		return peer.Write(t.Context(), &jsonrpc.Request{Method: notificationCancelled, Params: params})
	}, "peer cancellation")
}

func TestCancelListenAfterCompletion(t *testing.T) {
	for _, active := range []bool{false, true} {
		t.Run(fmt.Sprintf("peer_cancelled=%v", active), func(t *testing.T) {
			h := newListenCancelHarness(t)
			returned, release := make(chan struct{}), make(chan struct{})
			if active {
				h.server.AddReceivingMiddleware(func(next MethodHandler) MethodHandler {
					return func(ctx context.Context, method string, req Request) (Result, error) {
						result, err := next(ctx, method, req)
						if method == methodSubscriptionsListen {
							close(returned)
							select {
							case <-release:
							case <-t.Context().Done():
							}
						}
						return result, err
					}
				})
			}
			ss, peer, wire := h.memory(t, nil)
			id := listenCancelID(t, "completed")
			request := listenCancelRequest(t, id, active)
			listenCancelOK(t, func() error { return peer.Write(t.Context(), request) }, "listen request")
			call, _ := h.started(t, wire, id, active)
			if active {
				listenCancelPeerCancel(t, peer, id)
				listenCancelWait(t, returned, "peer-cancelled handler before final response")
				listenCancelOK(t, func() error { return call.extra.CancelListen(context.Background(), "no echo") }, "callback before peer-cancelled request retires")
				h.noSend(t)
				close(release)
			}
			listenCancelResult(t, wire.next(t), id)
			listenCancelWait(t, call.done, "completed listen")
			for range 2 {
				listenCancelOK(t, func() error { return call.extra.CancelListen(context.Background(), "too late") }, "callback after completion")
			}
			listenCancelOK(t, ss.Close, "session close")
			wire.end(t)
			h.noSend(t)
		})
	}
}

func TestCancelListenHTTPDisconnect(t *testing.T) {
	h := newListenCancelHarness(t)
	id := listenCancelID(t, "disconnected")
	wire, disconnect := h.http(t, listenCancelRequest(t, id, true))
	call, subExtra := h.started(t, wire, id, true)
	disconnect()
	h.stopped(t, call)
	listenCancelWait(t, listenCancelAsync(call.session.Wait), "session shutdown after disconnect")
	for _, extra := range []*RequestExtra{call.extra, subExtra} {
		listenCancelOK(t, func() error { return extra.CancelListen(context.Background(), "after disconnect") }, "retained callback after disconnect")
	}
	listenCancelOK(t, call.session.Close, "close disconnected session")
	h.noSend(t)
	if got := listenCancelWait(t, wire, "disconnected HTTP stream"); got.message != nil {
		t.Fatalf("unexpected wire message after disconnect: %+v", got.message)
	}
}

func TestCancelListenReusedID(t *testing.T) {
	for _, rawID := range []any{float64(19), "reused"} {
		t.Run(fmt.Sprintf("%T", rawID), func(t *testing.T) {
			h := newListenCancelHarness(t)
			ss, peer, wire := h.memory(t, nil)
			id := listenCancelID(t, rawID)
			request := listenCancelRequest(t, id, true)
			listenCancelOK(t, func() error { return peer.Write(t.Context(), request) }, "first listen")
			old, oldSubExtra := h.started(t, wire, id, true)
			listenCancelPeerCancel(t, peer, id)
			listenCancelResult(t, wire.next(t), id)
			h.stopped(t, old)

			listenCancelOK(t, func() error { return peer.Write(t.Context(), request) }, "listen reusing ID")
			current, _ := h.started(t, wire, id, true)
			for _, extra := range []*RequestExtra{old.extra, oldSubExtra} {
				listenCancelOK(t, func() error { return extra.CancelListen(context.Background(), "stale") }, "stale callback")
			}
			if err := current.ctx.Err(); err != nil {
				t.Fatalf("stale callback canceled the new listen: %v", err)
			}
			listenCancelOK(t, func() error {
				return h.server.ResourceUpdated(t.Context(), &ResourceUpdatedNotificationParams{URI: "file:///r1"})
			}, "resource update on reused ID")
			msg := wire.next(t)
			update, ok := msg.(*jsonrpc.Request)
			if !ok || update.Method != notificationResourceUpdated {
				t.Fatalf("new listen stopped delivering after stale callback: %+v", msg)
			}
			var params ResourceUpdatedNotificationParams
			if err := json.Unmarshal(update.Params, &params); err != nil {
				t.Fatal(err)
			}
			if params.URI != "file:///r1" || listenCancelID(t, params.Meta[MetaKeySubscriptionID]) != id {
				t.Fatalf("resource update = %+v, want URI file:///r1 and subscription %v", params, id.Raw())
			}
			h.noSend(t)
			listenCancelOK(t, func() error { return current.extra.CancelListen(context.Background(), "current") }, "current callback")
			listenCancelNotification(t, wire.next(t), id, "current")
			listenCancelResult(t, wire.next(t), id)
			h.stopped(t, current)
			listenCancelOK(t, ss.Close, "session close")
			wire.end(t)
			h.sentOnce(t, id, "current")
		})
	}
}

func listenCancelConcurrent(start <-chan struct{}, callback func(context.Context, string) error) []<-chan error {
	var calls []<-chan error
	for range 16 {
		calls = append(calls, listenCancelAsync(func() error {
			<-start
			return callback(context.Background(), "concurrent teardown")
		}))
	}
	return calls
}

func TestCancelListenConcurrentSessionClose(t *testing.T) {
	h := newListenCancelHarness(t)
	ss, peer, wire := h.memory(t, nil)
	id := listenCancelID(t, "concurrent-close")
	request := listenCancelRequest(t, id, true)
	listenCancelOK(t, func() error { return peer.Write(t.Context(), request) }, "listen request")
	call, _ := h.started(t, wire, id, true)
	start := make(chan struct{})
	calls := listenCancelConcurrent(start, call.extra.CancelListen)
	var closes []<-chan error
	for range 8 {
		closes = append(closes, listenCancelAsync(func() error {
			<-start
			return ss.Close()
		}))
	}
	close(start)
	for _, done := range calls {
		if err := listenCancelWait(t, done, "concurrent cancellation"); err != nil {
			t.Errorf("concurrent cancellation: %v", err)
		}
	}
	for _, closed := range closes {
		if err := listenCancelWait(t, closed, "concurrent session close"); err != nil {
			t.Fatal(err)
		}
	}
	sent := listenCancelWait(t, h.sent, "winning cancellation")
	if sent.params.Reason != "concurrent teardown" && sent.params.Reason != "server session closed" {
		t.Errorf("winning reason = %q", sent.params.Reason)
	}
	if sent.params.RequestID != id.Raw() || sent.route != id {
		t.Errorf("winning cancellation = %+v, want ID and route %v", sent, id.Raw())
	}
	wire.shutdown(t, map[jsonrpc.ID]string{id: sent.params.Reason})
	h.stopped(t, call)
	listenCancelOK(t, func() error { return call.extra.CancelListen(context.Background(), "after race") }, "callback after race")
	h.noSend(t)
}

func TestCancelListenConcurrentHTTPDisconnect(t *testing.T) {
	h := newListenCancelHarness(t)
	id := listenCancelID(t, float64(88))
	wire, disconnect := h.http(t, listenCancelRequest(t, id, true))
	call, _ := h.started(t, wire, id, true)
	start := make(chan struct{})
	calls := listenCancelConcurrent(start, call.extra.CancelListen)
	closed := listenCancelAsync(func() error { <-start; return call.session.Close() })
	disconnected := listenCancelAsync(func() error { <-start; disconnect(); return nil })
	close(start)
	var firstErr error
	for i, done := range calls {
		err := listenCancelWait(t, done, "cancellation racing disconnect")
		if i == 0 {
			firstErr = err
		} else if err != firstErr {
			t.Errorf("concurrent callback error = %v, want cached first error %v", err, firstErr)
		}
	}
	listenCancelWait(t, disconnected, "HTTP disconnect")
	listenCancelWait(t, closed, "session close racing disconnect")
	h.stopped(t, call)
	listenCancelWait(t, listenCancelAsync(call.session.Wait), "disconnected session shutdown")
	if err := listenCancelWait(t, listenCancelAsync(func() error {
		return call.extra.CancelListen(context.Background(), "after race")
	}), "retained callback after race"); err != firstErr {
		t.Errorf("retained callback error = %v, want %v", err, firstErr)
	}
	var attempts int
	for len(h.sent) > 0 {
		sent := <-h.sent
		attempts++
		if sent.params.RequestID != id.Raw() || sent.route != id {
			t.Errorf("cancellation racing disconnect = %+v, want ID and route %v", sent, id.Raw())
		}
	}
	if attempts > 1 {
		t.Errorf("attempted %d cancellation notifications, want at most one", attempts)
	}
	var cancellations, results int
	for {
		got := listenCancelWait(t, wire, "HTTP stream termination")
		if got.message == nil {
			break
		}
		switch msg := got.message.(type) {
		case *jsonrpc.Request:
			var params CancelledParams
			if err := json.Unmarshal(msg.Params, &params); err != nil {
				t.Fatal(err)
			}
			if params.Reason != "concurrent teardown" && params.Reason != "server session closed" {
				t.Errorf("cancellation reason = %q", params.Reason)
			}
			listenCancelNotification(t, msg, id, params.Reason)
			cancellations++
			if results != 0 {
				t.Error("cancellation arrived after final result")
			}
		case *jsonrpc.Response:
			listenCancelResult(t, msg, id)
			results++
		}
	}
	if cancellations > attempts || results > 1 {
		t.Errorf("wire contained %d cancellations and %d results (%d send attempts)", cancellations, results, attempts)
	}
}

type listenCancelFailConn struct {
	Connection
	err      error
	attempts chan []byte
}

func (conn *listenCancelFailConn) Write(ctx context.Context, msg jsonrpc.Message) error {
	if req, ok := msg.(*jsonrpc.Request); ok && req.Method == notificationCancelled {
		data, err := jsonrpc.EncodeMessage(msg)
		if err != nil {
			return err
		}
		conn.attempts <- data
		return conn.err
	}
	return conn.Connection.Write(ctx, msg)
}

func TestCancelListenNotificationFailure(t *testing.T) {
	for _, failure := range []string{"middleware", "transport"} {
		t.Run(failure, func(t *testing.T) {
			h := newListenCancelHarness(t)
			wantErr := errors.New("cancellation write failed")
			attempts := make(chan []byte, 16)
			var wrap func(Connection) Connection
			if failure == "transport" {
				wrap = func(conn Connection) Connection { return &listenCancelFailConn{conn, wantErr, attempts} }
			} else {
				// Unlike a broken transport, middleware failure does not itself
				// cancel the JSON-RPC connection, so the callback must unwind it.
				h.onCancel = func(context.Context, string, Request) (Result, error) { return nil, wantErr }
			}
			ss, peer, wire := h.memory(t, wrap)
			id := listenCancelID(t, "failed-write")
			request := listenCancelRequest(t, id, true)
			listenCancelOK(t, func() error { return peer.Write(t.Context(), request) }, "listen request")
			call, subExtra := h.started(t, wire, id, true)
			firstErr := listenCancelWait(t, listenCancelAsync(func() error {
				return subExtra.CancelListen(context.Background(), "failed teardown")
			}), "failing cancellation")
			if !errors.Is(firstErr, wantErr) {
				t.Fatalf("cancellation error = %v, want %v", firstErr, wantErr)
			}
			h.stopped(t, call)
			if call.ctx.Err() == nil {
				t.Error("listen context remains live after failed notification")
			}
			if failure == "middleware" {
				listenCancelResult(t, wire.next(t), id)
			} else {
				data := listenCancelWait(t, attempts, "failed wire write")
				msg, err := jsonrpc.DecodeMessage(data)
				if err != nil {
					t.Fatal(err)
				}
				listenCancelNotification(t, msg, id, "failed teardown")
				listenCancelWait(t, listenCancelAsync(ss.Wait), "shutdown after write failure")
			}
			listenCancelOK(t, ss.Close, "session close after notification failure")
			start := make(chan struct{})
			calls := listenCancelConcurrent(start, call.extra.CancelListen)
			close(start)
			for _, done := range calls {
				if err := listenCancelWait(t, done, "cached cancellation failure"); err != firstErr {
					t.Errorf("duplicate cancellation error = %v, want cached %v", err, firstErr)
				}
			}
			wire.end(t)
			h.sentOnce(t, id, "failed teardown")
			select {
			case data := <-attempts:
				t.Errorf("duplicate failed wire write: %s", data)
			default:
			}
		})
	}
}

type listenCancelContextConn struct {
	Connection
	attempts chan []byte
	abort    <-chan struct{}
}

func (conn *listenCancelContextConn) Write(ctx context.Context, msg jsonrpc.Message) error {
	if req, ok := msg.(*jsonrpc.Request); ok && req.Method == notificationCancelled {
		data, err := jsonrpc.EncodeMessage(msg)
		if err != nil {
			return err
		}
		conn.attempts <- data
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-conn.abort:
			return context.Canceled
		}
	}
	return conn.Connection.Write(ctx, msg)
}

func TestCancelListenWriteContext(t *testing.T) {
	for _, mode := range []string{"deadline", "cancelled"} {
		t.Run(mode, func(t *testing.T) {
			h := newListenCancelHarness(t)
			attempts := make(chan []byte, 16)
			ss, peer, wire := h.memory(t, func(conn Connection) Connection {
				return &listenCancelContextConn{conn, attempts, t.Context().Done()}
			})
			id := listenCancelID(t, "write-context")
			request := listenCancelRequest(t, id, true)
			listenCancelOK(t, func() error { return peer.Write(t.Context(), request) }, "listen request")
			call, subExtra := h.started(t, wire, id, true)
			var ctx context.Context
			var cancel context.CancelFunc
			wantErr := context.Canceled
			if mode == "deadline" {
				ctx, cancel = context.WithTimeout(t.Context(), 100*time.Millisecond)
				wantErr = context.DeadlineExceeded
			} else {
				ctx, cancel = context.WithCancel(t.Context())
			}
			defer cancel()
			done := listenCancelAsync(func() error { return subExtra.CancelListen(ctx, "write context ended") })
			data := listenCancelWait(t, attempts, "cancellation entering context-aware writer")
			msg, err := jsonrpc.DecodeMessage(data)
			if err != nil {
				t.Fatal(err)
			}
			listenCancelNotification(t, msg, id, "write context ended")
			if mode == "cancelled" {
				if err := call.ctx.Err(); err != nil {
					t.Errorf("listen canceled while its notification write is still pending: %v", err)
				}
				cancel()
			}
			firstErr := listenCancelWait(t, done, "cancellation write context ending")
			if !errors.Is(firstErr, wantErr) {
				t.Fatalf("cancellation error = %v, want %v", firstErr, wantErr)
			}
			h.stopped(t, call)
			if call.ctx.Err() == nil {
				t.Error("listen context remains live after cancellation write context ended")
			}
			listenCancelResult(t, wire.next(t), id)
			for range 2 {
				err := listenCancelWait(t, listenCancelAsync(func() error {
					return call.extra.CancelListen(context.Background(), "retry with live context")
				}), "cached write-context error")
				if err != firstErr {
					t.Errorf("duplicate callback error = %v, want cached %v", err, firstErr)
				}
			}

			// A context-limited write must leave the connection usable, rather
			// than relying on connection failure to unwind the first listen.
			nextID := listenCancelID(t, "connection-still-open")
			nextRequest := listenCancelRequest(t, nextID, false)
			listenCancelOK(t, func() error { return peer.Write(t.Context(), nextRequest) }, "request after failed cancellation write")
			next, _ := h.started(t, wire, nextID, false)
			listenCancelResult(t, wire.next(t), nextID)
			listenCancelWait(t, next.done, "subsequent request completion")
			listenCancelOK(t, ss.Close, "session close after write-context failure")
			wire.end(t)
			h.sentOnce(t, id, "write context ended")
			select {
			case data := <-attempts:
				t.Errorf("duplicate cancellation write: %s", data)
			default:
			}
		})
	}
}
