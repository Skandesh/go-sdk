// Copyright 2025 The Go MCP SDK Authors. All rights reserved.
// Use of this source code is governed by the license
// that can be found in the LICENSE file.

package mcp

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/jsonschema-go/jsonschema"
	"github.com/modelcontextprotocol/go-sdk/internal/jsonrpc2"
	"github.com/modelcontextprotocol/go-sdk/jsonrpc"
)

// Issue #1358: a result that cannot be encoded must not leave its HTTP request hanging.
func TestStreamableHTTPStructuredContentEncoding(t *testing.T) {
	cycle := map[string]any{}
	cycle["self"] = cycle
	large := strings.Repeat("x", 1<<20)
	results := []struct {
		name      string
		value     any
		id        jsonrpc.ID
		wantError bool
	}{
		{"NaN", math.NaN(), jsonrpc2.Int64ID(1358), true},
		{"positive_inf", math.Inf(1), jsonrpc2.StringID("infinite-result"), true},
		{"cycle", cycle, jsonrpc2.Int64ID(1359), true},
		{"large", large, jsonrpc2.Int64ID(1360), false},
	}
	for _, mode := range []struct {
		name         string
		stateless    bool
		jsonResponse bool
	}{
		{name: "stateless_SSE", stateless: true},
		{name: "stateless_JSON", stateless: true, jsonResponse: true},
		{name: "stateful_SSE"},
		{name: "stateful_JSON", jsonResponse: true},
	} {
		t.Run(mode.name, func(t *testing.T) {
			server := NewServer(testImpl, nil)
			for _, result := range results {
				server.AddTool(&Tool{Name: result.name, InputSchema: &jsonschema.Schema{Type: "object"}},
					func(context.Context, *CallToolRequest) (*CallToolResult, error) {
						return &CallToolResult{StructuredContent: map[string]any{"value": result.value}}, nil
					})
			}
			server.AddTool(&Tool{Name: "ok", InputSchema: &jsonschema.Schema{Type: "object"}},
				func(context.Context, *CallToolRequest) (*CallToolResult, error) {
					return &CallToolResult{Content: []Content{&TextContent{Text: "ok"}}}, nil
				})
			handler := NewStreamableHTTPHandler(func(*http.Request) *Server { return server }, &StreamableHTTPOptions{
				Stateless:    mode.stateless,
				JSONResponse: mode.jsonResponse,
			})
			httpServer := httptest.NewServer(mustNotPanic(t, handler))
			t.Cleanup(func() {
				handler.closeAll()
				httpServer.Close()
			})

			var sessionID string
			if !mode.stateless {
				initialize := req(1, methodInitialize, &InitializeParams{
					ProtocolVersion: protocolVersion20251125,
					ClientInfo:      testImpl,
					Capabilities:    &ClientCapabilities{},
				})
				response, id := structuredContentRPC(t, httpServer.URL, "", initialize)
				if response.Error != nil {
					t.Fatalf("initialize: %v", response.Error)
				}
				if id == "" {
					t.Fatalf("initialize response missing %s header", sessionIDHeader)
				}
				sessionID = id
				structuredContentRPC(t, httpServer.URL, sessionID, req(0, notificationInitialized, &InitializedParams{}))
			}

			for _, test := range results {
				t.Run(test.name, func(t *testing.T) {
					t.Run("response", func(t *testing.T) {
						call := req(1, methodCallTool, &CallToolParams{Name: test.name})
						call.ID = test.id
						response, _ := structuredContentRPC(t, httpServer.URL, sessionID, call)
						if test.wantError {
							var rpcErr *jsonrpc.Error
							if !errors.As(response.Error, &rpcErr) {
								t.Fatalf("error = %v, want JSON-RPC internal error (-32603)", response.Error)
							}
							if rpcErr.Code != jsonrpc.CodeInternalError {
								t.Errorf("error code = %d, want %d", rpcErr.Code, jsonrpc.CodeInternalError)
							}
							if len(response.Result) != 0 {
								t.Error("error response also contains a result")
							}
							return
						}
						result := successfulStructuredContentResult(t, response)
						structured, ok := result.StructuredContent.(map[string]any)
						if !ok {
							t.Fatalf("StructuredContent type = %T, want map[string]any", result.StructuredContent)
						}
						payload, ok := structured["value"].(string)
						if !ok || payload != large {
							t.Errorf("large result payload differs from the %d-byte input (got %d bytes)", len(large), len(payload))
						}
					})
					if test.wantError {
						t.Run("recovery", func(t *testing.T) {
							// Reuse the failed request ID to ensure it was retired even
							// though its result could not be encoded.
							call := req(1, methodCallTool, &CallToolParams{Name: "ok"})
							call.ID = test.id
							response, _ := structuredContentRPC(t, httpServer.URL, sessionID, call)
							result := successfulStructuredContentResult(t, response)
							if got := textContent(t, result); got != "ok" {
								t.Errorf("normal tool result = %q, want %q", got, "ok")
							}
						})
					}
				})
			}
		})
	}
}

func structuredContentRPC(t *testing.T, endpoint, sessionID string, message *jsonrpc.Request) (*jsonrpc.Response, string) {
	t.Helper()
	const timeout = 2 * time.Second
	ctx, cancel := context.WithTimeout(t.Context(), timeout)
	defer cancel()

	out := make(chan jsonrpc.Message)
	done := make(chan struct{})
	var messages []jsonrpc.Message
	go func() {
		defer close(done)
		for message := range out {
			messages = append(messages, message)
		}
	}()
	request := streamableRequest{
		method:   http.MethodPost,
		headers:  http.Header{protocolVersionHeader: {protocolVersion20251125}},
		messages: []jsonrpc.Message{message},
	}
	newSessionID, status, _, err := request.do(ctx, endpoint, sessionID, out)
	<-done
	if err != nil {
		t.Fatalf("%s request ID %v failed (timeout %v): %v", message.Method, message.ID.Raw(), timeout, err)
	}
	if !message.IsCall() {
		if status != http.StatusAccepted || len(messages) != 0 {
			t.Fatalf("notification: HTTP status %d, %d messages; want 202 with no messages", status, len(messages))
		}
		return nil, newSessionID
	}
	if status != http.StatusOK {
		t.Fatalf("%s request ID %v: HTTP status = %d, want 200", message.Method, message.ID.Raw(), status)
	}
	if len(messages) != 1 {
		t.Fatalf("%s request ID %v: got %d messages, want one response", message.Method, message.ID.Raw(), len(messages))
	}
	response, ok := messages[0].(*jsonrpc.Response)
	if !ok {
		t.Fatalf("decoded %T, want *jsonrpc.Response", messages[0])
	}
	if response.ID != message.ID {
		t.Errorf("response ID = %v, want %v", response.ID.Raw(), message.ID.Raw())
	}
	return response, newSessionID
}

func successfulStructuredContentResult(t *testing.T, response *jsonrpc.Response) *CallToolResult {
	t.Helper()
	if response.Error != nil {
		t.Fatalf("tool call returned error: %v", response.Error)
	}
	var result CallToolResult
	if err := json.Unmarshal(response.Result, &result); err != nil {
		t.Fatalf("decode tool result: %v", err)
	}
	if result.IsError {
		t.Fatal("tool result IsError = true, want false")
	}
	return &result
}
