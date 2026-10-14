// Copyright 2025 The Go MCP SDK Authors. All rights reserved.
// Use of this source code is governed by the license
// that can be found in the LICENSE file.

package mcp

import (
	"context"
	"math"
	"strings"
	"testing"
	"testing/synctest"
	"time"
)

func TestNotificationEncodingFailure(t *testing.T) {
	cycle := map[string]any{}
	cycle["self"] = cycle
	for _, test := range []struct {
		name string
		send func(context.Context, *ServerSession) error
	}{
		{"progress_NaN", func(ctx context.Context, ss *ServerSession) error {
			return ss.NotifyProgress(ctx, &ProgressNotificationParams{
				ProgressToken: "work", Progress: math.NaN(), Message: "bad",
			})
		}},
		{"progress_inf", func(ctx context.Context, ss *ServerSession) error {
			return ss.NotifyProgress(ctx, &ProgressNotificationParams{
				ProgressToken: "work", Progress: 1, Total: math.Inf(1), Message: "bad",
			})
		}},
		{"notification_cycle", func(ctx context.Context, ss *ServerSession) error {
			return ss.NotifyElicitationComplete(ctx, &ElicitationCompleteParams{
				ElicitationID: "bad", Meta: Meta{"value": cycle},
			})
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				var observed []string
				client := NewClient(testImpl, &ClientOptions{
					ProgressNotificationHandler: func(_ context.Context, req *ProgressNotificationClientRequest) {
						observed = append(observed, req.Params.Message)
					},
					ElicitationCompleteHandler: func(_ context.Context, req *ElicitationCompleteNotificationRequest) {
						observed = append(observed, req.Params.ElicitationID)
					},
				})
				sendErrors := make(chan error, 2)
				cs, _, cleanup := basicClientServerConnection(t, client, nil, func(s *Server) {
					AddTool(s, &Tool{Name: "notify"}, func(ctx context.Context, req *CallToolRequest, _ struct{}) (*CallToolResult, any, error) {
						sendErrors <- test.send(ctx, req.Session)
						sendErrors <- req.Session.NotifyProgress(ctx, &ProgressNotificationParams{
							ProgressToken: req.Params.GetProgressToken(), Progress: 1, Total: 1, Message: "good",
						})
						return &CallToolResult{Content: []Content{&TextContent{Text: "ok"}}}, nil, nil
					})
					AddTool(s, &Tool{Name: "normal"}, func(context.Context, *CallToolRequest, struct{}) (*CallToolResult, any, error) {
						return &CallToolResult{Content: []Content{&TextContent{Text: "ok"}}}, nil, nil
					})
				})
				defer cleanup()

				ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
				defer cancel()
				params := &CallToolParams{Name: "notify"}
				params.SetProgressToken("work")
				result, err := cs.CallTool(ctx, params)
				if err != nil {
					t.Fatal(err)
				}
				if result.IsError || textContent(t, result) != "ok" {
					t.Fatalf("tool result = %+v, want success", result)
				}
				if err := <-sendErrors; err == nil || !strings.Contains(err.Error(), "marshaling notify parameters") {
					t.Errorf("invalid notification send = %v, want local marshal error", err)
				}
				if err := <-sendErrors; err != nil {
					t.Errorf("valid progress send: %v", err)
				}

				// A response can arrive before the notification callback finishes.
				synctest.Wait()
				if len(observed) != 1 || observed[0] != "good" {
					t.Errorf("client notifications = %v, want only [good]", observed)
				}
				result, err = cs.CallTool(ctx, &CallToolParams{Name: "normal"})
				if err != nil {
					t.Fatal(err)
				}
				if result.IsError || textContent(t, result) != "ok" {
					t.Fatalf("subsequent tool result = %+v, want success", result)
				}
			})
		})
	}
}
