package imessage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"
	"time"
)

type testRequest struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      string          `json:"id"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params"`
}

type testServer struct {
	reader *io.PipeReader
	writer *io.PipeWriter
	decode *json.Decoder
}

func newTestClient(t *testing.T) (*Client, *testServer) {
	t.Helper()
	reader, serverWriter := io.Pipe()
	serverReader, writer := io.Pipe()
	client := NewClient(reader, writer)
	server := &testServer{serverReader, serverWriter, json.NewDecoder(serverReader)}
	t.Cleanup(func() {
		client.Close()
		serverReader.Close()
		serverWriter.Close()
	})
	return client, server
}

func (s *testServer) request(t *testing.T, method string) testRequest {
	t.Helper()
	var request testRequest
	if err := s.decode.Decode(&request); err != nil {
		t.Fatal(err)
	}
	if request.JSONRPC != "2.0" || request.ID == "" || request.Method != method {
		t.Fatalf("unexpected request: %+v", request)
	}
	return request
}

func (s *testServer) reply(t *testing.T, id string, result any) {
	t.Helper()
	if err := json.NewEncoder(s.writer).Encode(map[string]any{
		"jsonrpc": "2.0", "id": id, "result": result,
	}); err != nil {
		t.Fatal(err)
	}
}

func testContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func await[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case value := <-ch:
		return value
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for client")
		var zero T
		return zero
	}
}

func TestMethodsAndNotifications(t *testing.T) {
	client, server := newTestClient(t)
	ctx := testContext(t)
	finished := make(chan error, 1)
	go func() {
		chats, err := client.Chats(ctx, 12)
		if err == nil && (len(chats) != 1 || chats[0].ID != 42 || !chats[0].IsGroup || chats[0].UnreadCount == nil || *chats[0].UnreadCount != 3) {
			err = fmt.Errorf("unexpected chats: %+v", chats)
		}
		finished <- err
	}()
	request := server.request(t, "chats.list")
	if string(request.Params) != `{"limit":12}` {
		t.Fatalf("unexpected params: %s", request.Params)
	}
	server.reply(t, request.ID, json.RawMessage(`{"chats":[{"id":42,"name":"Team","guid":"iMessage;+;chat42","is_group":true,"participants":["+15551234567"],"unread_count":3,"last_message_at":"2026-09-05T11:00:00Z"}]}`))
	if err := await(t, finished); err != nil {
		t.Fatal(err)
	}

	go func() {
		messages, err := client.History(ctx, 42, 2)
		if err == nil && (len(messages) != 1 || messages[0].ID != 9007199254740993 || messages[0].ChatID != 42 || messages[0].GUID != "msg-guid" || messages[0].Sender != "sender" || messages[0].Text != "hi" || !messages[0].IsFromMe || messages[0].CreatedAt.Nanosecond() != 123456789) {
			err = fmt.Errorf("unexpected messages: %+v", messages)
		}
		finished <- err
	}()
	request = server.request(t, "messages.history")
	if string(request.Params) != `{"chat_id":42,"limit":2}` {
		t.Fatalf("unexpected params: %s", request.Params)
	}
	server.reply(t, request.ID, json.RawMessage(`{"messages":[{"id":9007199254740993,"chat_id":42,"guid":"msg-guid","sender":"sender","text":"hi","is_from_me":true,"created_at":"2026-09-05T11:00:00.123456789Z","attachments":[]}]}`))
	if err := await(t, finished); err != nil {
		t.Fatal(err)
	}

	go func() {
		id, err := client.SubscribeAll(ctx, -1)
		if err == nil && id != 7 {
			err = fmt.Errorf("unexpected subscription: %d", id)
		}
		finished <- err
	}()
	request = server.request(t, "watch.subscribe")
	if string(request.Params) != `{"since_rowid":-1}` {
		t.Fatalf("unexpected params: %s", request.Params)
	}
	server.reply(t, request.ID, json.RawMessage(`{"subscription":7,"buffer_limit":256}`))
	if err := await(t, finished); err != nil {
		t.Fatal(err)
	}
	for _, line := range []string{
		`{"jsonrpc":"2.0","method":"message","params":{"subscription":7,"message":{"id":81,"chat_id":42,"text":"new","is_group":true,"created_at":"2026-09-05T12:00:00Z","attachments":[{"transfer_name":"brief.pdf","mime_type":"application/pdf","total_bytes":1234,"original_path":"/tmp/Messages/Attachments/brief.pdf"}]},"reason":{"extension":true}}}`,
		`{"jsonrpc":"2.0","method":"watch.overflow","params":{"subscription":7,"resume_after_rowid":80,"reason":"buffer_limit_exceeded","terminal":true,"message":"watch status"}}`,
		`{"jsonrpc":"2.0","method":"error","params":{"subscription":7,"error":{"message":"watch failed","code":-32603}}}`,
	} {
		if _, err := fmt.Fprintln(server.writer, line); err != nil {
			t.Fatal(err)
		}
	}
	n := await(t, client.Notifications())
	if n.Method != "message" || n.Subscription != 7 || n.Message == nil || n.Message.ID != 81 || !n.Message.IsGroup ||
		len(n.Message.Attachments) != 1 || n.Message.Attachments[0].TransferName != "brief.pdf" ||
		n.Message.Attachments[0].MIMEType != "application/pdf" || n.Message.Attachments[0].TotalBytes != 1234 {
		t.Fatalf("unexpected notification: %+v", n)
	}
	n = await(t, client.Notifications())
	if n.Method != "watch.overflow" || n.ResumeAfterRowID == nil || *n.ResumeAfterRowID != 80 || !n.Terminal || n.Reason != "buffer_limit_exceeded" {
		t.Fatalf("unexpected overflow: %+v", n)
	}
	n = await(t, client.Notifications())
	if n.Method != "error" || !strings.Contains(string(n.Params), "watch failed") {
		t.Fatalf("unexpected watch error: %+v", n)
	}

	go func() {
		result, err := client.Send(ctx, 42, "hello\nworld")
		if err == nil && (!result.OK || result.GUID != "sent-guid" || result.ID != 82 || result.Transport != "bridge") {
			err = fmt.Errorf("unexpected send: %+v", result)
		}
		finished <- err
	}()
	request = server.request(t, "send")
	if string(request.Params) != `{"chat_id":42,"text":"hello\nworld","service":"imessage"}` {
		t.Fatalf("unexpected params: %s", request.Params)
	}
	server.reply(t, request.ID, json.RawMessage(`{"ok":true,"id":82,"guid":"sent-guid","transport":"bridge"}`))
	if err := await(t, finished); err != nil {
		t.Fatal(err)
	}
}

func TestUnknownNotificationsPreserveParams(t *testing.T) {
	for _, params := range []string{
		`[1, "extension", {"message":"status"}]`,
		`{"message":"watch status","subscription":"extension-owned","terminal":[]}`,
		`{"message":{"id":81},"future_field":true}`,
		`"extension status"`,
		`null`,
		``,
	} {
		t.Run(params, func(t *testing.T) {
			client, server := newTestClient(t)
			line := `{"jsonrpc":"2.0","method":"extension.status"`
			if params != "" {
				line += `,"params":` + params
			}
			if _, err := fmt.Fprintln(server.writer, line+`}`); err != nil {
				t.Fatal(err)
			}
			n := await(t, client.Notifications())
			if n.Method != "extension.status" || string(n.Params) != params {
				t.Fatalf("lost extension notification: %+v", n)
			}
			if n.Subscription != 0 || n.Message != nil || n.ResumeAfterRowID != nil || n.Reason != "" || n.Terminal {
				t.Fatalf("interpreted extension params as known fields: %+v", n)
			}
			if err := client.Err(); err != nil {
				t.Fatalf("extension closed client: %v", err)
			}
			finished := make(chan error, 1)
			ctx := testContext(t)
			go func() { _, err := client.Chats(ctx, 1); finished <- err }()
			request := server.request(t, "chats.list")
			server.reply(t, request.ID, json.RawMessage(`{"chats":[]}`))
			if err := await(t, finished); err != nil {
				t.Fatalf("RPC after extension failed: %v", err)
			}
		})
	}
}

func TestConcurrentOutOfOrderResponses(t *testing.T) {
	client, server := newTestClient(t)
	ctx := testContext(t)
	const count = 40
	finished := make(chan error, count)
	for i := 1; i <= count; i++ {
		go func() {
			chats, err := client.Chats(ctx, i)
			if err == nil && (len(chats) != 1 || chats[0].ID != int64(i)) {
				err = fmt.Errorf("response mismatch for %d: %+v", i, chats)
			}
			finished <- err
		}()
	}
	requests := make([]testRequest, count)
	seen := make(map[string]bool)
	for i := range requests {
		requests[i] = server.request(t, "chats.list")
		if seen[requests[i].ID] {
			t.Fatal("duplicate request ID")
		}
		seen[requests[i].ID] = true
	}
	for i := count - 1; i >= 0; i-- {
		var params struct{ Limit int }
		if err := json.Unmarshal(requests[i].Params, &params); err != nil {
			t.Fatal(err)
		}
		server.reply(t, requests[i].ID, map[string]any{"chats": []Chat{{ID: int64(params.Limit)}}})
	}
	for range count {
		if err := await(t, finished); err != nil {
			t.Fatal(err)
		}
	}
}

func TestRPCErrorPreservesAmbiguousSendData(t *testing.T) {
	for _, data := range []string{`{"retry_safe":false,"disposition":"still_in_flight","transport":"bridge","operation":"send","detail":"redacted","future_field":123}`, `"permission denied"`} {
		t.Run(data, func(t *testing.T) {
			client, server := newTestClient(t)
			finished := make(chan error, 1)
			ctx := testContext(t)
			go func() { _, err := client.Send(ctx, 42, "hi"); finished <- err }()
			request := server.request(t, "send")
			_, err := fmt.Fprintf(server.writer, "{\"jsonrpc\":\"2.0\",\"id\":%q,\"error\":{\"code\":-32001,\"message\":\"uncertain\",\"data\":%s}}\n", request.ID, data)
			if err != nil {
				t.Fatal(err)
			}
			var rpcErr *RPCError
			if err := await(t, finished); !errors.As(err, &rpcErr) || rpcErr.Code != -32001 || string(rpcErr.Data) != data {
				t.Fatalf("lost RPC error data: %v (%+v)", err, rpcErr)
			}
			if client.Err() != nil {
				t.Fatal("RPC errors must not close client")
			}
		})
	}
}

func TestCancellationCleansPendingAndIgnoresLateResponse(t *testing.T) {
	client, server := newTestClient(t)
	ctx, cancel := context.WithCancel(testContext(t))
	defer cancel()
	first := make(chan error, 1)
	go func() { _, err := client.Chats(ctx, 1); first <- err }()
	request := server.request(t, "chats.list")
	second := make(chan error, 1)
	go func() { _, err := client.Chats(testContext(t), 2); second <- err }()
	other := server.request(t, "chats.list") // Confirms the first write released its gate.
	cancel()
	if err := await(t, first); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected cancellation: %v", err)
	}
	client.mu.Lock()
	_, pending := client.pending[request.ID]
	client.mu.Unlock()
	if pending {
		t.Fatal("canceled request remains pending")
	}
	server.reply(t, request.ID, map[string]any{"chats": []Chat{{ID: 99}}})
	server.reply(t, other.ID, map[string]any{"chats": []Chat{}})
	if err := await(t, second); err != nil {
		t.Fatal(err)
	}
	if client.Err() != nil {
		t.Fatalf("late response closed client: %v", client.Err())
	}
}

func TestCloseWakesPendingAndBlockedWriters(t *testing.T) {
	client, server := newTestClient(t)
	ctx := testContext(t)
	finished := make(chan error, 3)
	go func() { _, err := client.Chats(ctx, 1); finished <- err }()
	server.request(t, "chats.list")
	for range 2 {
		go func() { _, err := client.Chats(ctx, 1); finished <- err }()
	}
	var wg sync.WaitGroup
	for range 5 {
		wg.Go(func() { client.Close() })
	}
	wg.Wait()
	for range 3 {
		if err := await(t, finished); !errors.Is(err, ErrClosed) {
			t.Fatalf("expected closed client: %v", err)
		}
	}
	if _, ok := <-client.Notifications(); ok {
		t.Fatal("notification channel is not closed")
	}
	client.mu.Lock()
	defer client.mu.Unlock()
	if len(client.pending) != 0 {
		t.Fatal("pending calls leaked")
	}
}

func TestBlockedWriteCancellation(t *testing.T) {
	client, _ := newTestClient(t)
	ctx, resume := newPausedContext(t, 2)
	cancelCtx, cancel := context.WithCancel(ctx.Context)
	ctx.Context = cancelCtx
	defer cancel()
	finished := make(chan error, 1)
	go func() { _, err := client.Send(ctx, 42, "hi"); finished <- err }()
	await(t, ctx.paused) // The server never reads, so the write cannot complete.
	cancel()
	resume()
	if err := await(t, finished); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected cancellation: %v", err)
	}
	if !errors.Is(client.Err(), context.Canceled) {
		t.Fatalf("blocked partial write did not close client: %v", client.Err())
	}
	await(t, client.readDone)
	select {
	case client.writeGate <- struct{}{}:
		<-client.writeGate
	case <-testContext(t).Done():
		t.Fatal("cancellation did not unblock writer")
	}
}

func TestKnownNotificationsDecodeOnlyTheirFields(t *testing.T) {
	for _, tt := range []struct {
		method string
		params string
	}{
		{"message", `{"subscription":7,"message":{"id":81},"reason":[],"terminal":"ignored"}`},
		{"watch.overflow", `{"subscription":7,"resume_after_rowid":80,"reason":"overflow","terminal":true,"message":"ignored"}`},
		{"error", `{"subscription":7,"message":"watch failed","error":{"code":-1},"terminal":"ignored"}`},
	} {
		t.Run(tt.method, func(t *testing.T) {
			client, server := newTestClient(t)
			if _, err := fmt.Fprintf(server.writer, "{\"jsonrpc\":\"2.0\",\"method\":%q,\"params\":%s}\n", tt.method, tt.params); err != nil {
				t.Fatal(err)
			}
			n := await(t, client.Notifications())
			if n.Method != tt.method || string(n.Params) != tt.params || n.Subscription != 7 {
				t.Fatalf("lost known notification fields: %+v", n)
			}
			switch tt.method {
			case "message":
				if n.Message == nil || n.Message.ID != 81 || n.Reason != "" || n.Terminal {
					t.Fatalf("unexpected message fields: %+v", n)
				}
			case "watch.overflow":
				if n.ResumeAfterRowID == nil || *n.ResumeAfterRowID != 80 || n.Reason != "overflow" || !n.Terminal || n.Message != nil {
					t.Fatalf("unexpected overflow fields: %+v", n)
				}
			case "error":
				if n.Message != nil || n.Terminal {
					t.Fatalf("unexpected error fields: %+v", n)
				}
			}
		})
	}
}

func TestNotificationOverflowTerminatesExplicitly(t *testing.T) {
	client, server := newTestClient(t)
	for i := 0; i <= NotificationBufferSize; i++ {
		_, err := fmt.Fprintf(server.writer, "{\"jsonrpc\":\"2.0\",\"method\":\"message\",\"params\":{\"subscription\":1,\"message\":{\"id\":%d}}}\n", i+1)
		if err != nil {
			t.Fatal(err)
		}
	}
	await(t, client.readDone)
	if !errors.Is(client.Err(), ErrNotificationOverflow) {
		t.Fatalf("expected explicit overflow: %v", client.Err())
	}
	count := 0
	for n := range client.Notifications() {
		count++
		if n.Message.ID != int64(count) {
			t.Fatal("buffered notification order changed")
		}
	}
	if count != NotificationBufferSize {
		t.Fatalf("buffered records lost: %d", count)
	}
}

func TestReaderFailures(t *testing.T) {
	for _, tt := range []struct {
		name string
		line string
		want error
	}{
		{"malformed", "not JSON\n", ErrProtocol},
		{"version", "{\"jsonrpc\":\"1.0\"}\n", ErrProtocol},
		{"missing message", "{\"jsonrpc\":\"2.0\",\"method\":\"message\",\"params\":{}}\n", ErrProtocol},
		{"message array", "{\"jsonrpc\":\"2.0\",\"method\":\"message\",\"params\":[]}\n", ErrProtocol},
		{"message string", "{\"jsonrpc\":\"2.0\",\"method\":\"message\",\"params\":{\"message\":\"invalid\"}}\n", ErrProtocol},
		{"overflow array", "{\"jsonrpc\":\"2.0\",\"method\":\"watch.overflow\",\"params\":[]}\n", ErrProtocol},
		{"overflow cursor", "{\"jsonrpc\":\"2.0\",\"method\":\"watch.overflow\",\"params\":{\"resume_after_rowid\":\"invalid\"}}\n", ErrProtocol},
		{"invalid response", "{\"jsonrpc\":\"2.0\",\"id\":\"1\"}\n", ErrProtocol},
		{"both result and error", "{\"jsonrpc\":\"2.0\",\"id\":\"1\",\"result\":{},\"error\":{\"code\":-1,\"message\":\"bad\"}}\n", ErrProtocol},
		{"oversize", strings.Repeat("x", MaxRecordBytes+1), ErrRecordTooLarge},
		{"EOF", "", io.EOF},
	} {
		t.Run(tt.name, func(t *testing.T) {
			client, server := newTestClient(t)
			finished := make(chan error, 1)
			ctx := testContext(t)
			go func() { _, err := client.Chats(ctx, 1); finished <- err }()
			server.request(t, "chats.list")
			_, _ = io.WriteString(server.writer, tt.line)
			server.writer.Close()
			await(t, client.readDone)
			if !errors.Is(client.Err(), tt.want) {
				t.Fatalf("expected %v: %v", tt.want, client.Err())
			}
			if err := await(t, finished); !errors.Is(err, tt.want) {
				t.Fatalf("pending caller got %v, want %v", err, tt.want)
			}
		})
	}
}

type shortWriter struct{}

func (shortWriter) Write(p []byte) (int, error) { return len(p) - 1, nil }
func (shortWriter) Close() error                { return nil }

func TestShortWriteTerminatesClient(t *testing.T) {
	reader, writer := io.Pipe()
	defer writer.Close()
	client := NewClient(reader, shortWriter{})
	defer client.Close()
	if _, err := client.Send(testContext(t), 42, "hi"); !errors.Is(err, io.ErrShortWrite) {
		t.Fatalf("short write not surfaced: %v", err)
	}
	await(t, client.readDone)
}

type pausedContext struct {
	context.Context
	pauseAt int
	calls   int
	paused  chan struct{}
	resume  chan struct{}
}

// Done pauses call at a specific select, making all competing events ready
// before selection rather than relying on sleeps or scheduler timing.
func (c *pausedContext) Done() <-chan struct{} {
	c.calls++
	if c.calls == c.pauseAt {
		close(c.paused)
		<-c.resume
	}
	return c.Context.Done()
}

func newPausedContext(t *testing.T, pauseAt int) (*pausedContext, func()) {
	t.Helper()
	ctx := &pausedContext{
		Context: testContext(t), pauseAt: pauseAt,
		paused: make(chan struct{}), resume: make(chan struct{}),
	}
	resume := sync.OnceFunc(func() { close(ctx.resume) })
	t.Cleanup(resume)
	return ctx, resume
}

type functionWriter func([]byte) (int, error)

func (w functionWriter) Write(p []byte) (int, error) { return w(p) }
func (functionWriter) Close() error                  { return nil }

func TestWriteFailurePoisonsBeforeReleasingGate(t *testing.T) {
	writeErr := errors.New("write failed")
	for _, tt := range []struct {
		name string
		n    int
		err  error
		want error
	}{
		{"failed", 0, writeErr, writeErr},
		{"partial error", 1, writeErr, writeErr},
		{"short write", 1, nil, io.ErrShortWrite},
	} {
		t.Run(tt.name, func(t *testing.T) {
			reader, serverWriter := io.Pipe()
			t.Cleanup(func() { serverWriter.Close() })
			writes := make(chan struct{}, 2)
			client := NewClient(reader, functionWriter(func([]byte) (int, error) {
				writes <- struct{}{}
				return tt.n, tt.err
			}))
			t.Cleanup(func() { client.Close() })
			ctx, resume := newPausedContext(t, 2)
			finished := make(chan error, 1)
			go func() { _, err := client.Send(ctx, 42, "first"); finished <- err }()
			await(t, ctx.paused)

			// The caller cannot process the write result yet. Acquiring the
			// gate proves the writer itself made the failure terminal first.
			select {
			case client.writeGate <- struct{}{}:
			case <-testContext(t).Done():
				t.Fatal("writer did not release gate")
			}
			err := client.Err()
			<-client.writeGate
			if !errors.Is(err, tt.want) {
				t.Fatalf("gate released before connection was poisoned: %v", err)
			}
			if _, err := client.Send(testContext(t), 42, "second"); !errors.Is(err, tt.want) {
				t.Fatalf("later call did not retain write failure: %v", err)
			}
			resume()
			if err := await(t, finished); !errors.Is(err, tt.want) {
				t.Fatalf("first call did not retain write failure: %v", err)
			}
			if len(writes) != 1 {
				t.Fatalf("wrote %d requests on failed connection", len(writes))
			}
		})
	}
}

func TestCompletedWriteCancellationKeepsClientHealthy(t *testing.T) {
	for _, tt := range []struct {
		name   string
		record string
	}{
		{"no response", ""},
		{"ack", `"result":{"ok":true,"id":82}`},
		{"RPC error", `"error":{"code":-32001,"message":"uncertain","data":{"retry_safe":false}}`},
	} {
		t.Run(tt.name, func(t *testing.T) {
			// Both select arms are ready on resume; repeat to exercise either
			// choice without relying on sleeps to arrange the race.
			for range 20 {
				client, server := newTestClient(t)
				ctx, resume := newPausedContext(t, 2)
				canceled, cancel := context.WithCancel(ctx.Context)
				ctx.Context = canceled
				t.Cleanup(cancel)
				type outcome struct {
					result SendResult
					err    error
				}
				finished := make(chan outcome, 1)
				go func() {
					result, err := client.Send(ctx, 42, "hi")
					finished <- outcome{result, err}
				}()
				request := server.request(t, "send")
				await(t, ctx.paused)
				// Acquiring the gate proves success is queued in written while
				// the caller is still paused before its write-phase select.
				select {
				case client.writeGate <- struct{}{}:
				case <-testContext(t).Done():
					t.Fatal("writer did not release gate")
				}
				<-client.writeGate
				if tt.record != "" {
					if err := client.handleRecord([]byte(fmt.Sprintf(`{"jsonrpc":"2.0","id":%q,%s}`, request.ID, tt.record))); err != nil {
						t.Fatal(err)
					}
				}
				cancel()
				resume()
				got := await(t, finished)
				switch tt.name {
				case "no response":
					if !errors.Is(got.err, context.Canceled) {
						t.Fatalf("expected cancellation: %v", got.err)
					}
				case "ack":
					if got.err != nil || !got.result.OK || got.result.ID != 82 {
						t.Fatalf("lost acknowledgement: %+v, %v", got.result, got.err)
					}
				case "RPC error":
					var rpcErr *RPCError
					if !errors.As(got.err, &rpcErr) || rpcErr.Code != -32001 || string(rpcErr.Data) != `{"retry_safe":false}` {
						t.Fatalf("lost RPC error: %v", got.err)
					}
				}
				if err := client.Err(); err != nil {
					t.Fatalf("completed write cancellation closed client: %v", err)
				}
				client.mu.Lock()
				pending := len(client.pending)
				client.mu.Unlock()
				if pending != 0 {
					t.Fatalf("pending calls leaked: %d", pending)
				}
				next := make(chan error, 1)
				nextCtx := testContext(t)
				go func() { _, err := client.Chats(nextCtx, 1); next <- err }()
				other := server.request(t, "chats.list")
				if tt.record == "" {
					server.reply(t, request.ID, json.RawMessage(`{"ok":true}`))
				}
				server.reply(t, other.ID, json.RawMessage(`{"chats":[]}`))
				if err := await(t, next); err != nil {
					t.Fatalf("subsequent RPC failed: %v", err)
				}
				client.Close()
			}
		})
	}
}

func TestReceivedResponseWinsOverEOF(t *testing.T) {
	for _, phase := range []struct {
		name    string
		pauseAt int
	}{
		{"write completion", 2},
		{"response wait", 3},
	} {
		for _, tt := range []struct {
			name   string
			record string
		}{
			{"ack", `"result":{"ok":true,"id":82,"guid":"sent-guid","transport":"bridge"}`},
			{"RPC error", `"error":{"code":-32001,"message":"uncertain","data":{"retry_safe":false,"disposition":"still_in_flight","future_field":123}}`},
		} {
			t.Run(phase.name+"/"+tt.name, func(t *testing.T) {
				client, server := newTestClient(t)
				ctx, resume := newPausedContext(t, phase.pauseAt)
				type outcome struct {
					result SendResult
					err    error
				}
				finished := make(chan outcome, 1)
				go func() {
					result, err := client.Send(ctx, 42, "hi")
					finished <- outcome{result, err}
				}()
				request := server.request(t, "send")
				await(t, ctx.paused)
				if _, err := fmt.Fprintf(server.writer, "{\"jsonrpc\":\"2.0\",\"id\":%q,%s}\n", request.ID, tt.record); err != nil {
					t.Fatal(err)
				}
				server.writer.Close()
				await(t, client.readDone)
				if !errors.Is(client.Err(), io.EOF) {
					t.Fatalf("expected terminal EOF: %v", client.Err())
				}
				resume()
				got := await(t, finished)
				if tt.name == "ack" {
					if got.err != nil || !got.result.OK || got.result.ID != 82 || got.result.GUID != "sent-guid" || got.result.Transport != "bridge" {
						t.Fatalf("lost received acknowledgement: %+v, %v", got.result, got.err)
					}
				} else {
					var rpcErr *RPCError
					if !errors.As(got.err, &rpcErr) || rpcErr.Code != -32001 || rpcErr.Message != "uncertain" || string(rpcErr.Data) != `{"retry_safe":false,"disposition":"still_in_flight","future_field":123}` {
						t.Fatalf("lost received RPC error: %v (%+v)", got.err, rpcErr)
					}
				}
			})
		}
	}
}

func TestInvalidResults(t *testing.T) {
	for _, result := range []string{`null`, `{}`, `{"chats":null}`, `{"chats":"invalid"}`} {
		t.Run(result, func(t *testing.T) {
			client, server := newTestClient(t)
			ctx := testContext(t)
			finished := make(chan error, 1)
			go func() { _, err := client.Chats(ctx, 1); finished <- err }()
			request := server.request(t, "chats.list")
			server.reply(t, request.ID, json.RawMessage(result))
			if err := await(t, finished); !errors.Is(err, ErrProtocol) {
				t.Fatalf("invalid result accepted: %v", err)
			}
		})
	}
}

func TestCanceledContextDoesNotWrite(t *testing.T) {
	client, _ := newTestClient(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := client.Chats(ctx, 1); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if client.Err() != nil {
		t.Fatal(client.Err())
	}
}

func TestValidationAndDefaultLimit(t *testing.T) {
	client, server := newTestClient(t)
	ctx := testContext(t)
	if _, err := client.Chats(ctx, -1); err == nil {
		t.Fatal("negative limit accepted")
	}
	if _, err := client.History(ctx, 0, 1); err == nil {
		t.Fatal("zero chat accepted")
	}
	if _, err := client.SubscribeAll(ctx, -2); err == nil {
		t.Fatal("invalid cursor accepted")
	}
	if _, err := client.Send(ctx, 1, ""); err == nil {
		t.Fatal("empty text accepted")
	}
	finished := make(chan error, 1)
	go func() { _, err := client.Chats(ctx, 0); finished <- err }()
	request := server.request(t, "chats.list")
	if string(request.Params) != "{}" {
		t.Fatalf("default limit not omitted: %s", request.Params)
	}
	server.reply(t, request.ID, map[string]any{"chats": []Chat{}})
	if err := await(t, finished); err != nil {
		t.Fatal(err)
	}
}
