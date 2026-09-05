package imessage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"testing"
)

func TestReplyNativeParams(t *testing.T) {
	client, server := newTestClient(t)
	finished := make(chan error, 1)
	go func() {
		result, err := client.Reply(testContext(t), 42, "parent-guid", "literal\nreply")
		if err == nil && (!result.OK || result.GUID != "reply-guid" || result.Transport != "bridge") {
			err = fmt.Errorf("unexpected reply: %+v", result)
		}
		finished <- err
	}()
	request := server.request(t, "send")
	if string(request.Params) != `{"chat_id":42,"text":"literal\nreply","service":"imessage","reply_to":"parent-guid"}` {
		t.Fatalf("unexpected params: %s", request.Params)
	}
	server.reply(t, request.ID, json.RawMessage(`{"ok":true,"guid":"reply-guid","transport":"bridge"}`))
	if err := await(t, finished); err != nil {
		t.Fatal(err)
	}
}

func TestNativeTapbackParams(t *testing.T) {
	for _, reaction := range []Reaction{ReactionLove, ReactionLike, ReactionDislike, ReactionLaugh, ReactionEmphasize, ReactionQuestion} {
		for _, remove := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/remove=%t", reaction, remove), func(t *testing.T) {
				client, server := newTestClient(t)
				ctx := testContext(t)
				finished := make(chan error, 1)
				go func() {
					if remove {
						finished <- client.RemoveReaction(ctx, 42, "parent-guid", reaction)
					} else {
						finished <- client.React(ctx, 42, "parent-guid", reaction)
					}
				}()
				request := server.request(t, "tapback")
				want := fmt.Sprintf(`{"chat_id":42,"message_guid":"parent-guid","reaction":%q,"remove":%t}`, reaction, remove)
				if string(request.Params) != want {
					t.Fatalf("unexpected params: %s", request.Params)
				}
				echo := string(reaction)
				if remove {
					echo = "remove-" + echo
				}
				server.reply(t, request.ID, map[string]any{"ok": true, "reaction": echo})
				if err := await(t, finished); err != nil {
					t.Fatal(err)
				}
			})
		}
	}
}

func TestNativeMutationValidation(t *testing.T) {
	// A zero client panics if a supposedly invalid input reaches RPC dispatch.
	client := &Client{}
	ctx := context.Background()
	for _, chatID := range []int64{0, -1} {
		if _, err := client.Reply(ctx, chatID, "guid", "text"); err == nil {
			t.Fatal("accepted invalid reply chat")
		}
		if err := client.React(ctx, chatID, "guid", ReactionLove); err == nil {
			t.Fatal("accepted invalid reaction chat")
		}
		if err := client.RemoveReaction(ctx, chatID, "guid", ReactionLove); err == nil {
			t.Fatal("accepted invalid removal chat")
		}
	}
	for _, guid := range []string{"", " \t\n"} {
		if _, err := client.Reply(ctx, 42, guid, "text"); err == nil {
			t.Fatal("accepted empty reply target")
		}
		if err := client.React(ctx, 42, guid, ReactionLove); err == nil {
			t.Fatal("accepted empty reaction target")
		}
		if err := client.RemoveReaction(ctx, 42, guid, ReactionLove); err == nil {
			t.Fatal("accepted empty removal target")
		}
	}
	if _, err := client.Reply(ctx, 42, "guid", ""); err == nil {
		t.Fatal("accepted empty reply text")
	}
	for _, reaction := range []Reaction{"", "heart", "LOVE", " love ", "remove-love", "custom", "2000"} {
		if err := client.React(ctx, 42, "guid", reaction); err == nil {
			t.Fatalf("accepted reaction %q", reaction)
		}
		if err := client.RemoveReaction(ctx, 42, "guid", reaction); err == nil {
			t.Fatalf("accepted removal %q", reaction)
		}
	}
}

func TestNativeMutationErrors(t *testing.T) {
	operations := []struct {
		name   string
		method string
		call   func(*Client, context.Context) error
	}{
		{"reply", "send", func(c *Client, ctx context.Context) error {
			_, err := c.Reply(ctx, 42, "guid", "text")
			return err
		}},
		{"react", "tapback", func(c *Client, ctx context.Context) error {
			return c.React(ctx, 42, "guid", ReactionLove)
		}},
		{"remove", "tapback", func(c *Client, ctx context.Context) error {
			return c.RemoveReaction(ctx, 42, "guid", ReactionLove)
		}},
	}
	for _, op := range operations {
		t.Run(op.name, func(t *testing.T) {
			for _, ack := range []string{`{}`, `null`, `[]`, `{"ok":false}`, `{"ok":"true"}`, `{"ok":null}`} {
				t.Run("ack="+ack, func(t *testing.T) {
					client, server := newTestClient(t)
					finished := make(chan error, 1)
					go func() { finished <- op.call(client, testContext(t)) }()
					request := server.request(t, op.method)
					server.reply(t, request.ID, json.RawMessage(ack))
					if err := await(t, finished); !errors.Is(err, ErrProtocol) {
						t.Fatalf("expected protocol error, got %v", err)
					}
				})
			}
			if op.method == "tapback" {
				for _, ack := range []string{`{"ok":true}`, `{"ok":true,"reaction":null}`, `{"ok":true,"reaction":2000}`, `{"ok":true,"reaction":"like"}`, `{"ok":true,"reaction":"remove-love"}`, `{"ok":true,"reaction":"love"}`} {
					if (op.name == "remove" && ack == `{"ok":true,"reaction":"remove-love"}`) || (op.name == "react" && ack == `{"ok":true,"reaction":"love"}`) {
						continue
					}
					t.Run("ack="+ack, func(t *testing.T) {
						client, server := newTestClient(t)
						finished := make(chan error, 1)
						go func() { finished <- op.call(client, testContext(t)) }()
						request := server.request(t, op.method)
						server.reply(t, request.ID, json.RawMessage(ack))
						if err := await(t, finished); !errors.Is(err, ErrProtocol) {
							t.Fatalf("expected protocol error, got %v", err)
						}
					})
				}
			}
			t.Run("upstream", func(t *testing.T) {
				client, server := newTestClient(t)
				finished := make(chan error, 1)
				go func() { finished <- op.call(client, testContext(t)) }()
				request := server.request(t, op.method)
				data := json.RawMessage(`{"retry_safe":false,"disposition":"may_have_completed","detail":"bridge failure"}`)
				if err := json.NewEncoder(server.writer).Encode(map[string]any{
					"jsonrpc": "2.0", "id": request.ID,
					"error": &RPCError{Code: -32001, Message: "Delivery outcome unknown", Data: data},
				}); err != nil {
					t.Fatal(err)
				}
				var rpcErr *RPCError
				if err := await(t, finished); !errors.As(err, &rpcErr) || rpcErr.Code != -32001 || rpcErr.Message != "Delivery outcome unknown" || string(rpcErr.Data) != string(data) {
					t.Fatalf("upstream error not preserved: %v", err)
				}
			})
			t.Run("canceled-before-write", func(t *testing.T) {
				client, _ := newTestClient(t)
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				if err := op.call(client, ctx); !errors.Is(err, context.Canceled) {
					t.Fatalf("expected cancellation, got %v", err)
				}
			})
			t.Run("canceled-after-write", func(t *testing.T) {
				client, server := newTestClient(t)
				ctx, cancel := context.WithCancel(testContext(t))
				defer cancel()
				finished := make(chan error, 1)
				go func() { finished <- op.call(client, ctx) }()
				server.request(t, op.method)
				cancel()
				if err := await(t, finished); !errors.Is(err, context.Canceled) {
					t.Fatalf("expected cancellation, got %v", err)
				}
			})
			t.Run("transport", func(t *testing.T) {
				client, server := newTestClient(t)
				finished := make(chan error, 1)
				go func() { finished <- op.call(client, testContext(t)) }()
				server.request(t, op.method)
				server.writer.Close()
				if err := await(t, finished); !errors.Is(err, io.EOF) {
					t.Fatalf("expected transport EOF, got %v", err)
				}
			})
		})
	}
}
