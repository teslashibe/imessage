package imessage

import (
	"context"
	"encoding/json"
	"testing"
)

func TestSubscribeChat(t *testing.T) {
	client, server := newTestClient(t)
	finished := make(chan error, 1)
	go func() { _, err := client.SubscribeChat(context.Background(), 42, 81); finished <- err }()
	request := server.request(t, "watch.subscribe")
	if string(request.Params) != `{"chat_id":42,"since_rowid":81}` {
		t.Fatalf("unexpected params: %s", request.Params)
	}
	server.reply(t, request.ID, json.RawMessage(`{"subscription":3}`))
	if err := await(t, finished); err != nil {
		t.Fatal(err)
	}
}

func TestSubscribeChatWithAttachments(t *testing.T) {
	client, server := newTestClient(t)
	finished := make(chan error, 1)
	go func() {
		_, err := client.SubscribeChatWithAttachments(context.Background(), 42, 81)
		finished <- err
	}()
	request := server.request(t, "watch.subscribe")
	if string(request.Params) != `{"chat_id":42,"since_rowid":81,"attachments":true}` {
		t.Fatalf("unexpected params: %s", request.Params)
	}
	server.reply(t, request.ID, json.RawMessage(`{"subscription":3}`))
	if err := await(t, finished); err != nil {
		t.Fatal(err)
	}
}

func TestSubscribeAllWithAttachments(t *testing.T) {
	client, server := newTestClient(t)
	finished := make(chan error, 1)
	go func() {
		_, err := client.SubscribeAllWithAttachments(context.Background(), -1)
		finished <- err
	}()
	request := server.request(t, "watch.subscribe")
	if string(request.Params) != `{"since_rowid":-1,"attachments":true}` {
		t.Fatalf("unexpected params: %s", request.Params)
	}
	server.reply(t, request.ID, json.RawMessage(`{"subscription":3}`))
	if err := await(t, finished); err != nil {
		t.Fatal(err)
	}
}
