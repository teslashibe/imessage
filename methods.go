package imessage

import (
	"context"
	"fmt"
)

// Chats lists recent chats. A zero limit uses the upstream default (20).
func (c *Client) Chats(ctx context.Context, limit int) ([]Chat, error) {
	if limit < 0 {
		return nil, fmt.Errorf("imessage: limit must be non-negative")
	}
	params := struct {
		Limit int `json:"limit,omitempty"`
	}{limit}
	var result struct {
		Chats *[]Chat `json:"chats"`
	}
	if err := c.call(ctx, "chats.list", params, &result); err != nil {
		return nil, err
	}
	if result.Chats == nil {
		return nil, fmt.Errorf("%w: missing chats", ErrProtocol)
	}
	return *result.Chats, nil
}

// History lists messages newest first. A zero limit uses the upstream default
// (50). Attachment metadata is not requested.
func (c *Client) History(ctx context.Context, chatID int64, limit int) ([]Message, error) {
	if chatID <= 0 || limit < 0 {
		return nil, fmt.Errorf("imessage: chat ID must be positive and limit non-negative")
	}
	params := struct {
		ChatID int64 `json:"chat_id"`
		Limit  int   `json:"limit,omitempty"`
	}{chatID, limit}
	var result struct {
		Messages *[]Message `json:"messages"`
	}
	if err := c.call(ctx, "messages.history", params, &result); err != nil {
		return nil, err
	}
	if result.Messages == nil {
		return nil, fmt.Errorf("%w: missing messages", ErrProtocol)
	}
	return *result.Messages, nil
}

// Subscribe starts an all-chat watch and returns its subscription ID. Zero starts
// at the current tail, -1 replays from the beginning, and positive cursors resume
// exclusively after that row. The context controls the RPC, not the lifetime of
// the subscription; Close ends all subscriptions. Events arrive on Notifications.
func (c *Client) Subscribe(ctx context.Context, sinceRowID int64) (int64, error) {
	if sinceRowID < -1 {
		return 0, fmt.Errorf("imessage: since row ID must be at least -1")
	}
	params := struct {
		SinceRowID int64 `json:"since_rowid"`
	}{sinceRowID}
	var result struct {
		Subscription int64 `json:"subscription"`
	}
	if err := c.call(ctx, "watch.subscribe", params, &result); err != nil {
		return 0, err
	}
	if result.Subscription <= 0 {
		return 0, fmt.Errorf("%w: missing subscription ID", ErrProtocol)
	}
	return result.Subscription, nil
}

// Send sends text to an existing chat. It never retries. An RPCError preserves
// upstream delivery disposition; cancellation or transport failure after writing
// leaves delivery uncertain and must not be treated as safe to retry.
func (c *Client) Send(ctx context.Context, chatID int64, text string) (SendResult, error) {
	if chatID <= 0 || text == "" {
		return SendResult{}, fmt.Errorf("imessage: chat ID must be positive and text non-empty")
	}
	params := struct {
		ChatID  int64  `json:"chat_id"`
		Text    string `json:"text"`
		Service string `json:"service"`
	}{chatID, text, "imessage"}
	var result SendResult
	if err := c.call(ctx, "send", params, &result); err != nil {
		return SendResult{}, err
	}
	if !result.OK {
		return result, fmt.Errorf("%w: send did not acknowledge success", ErrProtocol)
	}
	return result, nil
}
