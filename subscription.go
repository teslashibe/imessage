package imessage

import (
	"context"
	"fmt"
)

// SubscribeAll subscribes to all chats and returns a subscription ID. Zero starts
// at the current tail, -1 replays from the beginning, and positive cursors resume
// exclusively after that row. The context controls the RPC, not the lifetime of
// the subscription; Close ends all subscriptions. Events arrive on Notifications.
func (c *Client) SubscribeAll(ctx context.Context, sinceRowID int64) (int64, error) {
	return c.subscribe(ctx, 0, sinceRowID, false)
}

// SubscribeAllWithAttachments subscribes to all chats and includes attachment
// metadata and resolved local paths in each message notification.
func (c *Client) SubscribeAllWithAttachments(ctx context.Context, sinceRowID int64) (int64, error) {
	return c.subscribe(ctx, 0, sinceRowID, true)
}

func (c *Client) subscribe(ctx context.Context, chatID, sinceRowID int64, attachments bool) (int64, error) {
	if sinceRowID < -1 {
		return 0, fmt.Errorf("imessage: since row ID must be at least -1")
	}
	params := struct {
		ChatID      int64 `json:"chat_id,omitempty"`
		SinceRowID  int64 `json:"since_rowid"`
		Attachments bool  `json:"attachments,omitempty"`
	}{chatID, sinceRowID, attachments}
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

// SubscribeChat subscribes to one chat and returns a subscription ID. Cursor
// and lifetime semantics match SubscribeAll. The application must persist
// message IDs and deduplicate replayed notifications.
func (c *Client) SubscribeChat(ctx context.Context, chatID, sinceRowID int64) (int64, error) {
	return c.subscribeChat(ctx, chatID, sinceRowID, false)
}

// SubscribeChatWithAttachments subscribes to one chat and includes attachment
// metadata and resolved local paths in each message notification.
func (c *Client) SubscribeChatWithAttachments(ctx context.Context, chatID, sinceRowID int64) (int64, error) {
	return c.subscribeChat(ctx, chatID, sinceRowID, true)
}

func (c *Client) subscribeChat(ctx context.Context, chatID, sinceRowID int64, attachments bool) (int64, error) {
	if chatID <= 0 || sinceRowID < -1 {
		return 0, fmt.Errorf("imessage: positive chat ID and cursor >= -1 required")
	}
	return c.subscribe(ctx, chatID, sinceRowID, attachments)
}
