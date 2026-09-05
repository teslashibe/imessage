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

// SubscribeChat subscribes to one chat and returns a subscription ID. Cursor
// and lifetime semantics match SubscribeAll. The application must persist
// message IDs and deduplicate replayed notifications.
func (c *Client) SubscribeChat(ctx context.Context, chatID, sinceRowID int64) (int64, error) {
	if chatID <= 0 || sinceRowID < -1 {
		return 0, fmt.Errorf("imessage: positive chat ID and cursor >= -1 required")
	}
	params := struct {
		ChatID     int64 `json:"chat_id"`
		SinceRowID int64 `json:"since_rowid"`
	}{chatID, sinceRowID}
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
