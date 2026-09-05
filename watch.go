package imessage

import (
	"context"
	"fmt"
)

// SubscribeChat watches only the selected chat. The cursor is exclusive; zero
// starts at the current tail and -1 replays from the beginning. The application
// must persist message IDs and deduplicate any replay.
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
