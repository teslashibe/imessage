package imessage

import (
	"context"
	"fmt"
	"strings"
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

// Send sends text to an existing chat. It never retries. An RPCError preserves
// upstream delivery disposition; cancellation or transport failure after writing
// leaves delivery uncertain and must not be treated as safe to retry.
func (c *Client) Send(ctx context.Context, chatID int64, text string) (SendResult, error) {
	return c.send(ctx, chatID, text, "")
}

// Reply sends a native inline reply to messageGUID in the given chat, not a
// quoted or prefixed text message. It has the same acknowledgement and retry
// semantics as Send.
//
// Verified against imsg v0.15.1: send with reply_to requires the private IMCore
// bridge; AppleScript cannot send threaded replies. A basic signed imsg install
// with SIP enabled does not provide this capability. Upstream's injected bridge
// requires weakened macOS protections and may still be blocked by library
// validation or entitlement checks. This client neither configures that bridge
// nor changes permissions or protections, and returns upstream RPCError details
// without retrying or falling back to a plain message.
//
// See https://github.com/openclaw/imsg/blob/v0.15.1/Sources/imsg/RPCServer+Handlers.swift
// and https://github.com/openclaw/imsg/blob/v0.15.1/docs/advanced-imcore.md.
func (c *Client) Reply(ctx context.Context, chatID int64, messageGUID, text string) (SendResult, error) {
	if strings.TrimSpace(messageGUID) == "" {
		return SendResult{}, fmt.Errorf("imessage: message GUID must be non-empty")
	}
	return c.send(ctx, chatID, text, messageGUID)
}

func (c *Client) send(ctx context.Context, chatID int64, text, replyTo string) (SendResult, error) {
	if chatID <= 0 || text == "" {
		return SendResult{}, fmt.Errorf("imessage: chat ID must be positive and text non-empty")
	}
	params := struct {
		ChatID  int64  `json:"chat_id"`
		Text    string `json:"text"`
		Service string `json:"service"`
		ReplyTo string `json:"reply_to,omitempty"`
	}{chatID, text, "imessage", replyTo}
	var result SendResult
	if err := c.call(ctx, "send", params, &result); err != nil {
		return SendResult{}, err
	}
	if !result.OK {
		return result, fmt.Errorf("%w: send did not acknowledge success", ErrProtocol)
	}
	return result, nil
}

// React adds a native tapback to messageGUID in the given chat. imsg v0.15.1's
// tapback RPC requires both targets; it cannot infer a chat from a message GUID.
// Reactions target the first message part (upstream's default).
//
// Like Reply, this requires the private IMCore bridge, including its sendReaction
// capability. A basic signed install with SIP enabled cannot use this RPC. The
// separate CLI react command uses AppleScript/accessibility automation and only
// targets the latest incoming message; it is not an RPC fallback. This stream
// client starts no processes and never substitutes emoji text for a tapback.
//
// Success acknowledges acceptance, not delivery. Upstream RPCError details are
// preserved; cancellation or transport failure can leave the outcome uncertain.
// No operation is retried automatically.
//
// See https://github.com/openclaw/imsg/blob/v0.15.1/Sources/imsg/RPCServer+BridgeMessageHandlers.swift
// and https://github.com/openclaw/imsg/blob/v0.15.1/Sources/imsg/Commands/ReactCommand.swift.
func (c *Client) React(ctx context.Context, chatID int64, messageGUID string, reaction Reaction) error {
	return c.tapback(ctx, chatID, messageGUID, reaction, false)
}

// RemoveReaction removes the specified native tapback from messageGUID in the
// given chat. It has the same capability, acknowledgement, and retry constraints
// as React; it does not delete the target message or send any text.
func (c *Client) RemoveReaction(ctx context.Context, chatID int64, messageGUID string, reaction Reaction) error {
	return c.tapback(ctx, chatID, messageGUID, reaction, true)
}

func (c *Client) tapback(ctx context.Context, chatID int64, messageGUID string, reaction Reaction, remove bool) error {
	if chatID <= 0 || strings.TrimSpace(messageGUID) == "" {
		return fmt.Errorf("imessage: chat ID must be positive and message GUID non-empty")
	}
	switch reaction {
	case ReactionLove, ReactionLike, ReactionDislike, ReactionLaugh, ReactionEmphasize, ReactionQuestion:
	default:
		return fmt.Errorf("imessage: unsupported reaction %q", reaction)
	}
	params := struct {
		ChatID      int64    `json:"chat_id"`
		MessageGUID string   `json:"message_guid"`
		Reaction    Reaction `json:"reaction"`
		Remove      bool     `json:"remove"`
	}{chatID, messageGUID, reaction, remove}
	var result struct {
		OK       bool   `json:"ok"`
		Reaction string `json:"reaction"`
	}
	if err := c.call(ctx, "tapback", params, &result); err != nil {
		return err
	}
	expected := string(reaction)
	if remove {
		expected = "remove-" + expected
	}
	if !result.OK || result.Reaction != expected {
		return fmt.Errorf("%w: tapback did not acknowledge %s", ErrProtocol, expected)
	}
	return nil
}
