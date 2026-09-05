package imessage

import (
	"encoding/json"
	"fmt"
	"time"
)

// Chat describes a conversation in the current Messages database.
type Chat struct {
	ID                  int64     `json:"id"`
	Name                string    `json:"name"`
	DisplayName         string    `json:"display_name"`
	ContactName         string    `json:"contact_name"`
	Identifier          string    `json:"identifier"`
	GUID                string    `json:"guid"`
	Service             string    `json:"service"`
	LastMessageAt       time.Time `json:"last_message_at"`
	IsGroup             bool      `json:"is_group"`
	Participants        []string  `json:"participants"`
	AccountID           string    `json:"account_id"`
	AccountLogin        string    `json:"account_login"`
	LastAddressedHandle string    `json:"last_addressed_handle"`
	UnreadCount         *int64    `json:"unread_count,omitempty"`
}

// Message.ID is a row ID scoped to one database instance, not a portable cursor.
type Message struct {
	ID                   int64           `json:"id"`
	GUID                 string          `json:"guid"`
	ChatID               int64           `json:"chat_id"`
	ChatIdentifier       string          `json:"chat_identifier"`
	ChatGUID             string          `json:"chat_guid"`
	ChatName             string          `json:"chat_name"`
	Participants         []string        `json:"participants"`
	IsGroup              bool            `json:"is_group"`
	Sender               string          `json:"sender"`
	SenderName           string          `json:"sender_name"`
	Text                 string          `json:"text"`
	IsFromMe             bool            `json:"is_from_me"`
	CreatedAt            time.Time       `json:"created_at"`
	ReplyToGUID          string          `json:"reply_to_guid"`
	ThreadOriginatorGUID string          `json:"thread_originator_guid"`
	DestinationCallerID  string          `json:"destination_caller_id"`
	BalloonBundleID      string          `json:"balloon_bundle_id"`
	Attachments          []Attachment    `json:"attachments"`
	URLPreview           json.RawMessage `json:"url_preview,omitempty"`
	Poll                 json.RawMessage `json:"poll,omitempty"`
	IsRead               *bool           `json:"is_read,omitempty"`
	DateRead             *time.Time      `json:"date_read,omitempty"`
}

type Attachment struct {
	Filename          string `json:"filename"`
	TransferName      string `json:"transfer_name"`
	UTI               string `json:"uti"`
	MIMEType          string `json:"mime_type"`
	TotalBytes        int64  `json:"total_bytes"`
	IsSticker         bool   `json:"is_sticker"`
	Missing           bool   `json:"missing"`
	OriginalPath      string `json:"original_path"`
	ConvertedPath     string `json:"converted_path"`
	ConvertedMIMEType string `json:"converted_mime_type"`
}

// SendResult acknowledges acceptance, not necessarily delivery. ID and GUID
// are best-effort and can be absent (zero/empty).
type SendResult struct {
	OK        bool   `json:"ok"`
	ID        int64  `json:"id,omitempty"`
	GUID      string `json:"guid,omitempty"`
	Transport string `json:"transport,omitempty"`
}

// RPCError preserves data verbatim, including retry_safe and disposition for
// ambiguous sends. The client never retries a request automatically.
type RPCError struct {
	Code    int             `json:"code"`
	Message string          `json:"message"`
	Data    json.RawMessage `json:"data,omitempty"`
}

func (e *RPCError) Error() string {
	return fmt.Sprintf("imessage RPC error %d: %s", e.Code, e.Message)
}

// Notification preserves every upstream method and its unmodified parameters.
// Message is populated for message notifications. Watch overflow notifications
// expose the server's resumable cursor; generic errors remain available in Params.
type Notification struct {
	Method           string
	Params           json.RawMessage
	Subscription     int64
	Message          *Message
	ResumeAfterRowID *int64
	Reason           string
	Terminal         bool
}
