package imessage

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"sync"
)

const (
	// MaxRecordBytes bounds a single inbound or outbound JSON line.
	MaxRecordBytes = 8 << 20
	// NotificationBufferSize bounds unread notifications. Overflow closes the client.
	NotificationBufferSize = 256
)

var (
	ErrClosed               = errors.New("imessage: client closed")
	ErrProtocol             = errors.New("imessage: invalid RPC record")
	ErrRecordTooLarge       = errors.New("imessage: RPC record exceeds size limit")
	ErrNotificationOverflow = errors.New("imessage: notification buffer overflow; stream terminated")
)

// Client multiplexes newline-delimited imsg JSON-RPC over owned streams.
// The supplied Close methods must unblock their respective Read/Write operations.
// Cancellation stops waiting, not server execution: a canceled Send may still
// deliver. Canceling a blocked write closes the client to avoid partial framing.
type Client struct {
	reader io.ReadCloser
	writer io.WriteCloser

	mu      sync.Mutex
	nextID  uint64
	pending map[string]chan response
	err     error

	writeGate     chan struct{}
	done          chan struct{}
	stopped       chan struct{}
	readDone      chan struct{}
	notifications chan Notification
}

type response struct {
	result json.RawMessage
	err    *RPCError
}

// NewClient takes ownership of reader and writer and immediately begins reading.
// Typically these are a child process's stdout and stdin, respectively.
func NewClient(reader io.ReadCloser, writer io.WriteCloser) *Client {
	c := &Client{
		reader: reader, writer: writer,
		pending:   make(map[string]chan response),
		writeGate: make(chan struct{}, 1),
		done:      make(chan struct{}), stopped: make(chan struct{}), readDone: make(chan struct{}),
		notifications: make(chan Notification, NotificationBufferSize),
	}
	go c.readLoop()
	return c
}

// Notifications is closed after terminal failure or Close. Buffered events drain
// first; check Err after draining. Consume it continuously while subscribed.
func (c *Client) Notifications() <-chan Notification { return c.notifications }

// Err returns the first terminal error, or nil while the client is open.
// Explicit Close records ErrClosed.
func (c *Client) Err() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.err
}

// Close closes both streams and waits for the reader to exit. It is idempotent.
// Stream failures are reported through Err; Close itself returns nil.
func (c *Client) Close() error {
	c.stop(ErrClosed)
	<-c.stopped
	<-c.readDone
	return nil
}

func (c *Client) stop(err error) {
	c.mu.Lock()
	if c.err != nil {
		c.mu.Unlock()
		return
	}
	c.err = err
	clear(c.pending)
	close(c.done)
	c.mu.Unlock()
	// Do not hold mu: closing the reader can wake readLoop immediately.
	_ = c.reader.Close()
	_ = c.writer.Close()
	close(c.stopped)
}

func (c *Client) call(ctx context.Context, method string, params any, result any) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case c.writeGate <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	case <-c.done:
		return c.Err()
	}
	if err := ctx.Err(); err != nil {
		<-c.writeGate
		return err
	}

	c.mu.Lock()
	if c.err != nil {
		err := c.err
		c.mu.Unlock()
		<-c.writeGate
		return err
	}
	c.nextID++
	id := strconv.FormatUint(c.nextID, 10)
	replies := make(chan response, 1)
	c.pending[id] = replies
	c.mu.Unlock()
	defer func() {
		c.mu.Lock()
		delete(c.pending, id)
		c.mu.Unlock()
	}()

	request := struct {
		JSONRPC string `json:"jsonrpc"`
		ID      string `json:"id"`
		Method  string `json:"method"`
		Params  any    `json:"params"`
	}{"2.0", id, method, params}
	line, err := json.Marshal(request)
	if err != nil || len(line)+1 > MaxRecordBytes {
		<-c.writeGate
		if err != nil {
			return err
		}
		return ErrRecordTooLarge
	}
	line = append(line, '\n')
	written := make(chan error, 1)
	var writeMu sync.Mutex
	go func() {
		defer func() { <-c.writeGate }()
		n, err := c.writer.Write(line)
		if err == nil && n != len(line) {
			err = io.ErrShortWrite
		}
		writeMu.Lock()
		defer writeMu.Unlock()
		if err != nil {
			// A failed write may have broken framing. Poison the connection
			// before another caller can acquire the write gate.
			c.stop(fmt.Errorf("imessage: write: %w", err))
		}
		written <- err
	}()
	select {
	case err = <-written:
		if err != nil {
			err = c.Err()
		}
	case <-ctx.Done():
		// Serialize with publication so cancellation only closes a write
		// that has not completed. Never hold writeMu during Write itself.
		writeMu.Lock()
		select {
		case err = <-written:
			if err != nil {
				err = c.Err()
			}
		default:
			// An interrupted write may have emitted only part of a JSON line.
			c.stop(fmt.Errorf("imessage: write canceled: %w", ctx.Err()))
			err = ctx.Err()
		}
		writeMu.Unlock()
	case <-c.done:
		err = c.Err()
	}
	var reply response
	if err == nil {
		select {
		case reply = <-replies:
		case <-ctx.Done():
			err = ctx.Err()
		case <-c.done:
			err = c.Err()
		}
	}
	if err != nil {
		// A received response is authoritative even if shutdown became ready
		// in either wait above (including before Write returned).
		select {
		case reply = <-replies:
		default:
			return err
		}
	}
	if reply.err != nil {
		return reply.err
	}
	if err := json.Unmarshal(reply.result, result); err != nil {
		return fmt.Errorf("%w: %s result: %v", ErrProtocol, method, err)
	}
	return nil
}

func (c *Client) readLoop() {
	defer close(c.readDone)
	defer close(c.notifications)
	scanner := bufio.NewScanner(c.reader)
	scanner.Buffer(make([]byte, 4096), MaxRecordBytes)
	for scanner.Scan() {
		if err := c.handleRecord(scanner.Bytes()); err != nil {
			c.stop(err)
			return
		}
		select {
		case <-c.done:
			return
		default:
		}
	}
	err := scanner.Err()
	if errors.Is(err, bufio.ErrTooLong) {
		err = ErrRecordTooLarge
	} else if err == nil {
		err = io.EOF
	}
	c.stop(fmt.Errorf("imessage: read: %w", err))
}

func (c *Client) handleRecord(line []byte) error {
	var record struct {
		JSONRPC string          `json:"jsonrpc"`
		ID      json.RawMessage `json:"id"`
		Method  string          `json:"method"`
		Params  json.RawMessage `json:"params"`
		Result  json.RawMessage `json:"result"`
		Error   *RPCError       `json:"error"`
	}
	if err := json.Unmarshal(line, &record); err != nil {
		return fmt.Errorf("%w: %v", ErrProtocol, err)
	}
	if record.JSONRPC != "2.0" {
		return fmt.Errorf("%w: jsonrpc must be 2.0", ErrProtocol)
	}
	if len(record.ID) == 0 {
		if record.Method == "" || len(record.Result) != 0 || record.Error != nil {
			return fmt.Errorf("%w: invalid notification", ErrProtocol)
		}
		return c.notify(record.Method, record.Params)
	}
	var id string
	if err := json.Unmarshal(record.ID, &id); err != nil || id == "" || record.Method != "" || (len(record.Result) != 0) == (record.Error != nil) {
		return fmt.Errorf("%w: invalid response", ErrProtocol)
	}
	c.mu.Lock()
	replies := c.pending[id]
	delete(c.pending, id)
	if replies != nil {
		// Publish under mu so stop cannot signal done before this response
		// reaches its caller. The single-response channel is buffered.
		replies <- response{result: record.Result, err: record.Error}
	}
	c.mu.Unlock()
	// Late responses to canceled calls are expected and ignored.
	return nil
}

func (c *Client) notify(method string, params json.RawMessage) error {
	n := Notification{Method: method, Params: params}
	switch method {
	case "message":
		var fields struct {
			Subscription int64    `json:"subscription"`
			Message      *Message `json:"message"`
		}
		if len(params) != 0 {
			if err := json.Unmarshal(params, &fields); err != nil {
				return fmt.Errorf("%w: notification params: %v", ErrProtocol, err)
			}
		}
		if fields.Message == nil {
			return fmt.Errorf("%w: missing notification message", ErrProtocol)
		}
		n.Subscription, n.Message = fields.Subscription, fields.Message
	case "watch.overflow":
		var fields struct {
			Subscription     int64  `json:"subscription"`
			ResumeAfterRowID *int64 `json:"resume_after_rowid"`
			Reason           string `json:"reason"`
			Terminal         bool   `json:"terminal"`
		}
		if len(params) != 0 {
			if err := json.Unmarshal(params, &fields); err != nil {
				return fmt.Errorf("%w: notification params: %v", ErrProtocol, err)
			}
		}
		n.Subscription, n.ResumeAfterRowID = fields.Subscription, fields.ResumeAfterRowID
		n.Reason, n.Terminal = fields.Reason, fields.Terminal
	case "error":
		var fields struct {
			Subscription int64 `json:"subscription"`
		}
		if len(params) != 0 {
			if err := json.Unmarshal(params, &fields); err != nil {
				return fmt.Errorf("%w: notification params: %v", ErrProtocol, err)
			}
		}
		n.Subscription = fields.Subscription
	}
	select {
	case <-c.done:
		return c.Err()
	default:
	}
	select {
	case c.notifications <- n:
		return nil
	default:
		return ErrNotificationOverflow
	}
}
