# imessage

A Go client for the newline-delimited JSON-RPC interface provided by [imsg](https://github.com/openclaw/imsg). It supports chat/history reads, message subscriptions, sending to existing chats, native inline replies, and tapbacks when supported by the upstream runtime.

[agent-go](https://github.com/teslashibe/agent-go) uses this package for its Messages transport. The library is independently usable and owns only the RPC streams—not process startup, SSH connections, authentication, or application policy. It is not a standalone CLI or an implementation of Apple's messaging service.

## Requirements and installation

- Go 1.25.0 or newer, as declared in `go.mod`. Use a currently supported
  patched release; CI tests Go 1.25.13 and Go 1.26.6.
- For live Messages access, an operator-managed `imsg rpc` process on macOS with Messages configured and the permissions required by imsg. Follow [upstream documentation](https://github.com/openclaw/imsg) for installation and permissions.
- The Go transport can run separately from the Mac when supplied appropriate streams. This package does not install imsg or configure remote connections.

Install the package from your application's Go module:

```sh
go get github.com/teslashibe/imessage
```

## Compatibility and privacy

| Area | Support and limitations |
| --- | --- |
| Go | The module declares Go 1.25.0. Use a patched Go toolchain; CI tests 1.25.13 and 1.26.6. |
| imsg | RPC behavior is documented and tested against exactly imsg v0.15.1. Other versions may change methods, fields, or semantics. |
| Reads, sends, and subscriptions | Supported through an operator-managed `imsg rpc` process. The caller owns process startup, authentication, transport security, and access policy. |
| Native replies and tapbacks | In imsg v0.15.1 these require its private IMCore bridge. A normal signed imsg installation with SIP enabled does not provide them. There is no fallback in this package. |
| macOS protections | This package does not install imsg, enable its private bridge, weaken SIP, alter entitlements, or change other system protections. |
| Privacy | Messages, participants, chat metadata, and RPC payloads may be sensitive. Keep them out of logs and issue reports, secure the supplied streams, and grant the imsg host only the permissions it needs. |

## Usage

Supply the stdout reader and stdin writer of an already-started RPC process or equivalent transport. This read-only example returns a count without printing chat metadata:

```go
package example

import (
	"context"
	"io"

	"github.com/teslashibe/imessage"
)

func CountChats(ctx context.Context, reader io.ReadCloser, writer io.WriteCloser) (int, error) {
	client := imessage.NewClient(reader, writer)
	defer client.Close()

	chats, err := client.Chats(ctx, 20)
	return len(chats), err
}
```

`NewClient` immediately starts reading and takes ownership of both streams. Their `Close` methods must unblock pending reads/writes. The caller remains responsible for bounding requests with contexts and waiting for or terminating any child process it starts. Closing this client closes the supplied streams, not an independently managed process.

## API and delivery semantics

- `Chats(ctx, limit)` lists recent chats; zero uses the upstream default of 20.
- `History(ctx, chatID, limit)` returns messages newest first; zero uses the upstream default of 50. It does not request attachment metadata.
- `Send(ctx, chatID, text)` sends iMessage text to an existing positive chat ID.
- `Reply(ctx, chatID, messageGUID, text)` requests a native threaded reply.
- `React` and `RemoveReaction` target an exact chat ID and message GUID. Supported constants are `ReactionLove`, `ReactionLike`, `ReactionDislike`, `ReactionLaugh`, `ReactionEmphasize`, and `ReactionQuestion`.

**An acknowledgement is acceptance, not proof of delivery.** `SendResult.ID` and `GUID` are best-effort. Requests are never automatically retried. `RPCError` retains upstream error data, including delivery disposition and retry-safety information. Cancellation stops waiting, not server execution: a canceled send can still deliver. Interrupted writes close the client to avoid corrupting framing. Reconcile uncertain effects before retrying.

### Native reply and reaction limitations

Source comments document these RPCs against exactly imsg v0.15.1. Native inline replies and tapback RPCs require upstream's private IMCore bridge; a basic signed imsg installation with SIP enabled does not provide these capabilities. That bridge requires weakened macOS protections and can still encounter entitlement or library-validation restrictions. This package does not configure it or change system protections.

There is no fallback to plain text, emoji messages, or the separate upstream CLI reaction command. See the [upstream capability documentation](https://github.com/openclaw/imsg/blob/v0.15.1/docs/advanced-imcore.md) before relying on these operations.

## Subscriptions and recovery

`SubscribeAll(ctx, sinceRowID)` watches all chats; `SubscribeChat(ctx, chatID, sinceRowID)` watches one. Both return a subscription ID:

- `0` starts at the current tail.
- `-1` replays from the beginning.
- Positive cursors resume exclusively after that row ID.

The context bounds subscription setup, not its lifetime. `Close` ends the client's subscriptions. Continuously consume `Notifications()` while subscribed and check `Err()` after the channel drains. Persist message IDs and deduplicate replayed events; row IDs belong to a particular Messages database and are not portable cursors.

Notifications preserve unknown methods and raw parameters. Server `watch.overflow` events expose `ResumeAfterRowID`, `Reason`, and `Terminal` for application-managed recovery. Local notification buffering is bounded to 256 events: overflow terminates the client with `ErrNotificationOverflow`. JSON records are bounded to 8 MiB. Explicit closure records `ErrClosed`; `Close` itself returns nil.

## Development

From the repository root:

```sh
go mod tidy
go build ./...
go vet ./...
go test ./...
go test -race ./...
```

Tests use in-memory streams and fake RPC peers, not live Messages. No executable is built or installed by this Go module. Keep message contents, participant identities, database paths, and raw RPC payloads out of public logs and examples.

## License

[MIT](LICENSE). The external imsg runtime is distributed separately under its own license.
