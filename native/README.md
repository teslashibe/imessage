# Native standard tapbacks on macOS

The Go client remains a stream-only RPC client. This optional, reproducible
upstream patch adds a SIP-enabled backend to `imsg`'s existing `tapback` RPC when
the injected bridge is unavailable. It does not add another Go transport or
change system permissions. Standard upstream `imsg` v0.15.3 still requires the
injected bridge for this RPC; installing an unpatched release does not enable
this implementation.

Build and test on the target Mac:

```sh
./scripts/build-native-imsg --output /absolute/new/verification-directory
```

The manifest pins the upstream commit, patch checksum and Swift dependencies.
The script retains logs, runs the upstream and regression tests, and produces
`bin/imsg` plus its required resource bundles. Keep the executable named `imsg`;
the upstream command parser requires that basename. Signing and installation are
separate operator actions. Retain the existing vendor installation for rollback,
sign the new build using your established operator Keychain identity, and deploy
it to a versioned directory with the resource bundles beside it. Explicitly
configure the consumer to use that executable and verify its actual permissions.
Do not replace the vendor's signature or imply that the vendor signed this patch.

## Contract and limits

- Resolve the supplied exact message UUID and chat GUID from the read-only
  Messages database; reject wrong-chat requests, attachments, nonzero parts,
  empty/oversized text and duplicated message text. The conservative duplicate
  check is intentional: Accessibility does not expose the message GUID.
- Open a fresh exact-message reply overlay and require an exact visible text body
  and one supported custom action. This currently uses English Messages labels.
- Reduce current own reaction state, including replacement and matching removal.
  An already-satisfied request returns success without toggling a reaction.
- Perform the requested action at most once. Verify the requested state on the
  same exact message/chat. Unverified or timed-out effects preserve the existing
  upstream delivery disposition; never retry or fall back after dispatch.
- The GUI session must be logged in, Messages available and the responsible
  process allowed to use Accessibility/Automation and read Messages data. No SIP
  changes or injected framework are required. UI changes and ambiguous targets
  fail conservatively; this is not a guarantee of atomic GUI execution.

The exact-message deep link and standard custom action approach follow the
MIT-licensed [Beeper implementation](https://github.com/beeper/platform-imessage/blob/v0.24.2/src/IMessage/Sources/IMessage/Messages/MessagesController.swift)
and its [deep links](https://github.com/beeper/platform-imessage/blob/v0.24.2/src/IMessage/Sources/IMessage/Messages/MessagesDeepLink.swift).
The underlying [openclaw/imsg source](https://github.com/openclaw/imsg/tree/f2455d94c72c05f105081f8c35319c87b496fc28)
is MIT-licensed. Both upstream license notices are included here. We do not copy
Beeper's outer reaction retries.
