# Contributing

Contributions are welcome through GitHub issues and pull requests.

Before proposing a change:

- Keep changes focused and preserve the package's transport-only scope.
- Do not include private message data, participant identities, database paths,
  credentials, or raw production RPC payloads.
- Document assumptions about the exact imsg version being targeted.
- Avoid changes that install imsg, manage macOS permissions, or weaken system
  protections.

Before submitting:

```sh
go mod tidy
go build ./...
go vet ./...
go test ./...
go test -race ./...
```

Use a patched Go toolchain; CI currently uses Go 1.25.12 and 1.26.5. Tests must remain
self-contained and must not access a live Messages database or send messages.
By contributing, you agree that your contribution is licensed under the MIT
License in this repository.
