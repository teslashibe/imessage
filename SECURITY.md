# Security policy

## Reporting a vulnerability

Please report suspected vulnerabilities privately through GitHub's security
advisory feature. Do not include message contents, participant identifiers,
authentication material, database paths, or raw RPC payloads in a public issue.

Include the affected version or commit, impact, reproduction steps using
sanitized data, and any suggested mitigation. Maintainers will acknowledge the
report and coordinate disclosure and remediation as appropriate.

## Scope

This package owns the JSON-RPC client and supplied streams. Operators remain
responsible for securing the transport, the imsg process and host, macOS
permissions, credentials, and application access policy. This package does not
install imsg or weaken macOS security protections.
