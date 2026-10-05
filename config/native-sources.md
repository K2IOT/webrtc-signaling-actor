# Native clock source contract

`NativeClockSource` is an adapter for an enrolled clock monitor. Its successful tests
use TEST_ONLY PKI and signing keys; they do not establish infrastructure clock quality.
The process launcher must supply the approved canonical HTTPS endpoint, trusted TLS
context with its client identity, enrolled Ed25519 source keys, cell/storage identity,
actual pod UID and actual process boot UUID. There are no implicit production inputs.

The adapter sends a read-only GET with `X-Signaling-Pod-Uid` and
`X-Signaling-Process-Boot`. The source must authenticate this workload and return
`application/json` with one bounded Content-Length, at most 32768 bytes, and close the
connection. Redirects, chunked/compressed bodies, duplicate headers, extra body bytes,
invalid UTF-8, duplicate JSON keys and unknown DTO properties are rejected.

The JSON object has exactly `report` and `signature`. `report` follows the existing
`ClockSafetyMonitor.Report` record; timestamps are ISO instants. `signature` is the
86-character unpadded base64url Ed25519 signature of
`ClockSafetyMonitor.signingBytes(report)`. Signing the report authenticates the
monitor; the enrolled monitor still has to measure the claimed pair uncertainty,
relative clock rate and continuity. A signature alone is not clock-quality evidence.

The adapter uses one owned TLS1.3 socket per poll, HTTPS hostname validation, no
connection pool and no additional executor. Its fixed SAFETY scheduler job runs at
100ms–1s intervals, with at most one poll in flight and a source I/O budget capped at
one second inside the original job budget. Delayed reports do not acquire a fresh
five-second window. An unavailable or invalid source invalidates cached trust.
Drain stops new polls, invalidates trust immediately and waits for the admitted poll
and its owned socket to finish. An error while closing the TLS socket retains the
physical receipt and admitted slot as UNKNOWN; a failed close never permits a replacement
poll or reports successful drain. Register this owner with the native runtime drain
hooks when installing the concrete process launcher.

Revocation, directory, peer/compatibility sources and complete native launcher binding
remain separate required integrations. This adapter does not stand in for those
sources or for production qualification.
