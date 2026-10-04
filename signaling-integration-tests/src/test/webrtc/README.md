Run `npm ci` in this directory, install a system Chromium, then run the Maven
`WebRtcInteropIT` test. `CHROMIUM_PATH` selects Chromium (default
`/usr/bin/chromium`). No browser download occurs during the test.

The pinned test-only native endpoint is `@roamhq/wrtc` 0.10.0 and Playwright
is 1.63.0. A real browser and native `RTCPeerConnection` exchange two data
channel echoes across initial negotiation and ICE restart. The Java SDP
relay and ICE receive boundary inject early end markers, duplicate/reordered
candidates, retry while an ICE-agent call is pending, and stale generation
frames. Agent completions advance the receive cursor; socket delivery alone
does not. Logs contain only versions, counts and terminal markers.

This is loopback interoperability with explicit TEST_ONLY authority fixtures.
It does not qualify production mTLS routing, external TURN, the complete
client/device matrix, AZ faults, or capacity. PostgreSQL negotiation grants
are covered separately by `NegotiationGrantIT`.
