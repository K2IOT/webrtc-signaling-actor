# Task 1 independent source review

Verdict: **BLOCK** — one HIGH issue in the new observation registration. Source-only review; no tests, builds, source edits or runtime qualification performed.

## HIGH: gateway bootstrap observation runs in the relay body executor

Source: `signaling-cell-rpc/src/main/java/io/webrtc/signaling/rpc/GatewayRelayRpcServer.java:162-169`.

The pre-call catalog and `CompatibilityObservationService` correctly classify this RPC as CONTROL, and the client uses CONTROL channels/callbacks. However, the server body executor selector only recognizes `GatewayControlIngress/`; `CompatibilityObservationIngress/ObserveCompatibility` falls through to `cpu`, the same bounded executor as relay traffic. Relay saturation can therefore queue/reject the bootstrap/safety observation before its CONTROL admission is acquired. At the five-second expiry, fresh boot descriptors disappear and legitimate business capability gates close even while reserved control resources remain available. This breaks the intended CONTROL isolation for the newly added collector.

Required correction: select the server relay executor only for the actual relay method; route the observation method to `controlCpu`, consistently with the accepted pre-call CONTROL catalog. Preserve bounded executors and existing physical ownership.

## Source conclusions

- Observation methods avoid business capability prerequisites. Both servers retain the native TLS identity extraction; gateway ingress requires an actor workload identity. Cell replies bind destination/nonce; gateway replies additionally bind the immutable constructor gateway/boot. Client acceptance waits for terminal completion and rejects duplicate/malformed/stale replies.
- Collector topology bounds are enforced at enrollment (50 cells, 128 gateway boots); actor snapshots require 4–7 distinct members across at least two enrolled zones. Five-second membership/gateway expiry is checked; schema expiry is separately retained. Local gateway observations bind through the native server, avoiding self-bootstrap through business gates.
- Polling retains every original RPC physical completion and does not release the worker slot on logical timeout. Ambiguous dispatch throws preserve an unresolved owner; stale/failed observations remove snapshots. The separate lifecycle owns its own scheduler and its asynchronous stop callback follows physical drain.
- Rollback requires explicit source/target hashes, exact retained schema checksums, exact enrolled process/owner/writer rosters, and writer epochs. Admission UUIDs remain stable after permanent disablement; eligibility checks all captured gates and original drain receipts before reads and before publishing observations.
- Writer inspection uses tracked RECOVERY SQL, actual cell/storage epoch/non-recovery verification and the native exclusive `(100,1)` transaction barrier. Fixed catalog `EXISTS` probes avoid arbitrary table identifiers and full-table count scans. Any nonempty durable catalog or unexpected table yields unknown incompatible-work provenance; schema policy is checked independently. No successful remote fence/provenance/target-deployment evidence is manufactured.
- Roster completeness, actual original permanent drains, separate observation DB ownership, and target-build/schema policy remain trusted enrollment obligations. Source review cannot establish those deployment facts. No critical security finding identified in the reviewed additions.

Reviewed the exact owned files from `task1-report.md`, including new files directly because they may be absent from the index. Existing unrelated accepted changes were preserved.
