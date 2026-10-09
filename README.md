# WebRTC signaling actor

Implementation of the v1.11 production-standard target specification. Tasks 1–21 have local contract coverage; **Tasks 22–24 remain in progress**. The branch includes fenced PostgreSQL/Pekko call and session state, scoped command results, native cross-cell proof/activation composition, bounded WSS and RPC transports, physical completion tracking, relay/recovery primitives, maintenance workers, telemetry and deployment/runbook contracts.

Native qualification work now covers signed clock reports over actual mTLS source I/O, winner-only snapshot authorization, portable proof expiry, original-ID RPC retries and native shutdown. The distributed worker uses real TLS1.3/RS256, seeded disjoint call ranges, bounded live ICE traces, current-socket reconnect sequencing, raw HDR histograms and original workload/resource time series. Native 5x destination/bucket selection and scheduled malformed/oversized wire probes now exist, with separate original-source counters. Complete native launcher/source installation, the four remaining security schedulers and genuine staged production runs remain open.

## Build

Use Java 21 and run:

```sh
./mvnw verify
```

The wrapper pins Maven 3.9.11. Configure your normal Maven proxy settings when required by the environment. The managed cloud workspace already supplies `/workspace/.onboarding/activate.sh` for its Java/Maven/proxy setup.

The launcher requires exactly one of `gateway`, `actor`, or `control` as an active Spring profile. It loads the packaged `production-defaults.yaml`. Set `SIGNALING_IDENTITY_ISSUER` and `SIGNALING_IDENTITY_AUDIENCE` from the actual identity contract; empty or unresolved values fail startup. When `SIGNALING_RUNTIME_CONTRACT` is set, Boot reads that absolute YAML file before binding the typed configuration. It supports the existing `signaling` configuration sections, overrides packaged defaults, and preserves deployment environment/CLI precedence. Missing or invalid files reject startup; see [runtime configuration format](config/native-sources.md#mounted-runtime-configuration). Deployment profiles currently select a plane and validate configuration. Complete executable runtime composition remains an open Task 22 dependency. See [native source contract](config/native-sources.md) for the enrolled clock adapter; it does not configure production identity or clock measurements.

The compatibility manifest pins an **unqualified candidate**. Dependency integration, security scans, image digests and infrastructure qualification remain release gates.

## Contracts

Public WSS handshakes require and select the `webrtc-signaling.v1` subprotocol; input uses strict JSON; internal transport uses generated protobuf/gRPC. Public identity and routing authority come from authenticated server context, and critical repositories must validate them again at the authoritative store. Counter values on public JSON use decimal strings to preserve values above JavaScript's exact-integer limit.

User IDs use case-sensitive NFC UTF-8 with no whitespace trimming or case folding. Opaque issuer/jti values remain exact. Identifiers have UTF-8 byte bounds; oversized values are rejected. Call IDs use `<coordinatorCell>.e<routingEpoch>.<UUID>`; the server generates the random UUID. A call ID provides routing, not authorization.

INVITE scope is `INVITE` at caller home; call-bound scope is `CALL:<callId>` at the original coordinator. Canonical SHA-256 intent hashes include command type, scope, authenticated issuer/jti/user and validated payload/negotiation. Transport retry generation, incarnation, trace and deadline do not change intent. Reservations, scoped call replay and native outbox are implemented; bounded delivery/reconciliation workers are covered locally, with production scheduler/source installation still required.

The frozen previous-release protobuf fixture exercises adjacent internal minor versions 0/1, including preservation of unknown fields. This is a schema contract test, not evidence of a real mixed-fleet rolling upgrade.

## Status

See [execution status](docs/superpowers/execution/2026-10-03-webrtc-signaling-v1.11-production.md), [plan](docs/superpowers/plans/2026-10-03-webrtc-signaling-v1.11-production-implementation.md) and [spec](docs/superpowers/specs/2026-10-03-webrtc-signaling-10m-design-v1.11-production.md).

The identity deployment contract is implemented as required, fail-closed configuration; the identity platform must still supply its concrete values before release. No issuer, audience, JWT lifetime, revocation bound or calling policy has been fabricated. Release status remains **NOT_QUALIFIED**; there is no production or 10M capacity claim.

## Task 22 local correctness and faults

The [local invariant/fault suite](qualification/scenarios/README.md) binds method-level test results and actual actor JVM, Artery partition/rejoin and synchronous PostgreSQL fault receipts to the original source and run. It emits LOCAL TEST ONLY evidence; external three-AZ, production identity/source enrollment, staged capacity and deployed-candidate release gates remain NOT_QUALIFIED under Tasks 23–24.
