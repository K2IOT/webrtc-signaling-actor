# WebRTC signaling actor

Implementation of the v1.11 production-standard target specification. This branch currently contains **Tasks 1–11 of 24**: Java 21 configuration and protocol contracts, RS256 authentication, asynchronous directory/bootstrap contracts, real PostgreSQL authority migrations, bounded virtual-thread JPA/JDBC transactions, and fenced gateway/session registration. Reservation/winner arbitration, durable command results/outbox, PostgreSQL shard leases and multi-node Pekko sharding are verified. UserActor hydration, serialization and idle passivation are verified. CallActor business transitions and WSS integration follow later tasks.

## Build

Use Java 21 and run:

```sh
./mvnw verify
```

The wrapper pins Maven 3.9.11. Configure your normal Maven proxy settings when required by the environment. The managed cloud workspace already supplies `/workspace/.onboarding/activate.sh` for its Java/Maven/proxy setup.

The launcher requires exactly one of `gateway`, `actor`, or `control` as an active Spring profile. It loads the packaged `production-defaults.yaml`. Set `SIGNALING_IDENTITY_ISSUER` and `SIGNALING_IDENTITY_AUDIENCE` from the actual identity contract; empty or unresolved values fail startup. Deployment profiles currently select a plane and validate configuration; their runtime services are later plan tasks.

The compatibility manifest pins an **unqualified candidate**. Dependency integration, security scans, image digests and infrastructure qualification remain release gates.

## Contracts

Public input uses `webrtc-signaling.v1` JSON; internal transport uses generated protobuf/gRPC. Public identity and routing authority come from authenticated server context, and critical repositories must validate them again at the authoritative store. Counter values on public JSON use decimal strings to preserve values above JavaScript's exact-integer limit.

User IDs use case-sensitive NFC UTF-8 with no whitespace trimming or case folding. Opaque issuer/jti values remain exact. Identifiers have UTF-8 byte bounds; oversized values are rejected. Call IDs use `<coordinatorCell>.e<routingEpoch>.<UUID>`; the server generates the random UUID. A call ID provides routing, not authorization.

INVITE scope is `INVITE` at caller home; call-bound scope is `CALL:<callId>` at the original coordinator. Canonical SHA-256 intent hashes include command type, scope, authenticated issuer/jti/user and validated payload/negotiation. Transport retry generation, incarnation, trace and deadline do not change intent. Reservations, call replay and outbox are subsequent implementation tasks.

The frozen previous-release protobuf fixture exercises adjacent internal minor versions 0/1, including preservation of unknown fields. This is a schema contract test, not evidence of a real mixed-fleet rolling upgrade.

## Status

See [execution status](docs/superpowers/execution/2026-10-03-webrtc-signaling-v1.11-production.md), [plan](docs/superpowers/plans/2026-10-03-webrtc-signaling-v1.11-production-implementation.md) and [spec](docs/superpowers/specs/2026-10-03-webrtc-signaling-10m-design-v1.11-production.md).

The identity deployment contract is implemented as required, fail-closed configuration; the identity platform must still supply its concrete values before release. No issuer, audience, JWT lifetime, revocation bound or calling policy has been fabricated. Release status remains **NOT_QUALIFIED**; there is no production or 10M capacity claim.
