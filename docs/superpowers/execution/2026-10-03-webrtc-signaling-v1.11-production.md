# Native inline execution status

Plan: `docs/superpowers/plans/2026-10-03-webrtc-signaling-v1.11-production-implementation.md`.
Spec: full 1,762-line v1.11 document, synchronized from origin at `a3301a8` and read in full. The 665-line plan was read in full. Implementation runs inline on `work` in the dedicated managed cloud checkout.

| Task | Status | Evidence |
| --- | --- | --- |
| 1 | Complete | `98d1bb3`; config tests RED→GREEN; startup rejection/defaults/fingerprint; reactor package succeeds |
| 2 | Complete | `5bf8797`; protocol, config-adapter and internal-envelope RED→GREEN; protobuf/gRPC generation; N/N−1 schema tests |
| 3 | Implemented contract | `44872bf`; strict RS256, bounded verification, policy and revocation interfaces; production identity settings required and unconfigured |
| 4 | Implemented contract | `a3a8990`; bootstrap, bucket hashing, durable-directory interfaces and freeze/install/publication protocol; JDBC adapter follows storage |
| 5 | Complete | `884e19c`; real PostgreSQL 17.6 migrations and safety constraints; two fixed pools, admitted VTs, native authority barriers and JPA-bound transaction tests |
| 6 | Complete | `62df892`; generation/incarnation fences, five-session admission, refresh, old-close protection, irreversible gateway boots and bounded pulse batches |
| 7 | Complete | `5fd5d4f`; reciprocal reservations, Release-before-Reserve tombstone, immutable winner and 100 ACCEPT race |
| 8 | Implemented transaction layer | `c4b474b`; scoped result/call/outbox COMMIT, primary replay and fenced outbox completion; workflow transitions follow actors/RPC |
| 9 | Implemented lease adapter | `5fa4179`; public LeaseProvider, physical cleanup/reconciliation, sequence replay and stale callback gates |
| 10 | Complete local runtime tests | PostgreSQL acquire-before-child, warm/cold routes, exact placement release, coordinator restart, shard relocation, full actor restart, incompatible fingerprint rejection |
| 11–24 | Pending | Business actors, RPC/WSS, deployment and qualification remain unfulfilled |

`./mvnw verify` at Task 2 completion: 43 tests. After the review fix: **51 tests, zero failures/errors/skips**. Empty-module JAR warnings are expected scaffold warnings; startup-failure warnings belong to negative config tests. Tests exercise configuration/protocol contracts, not any unimplemented runtime or production scale.

## Required identity input

Before production deployment and qualification, supply:

- Concrete trusted issuer and signaling audience.
- Maximum accepted JWT lifetime and enforceable issuance-time policy.
- Preserved-jti refresh behavior (the spec baseline) or an approved replacement transition contract.
- `revocationPropagationSLO` and `hardSafetyBound`.
- Durable ordered revocation event source and authoritative high-water/cursor interface.
- Explicit calling policy and its authoritative source: open authenticated users or tenant/contact/block rules.

The full spec still says the concrete revocation bound is obtained from the identity platform. Its restoration did not supply that external contract. Private signing keys are not required by signaling.

## Decisions recorded during execution

1. Retain the native dedicated cloud checkout on `work` instead of creating a nested worktree, matching the user's native execution request. Cost if wrong: weaker git-worktree isolation.
2. Cloud skill packages did not expose their referenced task-start/test-guidance/reviewer-template assets. Native brief extraction, test logs and ledger updates preserve task text and test/commit gates. Cost if wrong: helper bookkeeping differs.
3. Protocol exposes a narrow `ProtocolLimits` contract mapped by app config. Direct protocol→app consumption would create a cycle once app launches protocol consumers. Cost if wrong: one adapter/interface adjustment.
4. Pin a candidate BOM and exact image versions without qualification claims. Public Lease integration, dependency security, image digests and infrastructure still require their planned gates. Cost if wrong: replace dependencies and rerun affected gates.

Native ledger, exact task briefs and test logs are preserved in the ignored `.superpowers/sdd/2026-10-03-webrtc-signaling-v1.11-production-implementation/` directory while execution continues. Resume from the ledger; do not redo completed tasks.

6. Continue implementation using required unconfigured identity settings and explicit test-only fixtures, as authorized by the user; never fabricate production policy. Cost if wrong: deployment remains fail-closed until the real settings are supplied.
7. Isolate directory migrations in `db/directory/migration` to avoid duplicate V001 versions on a shared launcher classpath. Cost if wrong: adjust the configured Flyway location.
8. Select named integration tests through Failsafe `-Dit.test`, because the plan’s `-Dtest` would select Surefire rather than execute the named IT. Cost if wrong: runner invocation differs.

Current verified reactor result after Task 6: **87 tests, zero failures/errors/skips**. PostgreSQL tests use real single-node containers and do not certify cross-AZ WAL durability.

## Fresh-context review and fix

One fresh-context whole-implemented-branch review inspected `a3301a8..5bf8797` using Superpowers requesting-code-review. No Critical findings. One Important finding: the Jackson byte decoder admitted UTF-16 and invalid UTF-8 inside strings. Eight regression cases failed first; strict UTF-8 decoding with REPORT and Unicode scalar validation then made them pass. Final reactor verification passes 51 tests.

One Minor finding remains deferred as the inline skill prescribes: config issuer/audience validation accepts lone UTF-16 surrogates, whose UTF-8 replacement can collide with `?` in the fingerprint. Reject these invalid operator inputs before production. The planned production compatibility and security gates remain blocked; this is not a production-ready release.

5. Later cumulative ICE/order, current authorization/ownership, unknown-COMMIT reconciliation, stale-actor fencing, AZ capacity and revocation freshness were not counted as defects or passed gates in Tasks 1–2. They remain Tasks 3–24. Cost if wrong: overstating foundation readiness; no runtime or production claim is permitted.

## Qualification

Release status: **NOT_QUALIFIED**. There is no deployed candidate image, runtime call path, PostgreSQL HA, browser/native interop, distributed load, AZ-loss, security/DR drill or 24-hour evidence. Tasks 23–24 additionally need the specified infrastructure and measured staged execution; architecture or unit tests cannot substitute for those gates.

Task 10 verification: `./mvnw -B -pl signaling-actors -am clean verify` passes **90 tests** in the selected reactor, including nine PostgreSQL/public Lease tests and two multi-node scenarios. Tests use loopback TCP with explicit test-only seed discovery; production requires cell-scoped Kubernetes API discovery and supplied mTLS contexts. The test-only full reformation has no automatic production reformation counterpart. Expected fault-injection and shutdown warnings are retained; these tests do not certify Kubernetes, PKI, shutdown deadlines, PostgreSQL HA or 10M throughput.

9. Avoid Pekko 1.1.3 Java `stateStoreModeDdata()`, which returns persistence; read the mode from config and assert its runtime name plus coordinator conversion. Cost if wrong: rerun compatibility gates after any BOM update.
10. Match the public `Address.hostPort()` owner format and observe the real shard parent via public Adapter/DeathWatch to gate and release exact placement tenures; the pinned Shard lifecycle does not itself call release on termination. This shares the existing controller and introduces no competing ownership loop. Cost if wrong: rollout remains blocked by lease lifecycle qualification.
