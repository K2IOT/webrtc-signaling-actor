# Pending Native Integrations Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Execution update (2026-10-12):** User explicitly selected inline execution without subagents and subsequently authorized tests/faults, including Docker provisioning on Ubuntu24.04. This supersedes the initial delegation/no-test restrictions below. Current implementation and runtime status are recorded in the execution ledger; historical checkboxes are not acceptance evidence. Production/P2 remains NOT_QUALIFIED.

**Goal:** Replace remaining repository integration placeholders with bounded native producers, collectors and client behavior, while retaining unavailable deployment inputs as explicit dependencies.

**Architecture:** Read-only mTLS observation endpoints bootstrap independently of business capability gates. Native SQL observation and durable recovery accounting retain original physical ownership; browser persistence and media recovery preserve exact intent/session/round identity. New initial-ICE receipts are additive and capability gated; provider receipt meanings are never invented.

**Tech Stack:** Existing pinned Java21/Spring/Pekko/gRPC/PostgreSQL stack; browser ES modules and IndexedDB.

**Spec:** `../specs/2026-10-03-webrtc-signaling-10m-design-v1.11-production.md`, especially §§40/46/47/49; prior remediation ledger `../execution/2026-10-09-audit-remediation.md`.

## Global Constraints

- User explicitly requested implementation of the pending scope on2026-10-12; preserve accepted uncommitted work on dev-release-minikube.
- GitNexus upstream impact precedes every existing symbol edit; report HIGH/CRITICAL before mutation. Unknown index coverage requires direct source callers.
- No tests added/run under the higher session restriction; root performs production compilation, static syntax checks and independent source review. No deployment/load/production qualification inferred.
- Authority,15s/30s participation TTL, original UNKNOWN owners, mTLS identity, fixed capacity and privacy remain binding. New observers never authorize business mutations or manufacture physical cleanup.
- Missing real issuer/provider/credentials/client-repository inputs remain required. Never invent cloud operations, fence/witness protocols, acknowledged-WAL manifests or successful evidence.
- No merge/push/deploy or edits in external client repositories without concrete authorized target.

## Review Focus

1. Bootstrap closed capability gates without exposing unauthenticated descriptors; bind cell/workload/current boot and freshness.
2. Unknown/stale WAL, unsafe timeline inference and incomplete replica observations must remain unknown or deny safety.
3. Cross-tab persistence, original issuer/jti/user and immutable request identity cannot be replaced during retry/account changes.
4. Fault cohort denominators, dedupe and outcomes must survive failure; wake/snapshot/transport ACK alone cannot imply active/media preservation.
5. New initial-ICE receipts must bind current call/round/generation/recipient, preserve ordering and avoid claiming gateway writes are peer ingestion.

## Task1: Actual compatibility observation and rollback collectors

**Files:** internal proto; CellRpcClient/Server and GatewayRelayRpcClient/Server; new native observation/collector/configuration classes; NativeCompatibilityEnrollment and lifecycle configuration.

**Interfaces:** Consumes actual authenticated membership/build/schema/gateway boot and tracked RPC/DB owners. Produces bounded scheduled maps consumed by NativeCompatibilityEnrollment plus conservative writer/physical/schema rollback facts.

- [ ] Add bounded read-only observation service, mTLS authorization, destination/current-boot bindings, no business capability prerequisite.
- [ ] Install native producer and physically tracked polling in actor/gateway/control lifecycle. Bound≤50cells,≤7actors/cell,≤128boots, freshness≤5s; missing/partial facts deny.
- [ ] Implement rollback collection behind an explicit admission-fence generation, original-owner barriers and actual writer schema/work inspection. Require complete compatible target policy; absence or unsupported provenance stays unknown. Do not equate busy counter zero with fleet settlement.
- [ ] Document executable/topology/rollback enrollment and independent source review; root compile.

## Task2: Actual WAL observations and production contract loading

**Files:** new storage PostgresWalFacts; native WAL enrollment/source/configuration; PrimaryCellFacts/safety wiring; separate production contract loader; provider renderer/template/docs.

**Interfaces:** Consumes explicitly enrolled admitted writer/standby endpoints and strict production references; produces immutable epoch/role/system/timeline/flush/replay/freshness facts and actual WAL readiness, separately from primary-valid.

- [ ] Collect real bounded SQL facts with tracked original physical completion. Unsupported privileges/identity/timeline/freshness remain unknown; checkpoint identity is not silently a current promotion proof.
- [ ] Install owned scheduling/drain and actual WAL metric producers; distinguish byte lag, source freshness and available replication duration observations.
- [ ] Implement strict separate production loader without local fallback, provider-specific render-only validation and unresolved-input rejection. No invented Secret values or provider protocol.
- [ ] Bind readiness to real configured WAL policy; document actual fence/witness/acknowledged-WAL/provider deployment inputs and source review; root compile.

## Task3: Browser durable commands and media recovery

**Files:** clients/native-browser/native-client.mjs; new durable-command-journal.mjs and media-recovery-controller.mjs; package/README.

**Interfaces:** Preserve synchronous command API; add explicit async persist-before-send API and scoped MEDIA_* observations. Exact original issuer/jti/user/domain binding is trusted enrollment; no JWT/SDP/ICE persistence.

- [ ] Implement transactional bounded IndexedDB unresolved intent journal across tabs; exact immutable envelope before send; settle only definitive authoritative original results. Never evict unresolved commands or reinterpret absent results.
- [ ] Add explicit scoped media operations using only existing validator fields and real PC events; disconnect debounce5s/recovery30s, one original restart UUID per admitted episode, authoritative grant/round before restart/offer.
- [ ] Preserve physical Promise timeout fencing, healthy PC reconnect and account/domain isolation; document actual application offer/answer/UI boundary.
- [ ] Independent JavaScript review; root syntax check, no browser acceptance claims.

## Task4: Durable trusted recovery cohort and outcome producers

**Files:** new observability/storage cohort accounting and additive migrationV017 reserved; native enrollment/composition hooks; fixed-label metrics and docs.

**Interfaces:** Trusted native fault/cohort enrollment supplies complete authority/fault start/eligibility; committed/read-authorized outcomes supply independent convergence/preservation/reattachment/media facts. No arbitrary public request may enroll a cohort.

- [ ] Implement bounded durable cohort membership/dedup/deadline accounting; raw personal/network/payload data excluded. Missing complete cohort/enrollment stays unknown.
- [ ] Wire real guarded workflow outcomes, full authorized route restoration/application receipts and valid media round observations through bounded asynchronous native hooks. Do not block authority or relabel scan hydration as denominator.
- [ ] Emit four separate eligible/successful fixed-label SLIs once; process failure cannot silently lose admitted denominator or count retries twice. Require trusted privacy key/provider facts where needed.
- [ ] Document actual provider/cohort enrollment and unrun fault/media gates; independent review; root compile.

## Task5: Native initial-ICE application receipts

**Files:** protocol operations/validator/capability; internal proto and actual relay producer/ingress; gateway native command/receipt paths; browser receive/resend and README; compatibility feature declaration.

**Interfaces:** Add an explicitly named bounded version1 receipt carrying current generation sender incarnation, contiguous cursor, bounded missing ranges and end receipt. It acknowledges ordered peer ICE ingestion, never queue/write completion. Producers require negotiated server/gateway/client support; old paths omit the feature.

- [ ] Write exact additive wire contract before code; enforce native call/round/generation/participant/authenticated-session and current recipient bindings.
- [ ] Implement bounded protected receipt forwarding over native WSS/RPC before DTLS with negotiated capability gates, original receipt/retry identity, finite cache/gap deadline and no SDP/ICE persistence.
- [ ] Connect actual browser ordered ICE completion to receipt emission and original-envelope retransmission, preserving old peer fallback/RESYNC and ambiguous PC fencing.
- [ ] Independent protocol/security/JavaScript review; root compile/syntax; native interoperability remains unrun.

## Task6: Combined review and pending deployment evidence

- [ ] Compile production reactor with `-Dmaven.test.skip=true`, static browser checks and direct-source scope review including new files; fix concrete findings.
- [ ] Record implemented versus input-blocked versus unrun runtime/performance gates in the new execution ledger. Preserve historical qualification artifacts.
- [ ] Request only still-missing concrete identity/provider/client/environment inputs; never mark actual deployment,3AZ/P2/soak/security/DR or tests PASS without execution.
