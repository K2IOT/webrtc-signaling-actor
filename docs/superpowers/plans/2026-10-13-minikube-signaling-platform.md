# Minikube Signaling Platform Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Triển khai toàn bộ signaling platform thành một candidate Minikube có evidence local, giữ các authority/security/consistency contract production.

**Architecture:** Hai cell `c001`, `c002` chạy hai Pekko cluster và hai Cell DB riêng. Identity, hai regional control scope, Directory DB, source service/DB và observability là các ownership riêng; JWKS và authenticated mTLS RPC nối các boundary. Chart hiện có được mở rộng bằng composition local, không thay native runtime bằng fixture.

**Tech Stack:** Java 21, Maven wrapper, Spring Boot, Netty, gRPC, Pekko Typed, PostgreSQL, CloudNativePG, Helm, Kubernetes/Minikube, Python qualification harness, OpenTelemetry.

**Spec:** [Approved design](../specs/2026-10-13-minikube-signaling-platform-design.md). Đọc cả spec và bốn subsystem plan dưới đây trước execution.

**Planning baseline:** Spec/source assessment started at `main` / `82b00a2`; final document check observed `8220bf2` added concurrently outside this plan. Approved namespace/status edits give spec SHA256 `26182ce56d323426b144fd8c8868bbdcf376a92b539ee8e2534ff9879657d1c4`. Runtime/chart implementation NOT_STARTED; tests/deployment/qualification NOT_RUN. Execution must record current HEAD/dirty fingerprint and preserve unrelated work.

## Global Constraints

- Mọi local input/artifact mang `LOCAL_TEST_ONLY`, mode `local-minikube`, environment `local-test-only`; production parser tiếp tục từ chối local enrollment.
- Namespace: `wsa-edge`, `wsa-identity`, `wsa-regional`, `wsa-c001`, `wsa-c002`, `wsa-platform`, `wsa-observability`; operator ở `cnpg-system`. Không dùng prefix cũ.
- Actor mỗi cell: 6, minimum Up 4, maxSurge 1, maxIngressProducers 7, PDB 4; chỉ một failure domain thật `az-local-node`. Không sửa quorum, fencing, lease hoặc session generation.
- Directory: 16,384 buckets, hash version 1; 1,024 ownership groups/shards. Directory mapping là routing authority; cache chỉ là hint. Call coordinator không đổi sau creation.
- Identity: RS256, RSA ≥2,048 bits, issuer `https://identity.wsa.test`, audience `webrtc-signaling`, TTL/maxTTL 15 minutes, skew 30 seconds, integer `iat`/`exp`, explicit `kid`, fresh `jti`; không thêm refresh API.
- JWKS: periodic refresh 30s, hard freshness 60s, request deadline 2s, body ≤64KiB, ≤64 keys, một request in-flight, unknown-kid refresh tối đa một lần/5s/process. Retirement tombstones phải replay trước readiness.
- Clock/source: window ≤5s, pair uncertainty ≤250,000µs, relative rate error ≤1,000ppm; existing 100ms safety cadence. Emergency retirement SLO 2s, hard bound 5s; không kéo dài TTL để vượt fault test.
- Four PG clusters: Directory, Cell c001, Cell c002, Source; mỗi cluster primary + synchronous standby, `ANY 1` với exact enrolled replica set. `localSingleWriter=false`; không async fallback.
- Pin repository: Kubernetes 1.33.5, CNPG 1.27.0, PG 17.6, Helm 3.19.0. Pin còn lại và linux/amd64 image digests phải resolve/record ở K1; không dùng `latest` hoặc bịa digest.
- Public hosts `identity.wsa.test`, `bootstrap.wsa.test`, `c001.wsa.test`, `c002.wsa.test`; cell URLs chính xác `wss://c001.wsa.test/ws`, `wss://c002.wsa.test/ws`.
- Internal TLS/mTLS, strict encrypted DNS, default-deny NetworkPolicy. Private keys, DB credentials và enrollment chỉ ở Kubernetes Secret; không nằm trong source/JAR/image/evidence. Client trust bootstrap chỉ export public CA certificate.
- Socket write không settle delivery; chỉ exact `EVENT_RECEIVED` settle `APPLICATION_RECEIVED`. Retry giữ request ID, scope và payload hash; late physical completion phải giữ fencing/receipt contract.
- Media P2P hoặc STUN/TURN độc lập; signaling không transport audio/video/datachannel media.
- Initial qualification ≤20 clients, fault observation 60s; security assertions dùng native hard deadlines. Minikube chỉ có thể đạt `LOCAL_QUALIFIED`; production/HA/physical fencing/10M luôn `NOT_QUALIFIED` trong phạm vi này.

## Review Focus

1. JWT epoch 0 có thể bị watermark mặc định 0 coi là revoked: I2/A2 test initial issuance epoch 1 và revoke cùng epoch.
2. Cached JWKS bị giữ sống bởi failed refresh/HTTP304 không hợp lệ: I3 test elapsed 60s, rollback clock và retirement replay.
3. Transfer owner vô tình giữ hai Cell DB credentials: A3/A4 và K4 test local SQL ownership, retry/checkpoint/CAS cùng migration ID.
4. Pod restart giữ chứng thư/enrollment của incarnation cũ: K2/A5/Q3 test PodUID, processBoot, secret version và certificate binding.
5. Manifest nhìn đúng nhưng CNI/DNS/operator còn bypass/plaintext: K1/K4/Q4 test traffic thật, exact identity và evidence, không chỉ lint YAML.

---

## File structure và subsystem plans

| Plan | File ownership / deliverable | Tasks |
| --- | --- | --- |
| [Identity/JWKS](2026-10-13-minikube-identity-jwks.md) | `signaling-identity`, JWKS owner trong `signaling-auth`, isolated auth tests | I1–I4 |
| [Authority/runtime](2026-10-13-minikube-authority-runtime.md) | New `signaling-local-platform` module; scoped transfer trong `signaling-control-api`; adapters/health trong `signaling-app`; trace exporter trong `signaling-observability` | A1–A8 |
| [Kubernetes composition](2026-10-13-minikube-kubernetes.md) | `deploy/minikube`, local overlays và enabled-plane changes trong existing Helm chart; deployment contract tests | K1–K6 |
| [Local qualification](2026-10-13-minikube-local-qualification.md) | `qualification/scenarios/minikube`, deployed clients, faults và evidence collector | Q1–Q4 |

Mỗi subsystem có test gate riêng. Chỉ Q4 xét acceptance của deployed candidate chung. Existing production contracts tại `docs/contracts` và v1.11 design không bị thay thế bởi plan này.

## Dependency order

| Tasks | Prerequisites | Result available to subsequent tasks |
| --- | --- | --- |
| I1 | none | Secret-mounted immutable key ring; no private key in image |
| A1 → A2 | none → A1 | Source HTTP/framing/clock owner → durable security/retirement log |
| I2 | I1, A2 | Source-bound login/securityEpoch và HTTPS Identity |
| I3 | I1 | Bounded enrolled JWKS cache owner |
| I4 | I2, I3, A2 | Publish/observe/switch/retire workflow |
| A3 → A4 | none → A3 | Single-cell transfer store → authenticated remote orchestrator |
| A5 | I3, A1, A2, A4 | Versioned local enrollment grammar/validation |
| K1 | none | Tool/image/cluster prerequisites lock and capacity gate |
| K2 | K1, A5 | Per-incarnation certificates and runtime Secrets |
| K3 | K1, A1, A2 | Four synchronous DB clusters and authoritative bootstrap |
| A6 | A5; K2/K3 for deployed validation | Complete native composition, measured WAL/compatibility providers |
| A7 | A5 | Private mTLS health and exec probe client |
| A8 | A6 | Bounded distributed trace export |
| K4 | K1, K2, K3, A6, A7 | Cell releases, regional scopes, strict DNS/network boundaries |
| K5 | K4, I4 | Reproducible lifecycle with ordered install/drain |
| K6 | K4, A8 | TLS metrics/logs/traces deployment |
| Q1 → Q2 → Q3 → Q4 | K5/K6 → Q1 → Q2 → Q3 | Driver → baseline flow → faults → audited local decision |

Tasks có prerequisite chưa deploy vẫn dùng isolated real TLS/PostgreSQL integration fixtures để test component; fixture không được ship vào runtime image. K5 là nơi hội tụ deployed dependencies.

## Execution rules và checkpoints

- Trước sửa existing function/class/method: GitNexus `impact(target, direction=upstream)`, báo direct callers/processes/risk; cảnh báo HIGH/CRITICAL trước edit. Dùng query/context để navigate; source và tests là evidence cuối.
- Trước mỗi commit: scoped tests PASS, `git diff --check`, GitNexus `detect_changes`, inspect staged diff; nếu graph unavailable ghi rõ, không ghi PASS. Chỉ stage task-owned files, giữ unrelated dirty files. Không tự push.
- Mỗi task kết thúc bằng một scoped commit với message trong subsystem plan; test helper mới thuộc đúng test file của task. Các assertion snippets là specification của test fixture, không phải API production hoặc code để copy nguyên văn.
- Khi thay contract interface, chạy thêm regression của direct consumers đã xác định bằng impact. Full `./mvnw -B clean verify` chỉ bắt buộc sau Java candidate hoàn chỉnh và khi Q3 phát hiện code fix; compile-only không thay test gate.
- K1 phải kiểm tra pinned versions/compatibility qua official docs và lock digest thực lúc execution. Các web links trong spec là provenance thiết kế, không chứng minh candidate hiện đã tương thích.
- Các integration gate dùng `./mvnw -B -pl signaling-integration-tests -am -Dit.test=<Class> verify`; unit gate dùng `-Dtest=<Class> test`. Surefire/Failsafe đã cho phép module dependency không có selected test; vẫn kiểm tra report của target class thật chạy.
- Các Python task dùng `python3 -m unittest discover -s qualification/scenarios/minikube/tests -p 'test_<owner>.py' -v`; tạo test file trước run để đỏ do missing feature, không lấy zero tests làm PASS.

## Completion ledger

Tạo `.superpowers/sdd/2026-10-13-minikube-signaling-platform/progress.md` khi bắt đầu execution. Một row/task có `implementation`, `unit_integration`, `deployed`, `evidence`, `commit`, `remaining`; trạng thái ban đầu tất cả `NOT_STARTED` / `NOT_RUN`. Checkbox chỉ là thao tác, không suy ra deployed PASS.

Checkpoint C1 = I1–I4/A1–A2 Java component PASS, with I1 final image scan explicitly pending until K1/K5; C2 = A3–A8 runtime PASS; C3 = K1–K6 actual installation PASS plus final clean image scans; C4 = Q1–Q4 evidence decision. Nếu prerequisite BLOCKED, giữ các task độc lập có thể hoàn thành và đánh dấu dependent tasks BLOCKED; không giảm security requirement để unlock.

Record spec SHA256, source HEAD + dirty fingerprint, tool/chart/image locks và test reports tại mỗi checkpoint. Evidence directory `.superpowers/sdd/2026-10-13-minikube-signaling-platform/runs/<run-id>` ignored; chỉ summary đã redact được commit nếu user yêu cầu.

## Acceptance traceability

| Spec sections / qualifications | Owning implementation | Deployed verification |
| --- | --- | --- |
| §1–4 topology/ownership/media; §5 invariants; §9 E2E | A3–A6, K3–K5 | Q01/Q02/Q05/Q11/Q18 |
| §6 identity/JWKS/rotation | I1–I4, A2 | Q02/Q03/Q06/Q07/Q08 |
| §7 PKI/enrollment; §8 sources/authority | A1/A2/A5/A6, K2/K3 | Q09/Q10/Q14/Q18 |
| §10 network; §12 probes/shutdown | A7, K4/K5 | Q01/Q12/Q13/Q14 |
| §11 resource/storage; §13 observability/evidence | K1/K3/K6, A8 | Q01/Q10/Q16 |
| §14 acceptance; §15 deliverables/lifecycle | K5, Q1–Q4 | Q01–Q18; optional subcases explicitly separate |
| §5/§16 preserving production contracts | All tasks | Q04/Q05/Q06/Q11/Q15/Q17 plus Q4 audit |

## Plan review record

Inline self-review checks: task coverage, exact paths/signatures/types, dependency DAG, five Review Focus owners, unchanged production semantics, and explicit unrun qualification. This document and its subsystem plans are planning artifacts; no application/Helm deployment implementation or qualification result is claimed.
