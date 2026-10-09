# Minikube CELL release implementation plan

> **For agentic workers:** use superpowers:executing-plans, native inline execution. Complete all tasks continuously; use one fresh whole-branch review at the end.

**Goal:** Release the unchanged CELL architecture to a real single-node Minikube environment and qualify local functionality plus approximately 1,000 clients.

**Spec:** `docs/superpowers/specs/2026-10-09-webrtc-signaling-minikube-cell-release.md`, together with the production v1.11 spec.

**Architecture:** Existing native actor/gateway/control factories, separate cell and directory authorities, generated local enrollment, signed safety sources and actual durable event delivery. Production defaults remain enforced.

**Tech Stack:** Java 21.0.8+9, Maven 3.9.11, Kubernetes 1.33.5, Minikube Docker driver, Calico, PostgreSQL 17.6 / CloudNativePG 1.27.0, Helm 3.19.0.

## Global Constraints

- Root implements inline; no per-task implementer/reviewer agents. Read-only research is permitted.
- One Maven process; no source edits while it runs. Activate `/workspace/.onboarding/activate.sh` and use the pinned Maven repository.
- Generate private inputs only in ignored artifacts/Secrets; safe diagnostics must not expose credentials or keys.
- No fabricated source facts, fake AZ roles, mocked serving dependencies, or changing admission/lease/shutdown budgets to pass local gates.
- Observe RED before each behavior change, then GREEN; trace failures to their original owner.
- Local deployment is explicitly authorized. Preserve original production qualification status and immutable evidence.

## Review Focus

Check accidental production weakening, provider startup/failure cleanup, unbounded caches or callbacks on event loops, replay/retired-key/source freshness behavior, event receipt routing across replaced sessions, physical drains, authority identity checks, local NetworkPolicy paths, and truthful campaign/evidence reporting under shared-host saturation.

### Task 1: Explicit local deployment and readiness contracts

**Files:** `deploy/helm/signaling/{values.schema.json,templates/_helpers.tpl,templates/services.yaml,templates/networkpolicy.yaml}`, `signaling-actors/.../cluster/ClusterReadiness.java`, deployment contract tests and readiness tests.
**Interfaces:** Produces explicit guarded local mode and one real failure-domain minimum; consumed by mounted provider and deploy script. Production callers retain the existing constructor and two-domain minimum.

- [x] Add failing local render/readiness cases, including production rejection of single-node values.
- [x] Run targeted tests. Expected: local acceptance fails while original production cases pass.
- [x] Implement guarded local mode, resource/placement overrides, and precise gateway delivery/control SQL paths.
- [x] Run deployment contract and actor tests. Expected: local and production cases pass.
- [x] Commit and ledger evidence.

### Task 2: Reproducible Minikube infrastructure and private enrollment

**Files:** `deploy/local/`, `runbooks/minikube-cell-release.md`; ignored generated inputs under this plan's workspace.
**Interfaces:** Produces real Kubernetes endpoints, cell/directory authorities, PKI, durable local source log and a typed provider contract for Task 3.

- [ ] Bootstrap pinned Minikube/Calico and verified Helm, preserving configured proxy/CA.
- [ ] Add scripts for namespaces, PostgreSQL, generated PKI/identity, migrations, source service and Secrets.
- [ ] Validate resource budgets, authority identities and source signatures. Expected: actual services ready with LOCAL_TEST_ONLY fingerprints; no credentials in tracked files.
- [ ] Commit reproducible artifacts and ledger evidence.

### Task 3: Mounted native runtime and signed gateway/control cache

**Files:** `signaling-app/.../runtime/`, `.../config/`, auto-configuration imports, integration tests, `config/native-sources.md`.
**Interfaces:** Consumes Task 2 mounts; publishes complete native enrollments before existing factories and owns clocks/feed workers through normal/partial startup drains.

- [ ] Write failing provider parsing/plane composition and signed-cache freshness/replay/security tests.
- [ ] Run targeted tests. Expected: missing provider/cache behavior fails.
- [ ] Implement bounded typed provider, native SQL/TLS/proof/directory inputs and real signed source/cache owners.
- [ ] Run real native integration tests including startup failures and physical cleanup. Expected: all pass.
- [ ] Commit and ledger evidence.

### Task 4: Durable gateway delivery and application receipts

**Files:** gateway ingress/delivery, internal RPC, outbox composition/maintenance, protocol handler tests and real WSS integration tests.
**Interfaces:** Produces actual APPLICATION_RECEIVED acknowledgements for durable outbox events; no write-only acknowledgement substitution. Consumed by call smoke/loadgen.

- [ ] Add failing end-to-end delivery/receipt, wrong-route/replaced-session and duplicate cases.
- [ ] Run tests. Expected: existing missing binding fails.
- [ ] Bind authenticated gateway event RPC, bounded pending receipt registry, EVENT_RECEIVED handling and actual OutboxDispatcher maintenance.
- [ ] Run real transport/storage tests. Expected: durable delivery retires only after matching application receipt.
- [ ] Commit and ledger evidence.

### Task 5: Release, complete smoke/integration, and local 1k campaign

**Files:** `deploy/local/`, `qualification/scenarios/local-1000.yaml`, local smoke/evidence tooling, loadgen fixes only when reproduced.
**Interfaces:** Consumes Tasks 1–4 release and actual identity/directory snapshots; publishes verifiable local evidence without changing original candidate evidence.

- [ ] Build/verify pinned candidate image, load it into Minikube, deploy first full cell, then second within measured budget.
- [ ] Run all spec-amendment smoke/integration cases. Expected: real serving passes; trace/debug regressions inline.
- [ ] Run 1,000-client local campaign with actual fingerprints and measurements. Expected: truthful functional results and explicit generator-limited status if applicable.
- [ ] Capture pod/resource/authority/image state and commit reproducible tooling/evidence summary.

### Task 6: Final verification, review and status

**Files:** execution checkpoint, local runbook, compatibility/status manifests, plan checkboxes and ledger.
**Interfaces:** Consumes original and local evidence; publishes separate local-release and production-qualification status with the user-authorized TODO list.

- [ ] Run full Maven verification and deployment/Python checks once final fixes are in place. Expected: no failures/errors/skips outside explicit qualification TODOs.
- [ ] Obtain one fresh-context whole-branch review; fix Important/Critical findings with RED→GREEN and a green suite.
- [ ] Record exact release identity, local tests, limitations and deferred campaigns. Expected: no unverified completion claims.
- [ ] Commit final status; retain running authorized local release and private enrollment needed to operate it.
