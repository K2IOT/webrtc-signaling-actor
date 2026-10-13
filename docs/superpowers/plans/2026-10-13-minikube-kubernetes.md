# Minikube Kubernetes Composition Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Reproducible single-node deployment với two-cell isolation, authoritative storage, per-Pod Secrets, secure networking và lifecycle thật.

**Architecture:** Extend existing signaling chart with enabled planes/per-plane resources/Secret refs and local-only overrides. Local composition owns Identity/platform/edge/DNS/observability and four separate CNPG clusters; two regional scope releases connect Directory plus their own Cell DB. Enrollment controller observes trusted Kubernetes Pod/SA identity before releasing per-incarnation Secrets.

**Tech Stack:** Kubernetes1.33.5, Helm3.19.0, CNPG1.27.0, PostgreSQL17.6, Minikube/CNI, Python CLI, existing application images, TLS SNI router, authenticated DoT, OTEL/Prometheus.

**Spec:** [Approved design](../specs/2026-10-13-minikube-signaling-platform-design.md); [master constraints/dependencies](2026-10-13-minikube-signaling-platform.md).

## Global Constraints

All master constraints apply. Baseline requests≈10.5CPU/21Gi before actual infrastructure/sidecars/surge; target≥16vCPU, Minikube≈56Gi, disk≥140Gi; gate on rendered totals/actual placement, not estimate. Actor6/cell/Up4/PDB4/surge1, actual single domain. PG8 instances: Directory/Cell10Gi each, Source5Gi each, observation storage total20Gi; Retain. TLS/mTLS all inter-workload hops; CNPG8000 is server TLS with exact operator-only boundary, not mTLS. No inter-Pod UDP/TCP53 plaintext fallback.

## Review Focus

1. NetworkPolicy objects exist but CNI does not enforce — K1/K4 active negative connection tests.
2. Certificates/Secret refs reused across simultaneous or restarted Pod UIDs — K2 `test_per_incarnation_secret`.
3. DB initialization or failover silently drops synchronous durability — K3 `test_authority_and_standby_gate`.
4. Selector OR broadens cross-cell/issuer access, DNS/operator metrics leaks HTTP — K4/K6 live transport assertions.
5. Down/reset deletes retained data or install resumes with stale enrollment — K5 `test_lifecycle_resume_and_drain`.

---

## File structure

`deploy/minikube/wsa.py` is the lifecycle CLI; `lib/` contains focused preflight, lock, PKI, database, lifecycle and evidence-reference modules. `values/` contains nonsecret plane config. `charts/platform/` contains local Identity/source/edge/DNS/controller templates; existing `deploy/helm/signaling` retains actor/gateway/control ownership. `postgres/` has four distinct cluster manifests and privileged bootstrap Jobs. `observability/` has collector/exporter config without credentials. Tests live at `qualification/scenarios/minikube/tests`; generated files live only under ignored `.superpowers` and Kubernetes Secrets.

### Task K1: Pin/validate prerequisites and capacity

**Files:** Create `deploy/minikube/wsa.py`, `deploy/minikube/lib/__init__.py`, `preflight.py`, `locks.py`, `deploy/minikube/versions.lock.json`, `deploy/minikube/README.md`, `qualification/scenarios/minikube/tests/test_preflight.py`.

**Interfaces:** CLI `python3 deploy/minikube/wsa.py preflight --context NAME --run-id ID --lock deploy/minikube/versions.lock.json`; exit0=PASS,2=BLOCKED,1=FAIL, JSON stdout schemaVersion1 with status/reasons/evidenceRefs. `preflight.check(context: str,lock: dict,rendered_objects: list[dict]) -> dict`; `locks.resolve(output: pathlib.Path) -> dict` records exact tool versions/chart versions/linux-amd64 image digests and official provenance URL, no floating tags.

- [ ] Write `test_cni_requires_active_denial`, `test_capacity_counts_sidecars_surge_and_jobs`, `test_missing_encryption_or_pin_blocks`; test fixture supplies command results, then deployed mode runs real probes:
  ```python
  self.assertEqual("BLOCKED", no_encryption_at_rest["status"])
  self.assertEqual("BLOCKED", policy_object_without_denied_traffic["status"])
  self.assertEqual("BLOCKED", missing_digest_lock["status"])
  self.assertGreater(total_requested_bytes, actor_only_requested_bytes)
  self.assertEqual("PASS", fully_validated_prerequisites["status"])
  ```
- [ ] Run master Python gate with `test_preflight.py`; expected red missing preflight.
- [ ] Implement explicit context check, actual API/kubelet version/driver/architecture, server-side dry-run support, Secret encryption-at-rest verification, resource/ephemeral storage/PVC capacity, required host clock metrics, enforceable CNI deny/allow probe and supported Pod customization for strict DNS/operator paths. Resolve compatible Minikube/CNI/SNI router/OTEL/log/trace/prometheus pins from official sources and pull/record immutable digests. Do not mutate user default context or claim unsupported Minikube flags work.
- [ ] Repeat test gate; expected PASS. `preflight` against real candidate must PASS before installation; missing prerequisite returns BLOCKED with exact reason. Lock is nonsecret and reviewable; version availability is verified at execution, not implied by plan.
- [ ] Scope review then commit `feat(deploy): gate and pin Minikube prerequisites`.

### Task K2: Per-incarnation PKI/Secret/enrollment provisioning

**Files:** Create `deploy/minikube/lib/pki.py`; create `signaling-local-platform/src/main/java/io/webrtc/signaling/localplatform/PodEnrollmentController.java`, `PodEnrollmentAdmission.java`; create `deploy/minikube/charts/platform/templates/enrollment-controller.yaml`, `enrollment-rbac.yaml`, `enrollment-webhook.yaml`, `namespaces.yaml`; test `qualification/scenarios/minikube/tests/test_pki.py`, `signaling-local-platform/src/test/java/io/webrtc/signaling/localplatform/PodEnrollmentControllerTest.java`.

**Interfaces:** `pki.provision(context: str,run_id: str) -> dict` creates CA/signing/admin/account Secrets directly through authenticated Kubernetes API, returns only names/versions/public fingerprints. `PodEnrollmentAdmission.allocateSecretName(String namespace,String serviceAccount,String workload): String` allocates random unique per-Pod-creation mount reference; webhook adds Secret volume, bounded enrollment init container and local markers before Pod persists. `PodEnrollmentController.reconcile(PodReference pod): CompletionStage<Void>`, `registerPod(String projectedToken,PodReference pod): CompletionStage<Void>`; `PodReference(String namespace,String name,UUID uid,String serviceAccount,String enrollmentSecret)` is obtained from authenticated API, never arbitrary request body. Registration endpoint9445 uses verified server TLS plus projected bound SA token authenticated by TokenReview for audience `wsa-enrollment`, with exact live namespace/SA/PodUID binding; bootstrap enrollment precedes workload mTLS issuance. Controller verifies approved workload/config/image policy before creating referenced Secret; no patch to immutable volumes after creation. Secret volume is optional only during init; init waits for valid complete mounted incarnation contract before main container can run, with no default credential. No Secret means Pending/init/unready.

- [ ] Write `test_per_incarnation_secret`, `test_signer_never_mounted_in_workload`, `rejectsUntrustedPodOrServiceAccount`; assertions:
  ```python
  self.assertNotEqual(old_pod_secret_name, replacement_pod_secret_name)
  self.assertNotEqual(c001_actor_private_fingerprint, c002_actor_private_fingerprint)
  self.assertFalse(any_workload_mounts_ca_signer)
  self.assertEqual("DENIED", spoofed_namespace_or_sa_result)
  self.assertEqual("LOCAL_TEST_ONLY", every_generated_secret_environment)
  ```
  Java test asserts one current PodUID/Secret ownerRef, wrong token audience/foreign bound PodUID denial, malformed CSR/SAN denial and stale Secret rejection before runtime starts. Expired or stolen unbound token cannot provision another Pod's key.
- [ ] Run Python gate `test_pki.py` and `./mvnw -B -pl signaling-local-platform -am -Dtest=PodEnrollmentControllerTest test`; expected red missing provisioning/controller.
- [ ] Implement distinct public/internal/cell-c001/cell-c002/DB CAs plus Ed25519 proof keys; RPC URI SAN exactly `spiffe://signaling/local-test-only/cell/<cell>/actor/<workloadId>` or gateway variant, separate enrolled control/source/admin/telemetry policies. Unique leaf/private key per incarnation, exact DNS SAN/CA/EKU/key usage, finite validity. CA signer keys mount only issuer controller; source-signing keys only source signer; cell proof-signing/privacy keys only corresponding authorized owner/worker; other consumers receive public verification keys. Identity JWT private ring only Identity. Generate in memory/ephemeral Jobs, write Secrets via API, redact exceptions/stdout, no files in build context. Files0440/readOnly/nonroot with explicit UID/fsGroup policy; controller RBAC only approved namespaces/Secrets/Pod observations, no application cluster-admin.
- [ ] Repeat gates; expected PASS including admission timeout fail-closed, failed Secret creation leaves init unready, concurrent replicas cannot share key, Pod restart recertification, mount mismatch, unauthorized CSR and expired CA/cert. Controller publishes fresh exact active-verifier inventory to A2 with controller-only mTLS identity, current PodUIDs and version CAS; stopped/stale controller blocks key switch rather than removing unobserved verifier from list. Init has finite100m/64Mi request and limit200m/128Mi, included in scheduler effective Pod resources. Bootstrap controller itself uses preprovisioned restricted certificate/Secret, exempt only from its own admission webhook to avoid circular startup; exemption is an exact SA/workload and tested. TokenReview RBAC belongs only controller, application tokens never receive that permission.
- [ ] Scope review then commit `feat(deploy): provision per-incarnation local PKI secrets`.

### Task K3: Four synchronous DB clusters and authority bootstrap

**Files:** Create `deploy/minikube/postgres/directory.yaml`, `cell-c001.yaml`, `cell-c002.yaml`, `source.yaml`, `bootstrap-jobs.yaml`, `deploy/minikube/lib/database.py`, `qualification/scenarios/minikube/tests/test_database.py`; reuse production `deploy/postgres/cloudnativepg-cluster.yaml`/`deploy/directory/cloudnativepg-directory.yaml` as source contracts, do not overwrite production placement.

**Interfaces:** `database.install(context: str,run_id: str) -> dict`; `database.enroll(context: str,run_id: str,cluster: str) -> dict` records actual system identifier/timeline/writer/standby identities and public WAL evidence, stores typed secret enrollment only after validation. CLI `wsa.py database-enroll --context NAME --run-id ID --cluster directory|c001|c002|source`; promoted writer requires explicit fresh measured enrollment, never automatic reuse.

- [ ] Write `test_authority_and_standby_gate`, `test_bootstrap_is_idempotent_and_scoped`, `test_pvc_retained`; assertions:
  ```python
  self.assertEqual(4, distinct_cluster_count)
  self.assertEqual(8, postgres_instance_count)
  self.assertEqual("ANY 1", validated_sync_ack_rule)
  self.assertEqual(16384, bootstrap_directory_bucket_count)
  self.assertEqual(0, source_or_directory_cell_authority_table_count)
  self.assertEqual("CLOSED", admission_without_exact_standby)
  self.assertEqual("Retain", storage_reclaim_policy)
  ```
- [ ] Run Python gate `test_database.py`; expected red missing manifests/bootstrap assertions.
- [ ] Implement TLS verify-full/client auth, distinct role credentials/Secrets, primary+real sync standby each, exact standby enrollment and explicit WAL durability rule. Local placement uses one real node/domain with preferred/disabled physical anti-affinity only. Provision retained unique volumes: Directory/Cell10Gi per instance, Source5Gi, no shared PGDATA. Controlled privileged migration Jobs verify checksums, seed authority only for genuinely new environment, initialize 16,384 bucket mappings and matching local ACTIVE authorities before publication; app startup does not invent epochs/leases. Source uses V001 source schema, Directory uses directory schema, cells existing cell migrations. Seed test users with actual BucketHasher, not assumed usernames/hash modulo cells.
- [ ] Repeat gate; expected PASS. Deployed smoke verifies synchronous commit receipts/exact replica names/system/timeline, wrong-role cross-DB denial, standby loss closing writes, same-writer restart revalidation and stale-promoted-writer enrollment rejection. Restore/promotion needing unavailable provider evidence is explicitly BLOCKED, not simulated PASS.
- [ ] Scope review then commit `feat(deploy): add separate synchronous local authority databases`.

### Task K4: Secure chart composition, DNS and plane boundaries

**Files:** Modify `deploy/helm/signaling/values.yaml`, `values.schema.json`, `templates/_helpers.tpl`, `actor-deployment.yaml`, `gateway-deployment.yaml`, `control-deployment.yaml`, `services.yaml`, `pdb.yaml`, `networkpolicy.yaml`; create `deploy/minikube/values/c001.yaml`, `c002.yaml`, `control-c001.yaml`, `control-c002.yaml`, `deploy/minikube/charts/platform/Chart.yaml`, `values.yaml`, templates `identity.yaml`, `source.yaml`, `clock-observer.yaml`, `edge.yaml`, `dns.yaml`, `networkpolicy.yaml`, `serviceaccounts.yaml`; test `qualification/scenarios/minikube/tests/test_chart.py`, `test_network.py`.

**Interfaces:** Existing chart values add `planes.actor.enabled`, `planes.gateway.enabled`, `planes.control.enabled`, per-plane resources, Secret references, scoped enrollment/discovery and local marker. `helm template` yields two cell releases actor+gateway and two regional releases control-only; no shared actor release. Platform chart installs Identity/source/edge/DNS/observer. Public SNI router passthrough maps four exact hosts; Bootstrap exposes8443, Identity8443/internalJWKS9443/admin9444, actorsgRPC8443, gatewayreceiver9443, transfer9553, health8559, management8558, Artery25520, DB5432, source9443, DoT853, OTLP4317.

- [ ] Write `test_distinct_cells_and_control_db_scope`, `test_network_selectors_use_namespace_and_pod`, `test_strict_encrypted_dns`, `test_probe_and_resource_contract`; assertions:
  ```python
  self.assertEqual({"wsa-c001", "wsa-c002"}, actor_discovery_namespaces)
  self.assertEqual(6, actor_replicas_per_cell)
  self.assertEqual(4, actor_pdb_min_available)
  self.assertFalse(control_c001_mounts_c002_db_credential)
  self.assertFalse(cross_cell_artery_or_sql_allowed)
  self.assertFalse(inter_pod_plaintext_dns_allowed)
  self.assertEqual("exec", every_application_probe_kind)
  ```
- [ ] Run Python gates `test_chart.py` and `test_network.py`; expected red unsupported chart inputs/no strict boundaries.
- [ ] Implement enabled-plane rendering while default production output preserves semantics. Apply spec resource table per plane, actor heap≤768Mi/direct≤256Mi within2Gi and measured whole RSS gate. startup5s×36, readiness2s/failure1, liveness10s/failure3, timeout2s with A7 verified mTLS exec; no curl-k/TCP-readiness. Restricted nonroot/readOnly/seccomp/dropALL; automountSA=false except minimal discovery/controller projected tokens. PodIP canonical address, namespace-local get/list/watch discovery; no shared remoting CA or actor Service across cells.
- [ ] Implement default-deny ingress/egress and exact allow pairs with namespace+pod selectors in the same peer item. Permit business cross-cell only actor/gateway mTLS gRPC and private control transfer9553, deny cross-cell SQL/Artery/management. Pod-local stub forwards only authenticated DoT853 to authoritative cluster resolver; resolver queries scoped authenticated API and enrolled encrypted external upstream. Configure operator/DB Pod customization on pinned packaging; unsupported encrypted path returns BLOCKED. API bootstrap uses preprovisioned actual API endpoint/CA, no circular DNS fallback. Clock observer host reads are minimal/read-only in explicitly scoped namespace; no unnecessary privileged containers.
- [ ] Repeat Python gates; run `helm lint --kube-version 1.33.5 deploy/helm/signaling -f deploy/helm/signaling/values-production.yaml -f qualification/scenarios/fixtures/deployment-values.yaml -f deploy/minikube/values/c001.yaml` and `helm lint --kube-version 1.33.5 deploy/minikube/charts/platform` with complete nonsecret defaults; run `bash qualification/scenarios/deployment-contract.sh` and `python3 qualification/scenarios/local-deployment-contract.py`. Expected PASS with production render regression. Deployed network fixture verifies both directions, wrong cert/URI/SAN, stolen SA label, actualCNI deny, DNS packet paths and cell-specific WSS host. Chart render alone does not settle this gate.
- [ ] Scope review then commit `feat(deploy): compose isolated secure Minikube planes`.

### Task K5: Ordered lifecycle, resume and graceful shutdown

**Files:** Create `deploy/minikube/lib/lifecycle.py`, `images.py`; modify `deploy/minikube/wsa.py`, `README.md`; test `qualification/scenarios/minikube/tests/test_lifecycle.py`.

**Interfaces:** CLI subcommands `build-images`, `provision`, `up`, `verify`, `fault`, `down`, `reset`, all require explicit `--context NAME --run-id ID`; reset additionally `--confirm-destroy ID`. `lifecycle.up(context: str,run_id: str) -> dict`, `down(context: str,run_id: str) -> dict`, `reset(context: str,run_id: str,confirmation: str) -> dict`; state records nonsecret observed stage/candidate fingerprint/evidence refs, not an authority source. `verify`/`fault` delegate Q1 APIs, no substitute deployment-smoke PASS.

- [ ] Write `test_lifecycle_resume_and_drain`, `test_reset_requires_exact_environment_confirmation`, `test_failed_stage_preserves_evidence`:
  ```python
  self.assertFalse(down_deleted_pvc_or_retirement_log)
  self.assertEqual("DENIED", reset_without_matching_run_id)
  self.assertEqual("BLOCKED", resume_with_changed_candidate_fingerprint)
  self.assertLess(readiness_closed_time, shutdown_started_time)
  self.assertTrue(waited_for_physical_sql_and_rpc_receipts)
  self.assertEqual("FAIL", preserved_failed_install_status)
  ```
- [ ] Run Python gate `test_lifecycle.py`; expected red missing ordered lifecycle.
- [ ] Implement order: preflight/locks → build immutable clean images → namespace/PKI/controller/strictDNS → DB+migrations+WAL enrollment → measured source/security replay → Identity/JWKS → bounded initial-six formation → regions/workers/RPC/gateway boot → regional readiness → SNI edge → observability → Q baseline. Initial source key state is seeded only for a genuinely new environment through explicit admin intent with exact approved ring version/public fingerprints, then reconciled against Identity before issuance; existing source state is never reset to fit a mount. Resume only after authoritative state recheck; never replay stale expected epochs/formation token. Normal joining uses join-only; full cluster reformation requires fresh authorized evidence through source/enrollment, never a permissive Helm boolean.
- [ ] Implement readiness-first graceful drain: gateway grace300s, actor/control90s, stop new work, retain safety/source/WAL observations while existing leases and physical receipts settle, then stop owners/discovery. `down` retains DB PVCs/source tombstones; reset is explicit destructive workflow after reviewable namespace/run inventory. No rollback of durable epochs/retired kids via image rollback. Public CA import/host mapping is documented and verified without globally disabling browser trust.
- [ ] Repeat gate; expected PASS including interruption/resume at every stage, failed image check, actor/system restart with fresh processBoot and cleanup after failed enrollment. Deployed SIGTERM behavior verified Q13; source remains available until dependent drains finish.
- [ ] Scope review then commit `feat(deploy): add ordered Minikube lifecycle commands`.

### Task K6: Authenticated observability and contained operator transports

**Files:** Create `deploy/minikube/observability/collector.yaml`, `prometheus.yaml`, `backends.yaml`, `operator-metrics-exporter.yaml`; modify `deploy/helm/signaling/templates/monitoring.yaml`, local platform network policy/resource values; create `qualification/scenarios/minikube/tests/test_observability.py`.

**Interfaces:** Collector receives actual OTLP gRPC4317mTLS; Prometheus scrapes health metrics8559mTLS and DB9187serverTLS with verified CA/SAN. Operator8080 is consumed only co-located/loopback by exporter, exported inter-Pod via mTLS; raw8000 permits exact operator-only serverTLS, no claim of client auth. Log collector uses node-local file/stdout reads then mTLS transport; backends/storage/network are equally authenticated/encrypted, authenticated dashboard has no public default credential.

- [ ] Write `test_no_plaintext_observability_hop`, `test_metrics_are_finite_and_redacted`, `test_operator_status_is_tls_not_mtls`; assertions:
  ```python
  self.assertTrue(all_interpod_telemetry_hops_authenticated_and_encrypted)
  self.assertFalse(raw_operator_8080_network_scrape_allowed)
  self.assertTrue(cnpg_monitoring_tls_enabled)
  self.assertEqual("serverTLS", cnpg_8000_transport_evidence)
  self.assertFalse(any_user_jti_sdp_ice_token_metric_label_or_log)
  self.assertEqual(20 * 1024**3, total_observability_persistent_bytes)
  ```
- [ ] Run Python gate `test_observability.py`; expected red missing configuration/proven transport.
- [ ] Implement bounded metrics/log/trace retention, Secret trust/client identities, per-backend resource limits and20Gi retained aggregate volumes. Enable CNPG `monitoring.tls.enabled` and verified scraper path; bind/contain operator8080 using supported co-located adapter, otherwise disable raw scrape and record blocked observability subgate until secure collection exists. Resolver/operator/observer/controller sidecars enter actual K1 resource budget, not free overhead. Import existing finite-label dashboard/alerts plus source/JWKS/authority/drain/outbox/exporter saturation signals.
- [ ] Repeat gate; expected PASS; deployed check proves exported A8 spans, SQL/outbox correlation, sanitized logs, finite labels and collector outage does not change authority/business behavior. Missing spans cannot be satisfied by matching trace ID in a log.
- [ ] Scope review then commit `feat(deploy): secure local metrics logs and traces`.
