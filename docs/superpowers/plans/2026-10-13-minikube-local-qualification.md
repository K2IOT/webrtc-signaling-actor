# Minikube Local Qualification Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Chứng minh deployed E2E/security/recovery contracts bằng reproducible evidence; không biến local PASS thành production qualification.

**Architecture:** Python runner điều phối một standalone deployed-client driver dùng protocol/codec hiện có và authenticated read-only SQL/source observations. Fault engine điều khiển đúng workload/operation của run, collector giữ nguyên raw outcomes cùng candidate fingerprint. Acceptance được tính từ assertions và receipts, không từ pod Ready hoặc socket write.

**Tech Stack:** Existing Java21/Netty protocol client, Maven integration-test classpath, Python unittest/subprocess, kubectl/Helm, real HTTPS/WSS/mTLS/PostgreSQL, trace/source evidence.

**Spec:** [Approved design](../specs/2026-10-13-minikube-signaling-platform-design.md); [master constraints/dependencies](2026-10-13-minikube-signaling-platform.md).

## Global Constraints

All master constraints apply. ≤20 initial clients; fault observation60s; security hard5s/JWKS60s/lease native deadlines are independent of observation window. Exact candidate SHA+dirty hash+image digest+config/enrollment versions. Evidence statuses PASS/FAIL/BLOCKED/NOT_RUN/UNKNOWN are preserved. No secret/JWT/raw SDP/ICE/credential in committed evidence; media qualification optional and separate. No automatic production Tasks23–24 completion.

## Review Focus

1. Cross-cell flow passes because both users actually map to same cell — Q2 select via actual BucketHasher/directory/SQL and prove RPC peer identity.
2. Socket write or transport ACK mistaken for application receipt — Q2 withhold EVENT_RECEIVED, Q3 drain/outbox assertions.
3. UNKNOWN retry uses new ID or late result counted twice — Q1/Q3 retain original scope/id/hash and physical receipts.
4. Fault hits wrong pod/candidate or key rotation test shortens TTL — Q3 bind PodUID/processBoot and use real policy elapsed time.
5. Missing/failed subcase, stale source or plaintext path still yields qualification — Q4 evidence validator rejects omissions/conflicting hashes.

---

## File structure

New runner, driver adapter, scenario catalogue, fault engine and evidence validator under `qualification/scenarios/minikube/`. Standalone Java driver and scenario tests under `signaling-integration-tests/src/test/java/io/webrtc/signaling/it/`; they call actual deployed endpoints, never create application fixtures in deployed mode. Existing `ScenarioRunner.java` and `ScenarioRunnerWssTest.java` contain concurrent work outside this plan: do not edit or stage them for this plan.

### Task Q1: Candidate-bound deployed client and evidence runner

**Files:** Create `qualification/scenarios/minikube/runner.py`, `client.py`, `evidence.py`, `scenarios.yaml`, `tests/test_runner.py`; create `signaling-integration-tests/src/test/java/io/webrtc/signaling/it/MinikubeScenarioDriver.java`, `MinikubeDriverTest.java`.

**Interfaces:** `runner.run(context: str,run_id: str,case_ids: list[str]) -> dict`; `client.run_case(case_id: str,scenario: dict) -> dict`; `evidence.record(run_dir: pathlib.Path,case_result: dict) -> None`. Java driver `main(String[] args): void` accepts `--case --scenario-file --result-file`; `runCase(String caseId,Path scenarioFile): CaseResult`; `CaseResult(String caseId,String status,List<AssertionResult> assertions,List<String> evidenceRefs)`; `AssertionResult(String name,String status,String expected,String observed)`. Scenario file references Secret names/endpoint/observed policy versions, never inlines JWT/DB credential; driver obtains narrowly scoped client credentials in memory. Build classpath with Maven test compilation + `dependency:build-classpath -DincludeScope=test`, launch target/test-classes plus exact resolved dependency paths.

- [ ] Write `test_candidate_binding_and_status_preservation`, `test_unknown_keeps_operation_identity`, `test_evidence_redaction`; assertions:
  ```python
  self.assertEqual("UNKNOWN", recorded_unknown_operation["status"])
  self.assertEqual(original_request_id, retry_request_id)
  self.assertEqual(original_payload_hash, retry_payload_hash)
  self.assertEqual("BLOCKED", mismatched_candidate["status"])
  self.assertFalse(serialized_evidence_contains_secret_or_jwt)
  ```
  Java fixture tests bounded TLS handshake/WSS upgrade/response decode, real protocol serialization, explicit request ID and event receipt correlation; no permissive test-only runtime enrollment.
- [ ] Run Python gate `test_runner.py` and `./mvnw -B -pl signaling-integration-tests -am -Dtest=MinikubeDriverTest test`; expected red missing deployed driver/collector behavior.
- [ ] Implement client using existing protocol codecs/message contracts and actual HTTPS login, Bootstrap, WSS upgrade/application auth, request/receipt frames. Keep TLS verification/WSS Origin/subprotocol checks; do not put JWT in URL/log. Define fixed finite client/admission/deadline budgets, maximum20 clients and same-operation UNKNOWN reconciliation. Collect redacted frames plus authoritative SQL commit/fence/outbox receipts and source/trace references; protected observational clients have read-only role, no test backdoor mutation endpoint.
- [ ] Repeat gates; expected PASS. CLI `python3 qualification/scenarios/minikube/runner.py --context NAME --run-id ID --cases Q02` must reject undeployed/mismatched candidate. Deployed pass is deferred Q2; fixture tests do not count as Q02 evidence.
- [ ] Scope review then commit `test(qualification): add candidate-bound Minikube driver`.

### Task Q2: Baseline E2E, routing, idempotency and isolation

**Files:** Create `qualification/scenarios/minikube/baseline.py`, `tests/test_baseline.py`; create `signaling-integration-tests/src/test/java/io/webrtc/signaling/it/MinikubeBaselineCases.java`; modify new `MinikubeScenarioDriver.java` dispatcher and `scenarios.yaml` only.

**Interfaces:** `baseline.run(case_id: str,context: str,run_id: str) -> dict`; Java `MinikubeBaselineCases.run(String caseId,Path scenarioFile): MinikubeScenarioDriver.CaseResult` implements Q01/Q02/Q04/Q05/Q14/Q17/Q18. Obtain two enrolled users whose actual bucket mappings resolve c001/c002; every assertion references exact policy/routing/storage/boot versions.

- [ ] Write `test_cross_cell_flow_requires_rpc_and_commit`, `test_socket_write_is_not_application_receipt`, `test_wrong_cell_is_fenced`; assertions:
  ```python
  self.assertEqual("wss://c001.wsa.test/ws", c001_bootstrap_url)
  self.assertEqual("wss://c002.wsa.test/ws", c002_bootstrap_url)
  self.assertNotEqual(caller_home_cell, callee_home_cell)
  self.assertTrue(authenticated_cross_cell_rpc_seen)
  self.assertTrue(call_and_outbox_committed_before_success)
  self.assertEqual("PENDING", delivery_without_event_received)
  self.assertEqual("APPLICATION_RECEIVED", delivery_after_exact_event_received)
  self.assertEqual(1, duplicate_same_payload_business_commit_count)
  self.assertEqual("REJECTED", wrong_cell_authoritative_mutation)
  ```
- [ ] Run Python gate `test_baseline.py`; expected red missing baseline assertions/dispatcher. Syntax/driver compile gate must also pass before using live driver.
- [ ] Implement cases in coverage table below: full Login→RS256→Bootstrap→Directory→WSS→Gateway→mTLSRPC→Pekko→SQL→cross-cell INVITE/ACCEPT/terminal flow; prove separate clusters/DBs/authenticated peer and immutable coordinator. Wrong-cell/stale route triggers explicit protocol rejection/re-resolve, no WSS redirect/proxy or unverified client body authority. Duplicate same identity/payload returns same durable result; same identity/different payload rejects. Enroll open authenticated-test-user call policy only for approved principals.
- [ ] Implement negative TLS/CNI/SA/peer/DB/discovery/Origin/subprotocol/JWT tests plus Q18 concrete native owner/worker startup evidence. Q17 verifies no media listener/path in signaling and required signaling metadata bounds/permessage-deflate policy; optional real P2P/TURN experiment remains separate. No dummy owner, forged AZ or cache-only SQL proof may satisfy assertions.
- [ ] Run `python3 qualification/scenarios/minikube/runner.py --context NAME --run-id ID --cases Q01,Q02,Q04,Q05,Q14,Q17,Q18`; expected all mandatory assertions PASS with real cluster. Preserve failed runs/raw receipts. Deployed gate includes resource/placement/mTLS/private-key image proof, not only successful call setup.
- [ ] Scope review then commit `test(qualification): prove deployed cross-cell signaling baseline`.

### Task Q3: Security, restart and graceful-drain fault suite

**Files:** Create `qualification/scenarios/minikube/faults.py`, `tests/test_faults.py`; create `signaling-integration-tests/src/test/java/io/webrtc/signaling/it/MinikubeFaultCases.java`; modify new driver dispatcher and scenario catalogue.

**Interfaces:** `faults.inject(context: str,run_id: str,case_id: str,target: dict) -> dict` validates exact namespace/PodUID/processBoot/candidate before mutation; `faults.restore(context: str,run_id: str,receipt: dict) -> None`; `MinikubeFaultCases.run(String caseId,Path scenarioFile): CaseResult`. CLI `wsa.py fault --context NAME --run-id ID --case Qxx`; only enrolled local candidate, no host clock mutation, no destructive reset disguised as fault.

- [ ] Write `test_fault_targets_exact_incarnation`, `test_retry_and_physical_completion`, `test_restore_after_failure`, `test_security_deadlines_are_not_observation_window`; assertions:
  ```python
  self.assertEqual("DENIED", mismatched_pod_uid_fault)
  self.assertEqual(original_request_id, reconciled_unknown_request_id)
  self.assertEqual(1, committed_effect_count_after_restart)
  self.assertTrue(late_old_owner_write_fenced)
  self.assertLessEqual(emergency_security_close_ms, 5000)
  self.assertLessEqual(source_invalid_to_protected_admission_closed_ms, 5000)
  self.assertFalse(ready_with_stale_runtime_enrollment)
  self.assertTrue(fault_restore_receipt_preserved)
  ```
- [ ] Run Python gate `test_faults.py`; expected red missing fault engine/validated target/restore semantics.
- [ ] Implement Q03/Q06/Q07/Q08/Q09/Q10/Q11/Q12/Q13/Q15/Q16 as table below, with bounded same-request reconciliation, actual SQL/application receipts and60s observation. Planned Q08 retirement waits last issuance+900s+30s+measured propagation on real candidate; injected-clock fixture may accelerate unit tests only. Assert initial source stale bound/JWKS60s independently, fresh same-token reconnect versus fresh-jti login, SQL generation increments and stale gateway boot/session rejection. Interrupted/incomplete operation remains UNKNOWN until authoritative reconciliation, never success by timeout.
- [ ] Implement DB restart for all four clusters: same-writer persistent restart must revalidate exact WAL state; standby loss closes durability-required authority; promoted writer stays fenced under stale enrollment. Promotion/restore subcase runs only with fresh local evidence and marked local; missing required evidence records BLOCKED, no hardware-fence claim. Actor restart preserves hydration/ownership/takeover guards; gateway restart renews actual boot authority; SIGTERM stops admission first and waits physical operations/safety workers/outbox drainage.
- [ ] Run `python3 qualification/scenarios/minikube/runner.py --context NAME --run-id ID --cases Q03,Q06,Q07,Q08,Q09,Q10,Q11,Q12,Q13,Q15,Q16`; expected all mandatory cases PASS. Any required assertion FAIL/BLOCKED/NOT_RUN/UNKNOWN prevents local qualification. Fault engine restores network/DB/source/collector condition and preserves prior failed attempts. Re-run relevant Java component tests only when fault reveals/fixes code defects.
- [ ] Scope review then commit `test(qualification): exercise security and recovery faults on Minikube`.

### Task Q4: Evidence audit and explicit local qualification

**Files:** Create `qualification/scenarios/minikube/validate_evidence.py`, `tests/test_evidence.py`, `qualification/minikube-local-qualification.md`; modify new runner/collector and `deploy/minikube/README.md` for evidence command.

**Interfaces:** `validate_evidence.validate(run_dir: pathlib.Path) -> dict` returns schemaVersion1/status/missing/failed/optionalSubcases/candidate/publicFingerprintReferences. CLI `python3 qualification/scenarios/minikube/validate_evidence.py --run-dir PATH`; exit0 only mandatory complete same-candidate evidence; status `LOCAL_QUALIFIED` or `NOT_QUALIFIED`, productionQualification/haQualification/capacity10mQualification always `NOT_QUALIFIED`.

- [ ] Write `test_missing_failed_unknown_or_mixed_candidate_cannot_qualify`, `test_hash_and_redaction_integrity`, `test_optional_subcases_remain_visible`:
  ```python
  self.assertEqual("NOT_QUALIFIED", missing_q18["status"])
  self.assertEqual("NOT_QUALIFIED", unknown_required_assertion["status"])
  self.assertEqual("NOT_QUALIFIED", mixed_image_digest_runs["status"])
  self.assertEqual("NOT_QUALIFIED", plaintext_internal_path["status"])
  self.assertEqual("LOCAL_QUALIFIED", all_required_same_candidate_pass["status"])
  self.assertEqual("NOT_RUN", optional_real_media_status)
  self.assertEqual("NOT_QUALIFIED", local_pass["productionQualification"])
  ```
- [ ] Run Python gate `test_evidence.py`; expected red missing validator.
- [ ] Implement manifest SHA256 validation, exact source SHA/dirty fingerprint/spec hash/tool/CNI/Helm/image locks/config and enrollment versions/public fingerprints, actual Pod placement/resource observations, timestamped sanitized SQL/fence/source/trace receipts and all required assertions. Evidence contains no secret values; retirement tombstones/private key Secret contents remain in cluster, evidence only public version/commit references. Reject stale/mismatched results, source unavailability hidden by empty batches, undocumented transport exceptions and zero-test reports. Keep optional outcomes separate, never relabel them PASS.
- [ ] Run all new Python tests, required existing deployment-contract regressions and `git diff --check`; run validator on real Q2/Q3 candidate. Expected0/LOCAL_QUALIFIED only if every mandatory Q01–Q18 assertion PASS. Update execution ledger with actual commits/reports/deployed outcome and each pending/optional subcase; do not change production v1.11 Tasks23–24 to complete.
- [ ] Scope review then commit `docs(qualification): audit Minikube local evidence and limits`. Final handoff includes exact candidate/evidence directory/actual run result/remaining unverified gates, with no production or capacity claim.

## Required scenario catalogue

| Case | Required deployed assertions | Owner |
| --- | --- | --- |
| Q01 | Context/pins/CNI deny proof;8PG/12actors/separate namespaces; probes/limits/PVC/Secret mounts/TLS paths/capacity actual | Q2 |
| Q02 | Login/JWT/Bootstrap exact URLs/two-home routing/WSS/cross-cell mTLS/actors/SQL/outbox/terminal application receipt | Q2 |
| Q03 | Same-token reconnect keeps session key, generation increases; fresh login new jti; duplicates do not create new effects | Q3 |
| Q04 | Duplicate request result equality; conflicting hash reject; UNKNOWN same identity and physical-completion reconciliation | Q2/Q3 |
| Q05 | Wrong-cell/stale-directory hint cannot authorize mutation; exact re-resolve; bucket freeze/fence consistency | Q2 |
| Q06 | Old session/generation/gateway boot cannot deliver or mutate after replacement/revoke | Q3 |
| Q07 | Periodic refresh, unknown-kid throttle, malformed/oversized JWKS, outage cache aging,60s stale closure, source retirement precedence | Q3 |
| Q08 | Publish/observe-all/switch/planned retirement; emergency≤5s; restart tombstones; retired kid/material cannot return | Q3 |
| Q09 | Unsafe/discontinuous/stale clock; unavailable/replayed/skipped revocation source; new protected work closed within contract bounds | Q3 |
| Q10 | Each DB restart; persistent data; exact writer/system/timeline/replica revalidation; standby loss no async fallback | Q3 |
| Q11 | Actor JVM restart/rehydration/lease takeover; old incarnation/late commit fenced; no shared actor cluster | Q3 |
| Q12 | Gateway restart/boot renewal/reattach; persisted sessions/outbox/generation behavior | Q3 |
| Q13 | SIGTERM readiness-first; gateway300s/actor-control90s; retained safety observations/physical receipt drain | Q3 |
| Q14 | Wrong CA/SAN/role/revoked peer; blocked cross-cell Artery/management/SQL; scoped SA/Secret access; no DNS plaintext fallback | Q2 |
| Q15 | Outbox retry/recovery/maintenance workers; pending until application receipt; exactly-once durable effect under duplicate/late completion | Q3 |
| Q16 | Real metrics/logs/parent-linked exported spans; no secret/high-cardinality payload; bounded collector outage | Q3 |
| Q17 | WSS Origin/subprotocol/JWT/limits/permessage-deflate policy; signaling/media separation | Q2 |
| Q18 | Required native owners/regions/workers; missing/bad enrollment closed; initial-six vs join-only/reformation; production rejects local | Q2 |

Optional, separately reported: Q10 promoted-writer fresh local enrollment/backup restore, Q17 real P2P/STUN/TURN media. `LOCAL_QUALIFIED` excludes these optional capabilities when NOT_RUN/BLOCKED; it must state that exclusion. Mandatory fail-closed response to stale promoted-writer enrollment and absence of signaling media transport are never optional.
