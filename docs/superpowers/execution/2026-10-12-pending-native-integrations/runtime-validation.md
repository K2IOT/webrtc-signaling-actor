# Pending native integrations: local runtime validation

2026-10-12, inline execution and review; no independent subagent acceptance.

Fresh `./qualification/scenarios/local/run-pending-native.sh` exited 0. Maven `./mvnw -B clean verify` passed all 12 modules in 18m09s, finished 17:29:37+07:00: **770 tests, 169 XML reports, 0 failures/errors/skips**. Chromium browser tests: 3 PASS; Python qualification regressions: 78 PASS. Whole-reactor Spotless check PASS in 2.646s; browser/script syntax and `git diff --check` PASS.

Evidence: `.superpowers/sdd/2026-10-12-pending-native-integrations/local-20261012T101009-2772386/summary.json`, with native run/evidence/matrix, original logs, copied XML and physical fault receipts. All 182 artifact SHA256 hashes rechecked successfully. Collector status is **LOCAL_SUITE_PASSED**, production qualification **NOT_QUALIFIED**. Source fingerprint `bff007f68a7152affe0a777f11279b4c32105a27dc23806580862ac8a140be20`, dirty checkout at HEAD `841100b7c5ed4da9d0036c3cbffce2f78eb4fa6a`. Source/docs were frozen during the run; this final report and ledger updates were added afterward without code changes. Historical FAILED attempts remain FAILED.

## Fixes covered

- Gateway bootstrap waits for trusted SESSION_S1 facts before consuming its sole BOOT_START; consumed/UNKNOWN boot is never retried.
- Null internal actor trace requests no longer throw; DB boundary admits bounded exact-class unsigned WAL BigInteger values.
- Recovery accounting uses immutable native DTOs and explicit PostgreSQL timestamp types; tests cover deduplication, deadlines, reconstruction, eligibility, wrong owner, isolated cohort loss and key drift.
- Journal-bound disaster abort uses additive V018 with exact operation/epoch/row shape and exclusive native barrier; ordinary writers remain fenced.
- Browser late restart-offer completion is bounded by the original 30s episode; retired PC completions cannot affect a replacement. Real IndexedDB and actual pre-DTLS candidate/end ingestion are tested separately from controller units.
- Positive native fixtures explicitly enroll exact TEST_ONLY cells/boots/capabilities and local mode; mandatory maintenance and physical-owner cleanup are retained. Quota/TTL/production denial were not relaxed.
- Control delivery remains pending after socket write; exact application receipt settles it, with stale-route rejection and no manufactured committed-command ACK.

## Evidence boundaries and remaining work

Local native evidence includes actual PostgreSQL synchronous standby loss, fenced primary promotion, SIGSTOP/KILL actor JVMs and six-member Artery partition/rejoin. Roles are on one host. Actual WSS/mTLS tests and Chromium/native boundary interop do not establish deployed browser/mobile/version compatibility or physical AZ isolation.

Production/P2 remain open: authenticated fleet/provider enrollment, fence/witness/backup restoration evidence, privacy key rotation/retention, complete large fault cohorts, deployed client receipt negotiation, physical three-AZ loss, capacity and 24-hour soak. Recovery accounting admits one complete scope of at most 128 calls; a durable observation retry queue and larger trusted cohort source are not implemented. Missing/queued-lost outcomes remain UNKNOWN and require trusted replay enrollment after process restart. Fresh-install migrations were exercised; a separate populated V017-to-V018 upgrade rehearsal remains unrun.

GitNexus upstream impacts preceded symbol edits. Final all/compare-main scope is broad and CRITICAL due release-branch changes, formatting and preserved pre-existing worktree edits; it is not a claim that new fixtures alone affect all flows. Direct diffs and fresh tests provide scoped evidence. No commit/push/deployment in this continuation.

Reproduce using `qualification/scenarios/local/run-pending-native.sh`; missing Ubuntu24.04 prerequisites can be installed by `qualification/scenarios/local/provision-ubuntu.sh`. The apt provisioner was syntax-checked but not executed because this host already had required tools.
