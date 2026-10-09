# Task 22 local invariant and fault suite

This suite produces **LOCAL TEST ONLY** evidence for Task 24. It does not qualify a production candidate or pass external three-AZ, AZ-loss, enrollment, capacity, security-platform or disaster-recovery drills.

## Run

Requirements: Linux (the actor process tests use SIGSTOP/SIGCONT and `/proc`), Java 21, Docker with permission to create/pause/kill disposable Testcontainers containers, Node/npm and an installed Chrome/Chromium. PostgreSQL 17.6 is pulled by Testcontainers. Fault injection targets only containers and child JVMs created by this run.

From the repository root:

```sh
npm ci --prefix signaling-integration-tests/src/test/webrtc
python3 -m venv .superpowers/task22-python
.superpowers/task22-python/bin/pip install -r qualification/requirements.txt
CHROMIUM_PATH=/usr/bin/google-chrome .superpowers/task22-python/bin/python qualification/scenarios/fault_suite.py --integration-only
CHROMIUM_PATH=/usr/bin/google-chrome .superpowers/task22-python/bin/python qualification/scenarios/fault_suite.py
.superpowers/task22-python/bin/python -m unittest discover -s qualification/scenarios/tests
```

Set `CHROMIUM_PATH` to the actual browser executable. The collector runs `./mvnw -B [-pl signaling-integration-tests -am] clean verify`; it never reuses reports from an earlier run. A failure exits nonzero and leaves `maven.log` and a FAILED evidence record for diagnosis. No Docker/browser/process prerequisite is silently skipped.

## Evidence contract

Each run gets a new directory under `.superpowers/sdd/2026-10-03-webrtc-signaling-v1.11-production-implementation/fault-run-*`. `--output` may point outside the repository or to a git-ignored directory within it.

- `run.json`: original command, start/end times, exit code, Java version, git commit, dirty state and complete source fingerprint.
- `fault-matrix.yaml`: exact method-level local mappings and external evidence requirements.
- Original Surefire/Failsafe XML reports and actual fault JSON receipts, copied with SHA-256 hashes in `evidence.json`.
- `maven.log`: original reactor output, also hashed.

The collector rejects changed sources, failed Maven runs, missing methods/receipts, duplicate invocations, malformed/empty/skipped/failed XML and reports or receipts outside the original run interval. Parameterized invocations retain distinct original identities. Local receipts require the expected scenario, `testOnly: true` and nonempty observations.

Actual local faults include bidirectional Artery message blackholing with minority shutdown/rejoin; actor SIGSTOP past the native 15-second root expiry and actor SIGKILL followed by silent-call recovery; synchronous standby pause while COMMIT waits; and primary container kill before standby promotion, signed test permit/storage-epoch admission and original native result reconciliation. Replicas share one Docker host/network; they do not prove cross-AZ durability, capacity or production recovery timing. The cluster uses loopback TCP and Pekko's test transport hook; deployed mTLS/membership gates remain separate.

Partition/rejoin uses six actor members, preserving the native DData majority cap with five survivors. It covers both takeover-before-rejoin and immediate same-address restart without prerequisite replacement acquisition. Read-only same-call reconciliation uses at most 20 queries per stage, at most 2 seconds per ask, and a single 60-second local fault-schedule deadline. UNKNOWN never becomes a success ACK, no mutation is automatically replayed, and native SBR/lease deadlines remain unchanged. Both receipts record query/UNKNOWN counts, UID/epoch changes and stale-pulse/release fences; the 60-second test bound does not qualify production recovery timing.

Generated fixed-seed schedules exercise native lifecycle transitions, duplicates, loss/reconciliation, stale actor identities and version reordering. Each generated lifecycle schedule terminates from every live phase through ESTABLISHED, using production negotiation and matching media observations. Native race tests cover reciprocal invites, 100-way ACCEPT, cancellation and release tombstones. Existing clock/security tests supply deterministic fail-closed local contracts, not measurements of an external authority's availability or freshness.

`LOCAL_AND_EXTERNAL` records can have local PASS while external remains NOT_RUN. `EXTERNAL_THREE_AZ` records never gain local qualification. Every successful artifact keeps `productionQualification: NOT_QUALIFIED`; Task 24 must obtain and validate deployed-candidate evidence independently.
