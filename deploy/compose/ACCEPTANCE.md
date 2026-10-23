# Acceptance run of the two-cell compose environment

**LOCAL_TEST_ONLY / NOT_QUALIFIED.** Outcome: **acceptance FAILED, BLOCKED by a product CPU defect on
this host.** The environment is **not** `LOCAL_QUALIFIED`. Nothing here says anything about
production, HA, fencing, N-1 or any capacity figure.

- Date: 2026-10-22/23 (UTC). Commits run: `6ab9a40` (first run) to `05bf35d` (last runs); the
  automation is `deploy/compose/acceptance.sh`.
- Host: Linux x86_64, 16 cores, 32 GiB RAM (about 24 GiB available), Docker 29.9.2, Compose
  v5.6.0, Java 21, Node 24. The host was shared: other containers and desktop/agent processes kept a
  baseline load average of about 4 (2 to 3 busy cores) before every run.
- Images (`make build`, Maven offline, `SOURCE_COMMIT` label): `signaling-app:local` 623 MB,
  `signaling-identity:local` 445 MB, `signaling-local-platform:local` 513 MB,
  `signaling-kube-api:local` 326 MB, `signaling-provisioner:local` 1.15 GB.

## Commands

```sh
RUN_DOCKER_TESTS=1 bash deploy/compose/tests/run-tests.sh      # stack down: 11 files, 0 failed
bash qualification/scenarios/image-contract.sh signaling-app:local   # PASS
make -C deploy/compose build
bash deploy/compose/acceptance.sh        # all phases; ACCEPTANCE_KEEP_UP=1 keeps the stack
bash deploy/compose/acceptance.sh smoke measure actor standby refusal isolation secrets   # on a running stack
```

`acceptance.sh` prints `PASS|FAIL|INFO <criterion> <check>: <evidence>` lines and writes its logs
under `deploy/compose/.smoke/acceptance/` (git-ignored).

## Results per criterion (spec section 9)

The last full runs: `final-1` (fresh `make down; make up`) and `final-2` (all other phases on a
stack brought up the same way, completed with `docker compose up -d --wait` after `make up`
failed, then left running for about 3 hours before the phases ran).

| # | Result | Evidence |
|---|---|---|
| 1 | **FAIL** (flaky) | 4 fresh `make up` runs with the final fixes: 1 reached all-healthy (339 s), 3 failed at 165-309 s with `dependency failed to start: container ...actor-c00x-y-1 is unhealthy` (an actor that had been healthy flapped). Even after the passing one, `/ready` was 200 on only 10 of 15 planes when probed (503 on the rest); twelve actors were always running. `local-platform` `/ready` 200 every time. |
| 2 | **FAIL** | No smoke run passed (about 20 attempts). Step 1 (login) always passed, step 2 (bootstrap) and step 3 (WSS `AUTH_OK` x2) passed in some runs; failures: step 2 `HTTP 401`; step 3 close `1008 AUTH_FRESHNESS_UNKNOWN`, `1008 AUTHORIZATION_REJECTED`, `1000 Bye`; step 4 close `1008 AUTH_FRESHNESS_UNKNOWN`, `1008 STALE_CONNECTION`, `INVITE: no ACK_COMMITTED within 30000 ms (last: no reply)`. |
| 3a | **FAIL** (smoke part) | `docker stop actor-c001-6`: gateway-c001, control and the other five c001 actors `/ready` 200 (5 s after a 10 s settle); smoke failed at step 4 (`no reply`); `docker start` -> healthy in 113 s, `/ready` 200; the smoke after it failed at step 2 (`HTTP 401`). The cell-readiness part passed, the "a new INVITE still commits" part was never observed. |
| 3b | **PARTIAL** | `docker stop pg-c001-standby`: primary `synchronous_standby_names` = `ANY 1 ("standby-c001")` before and after (no asynchronous fallback); 6 backends waiting on `SyncRep` (commits hang, fail closed); `call_state` rows 1 before and after. The smoke failed at step 3 (`1000 Bye`), before the INVITE, so "the c001 INVITE fails closed" was not isolated from the general overload. Restart: standby healthy in 4 s, `sync_state=quorum` 1 s later; c001 `/ready` did not return to 200 within 120 s and the smoke after it failed. |
| 4 | **PARTIAL** | Missing contract (actor-c001-6's mount without `provider.json`, compose environment): exit 1 in 20 s, `{"...","errorCode":"LOCAL_RUNTIME_REJECTED","role":"contract","category":"MISSING"}`, no stack frames. Empty mount: exit 1 in 12 s, `CONFIGURATION_REJECTED` (the runtime YAML is missing too), followed by a 22-frame Spring stack trace without values. Foreign SPIFFE id (gateway-c001's leaf in actor-c001-6's mount): exit 1 in 23 s, `LOCAL_RUNTIME_REJECTED` `identity` / `IDENTITY_MISMATCH`. Malformed fingerprint: exit 1 in 21 s, `LOCAL_RUNTIME_REJECTED` `identity` / `INVALID`. **Well-formed wrong fingerprint (64 hex, in actor-c001-6's place on the live cell): not refused at startup.** The product validates only the form; the value is a cluster-join property (`FingerprintCompatibilityChecker`). The container never became ready and exited 1 after 73-74 s with the generic `STARTUP_FAILED` (the 30 s membership gate), which does not name the cause. Spec 9.4 "fingerprint lệch" is therefore **not satisfied** as written (refused, but late and without a clear error). INFO: a sibling's leaf (actor-c001-5's in actor-c001-6's mount, no network) is not refused by a startup check; it exited 1 after 56 s with `STARTUP_FAILED` (no network); the per-pod id is the source's check and was not exercised. No secret-looking text in any refusal log. |
| 5 | not run here | Criterion 5 is the SP1/SP2 `./mvnw verify` ITs; this run did not execute them. Spec 8 network isolation probe: **PASS** (below). |
| 6 | **PASS** | No tracked file with a PEM private-key body added since `c6370f8` (16 at HEAD, all pre-existing TEST_ONLY fixtures and the identity key); 0 long password/secret/token literals added under `deploy/compose`; no untracked key, store or password file; 44 secret values of the run (passwords, tokens, key bodies) found 0 times in `docker inspect` env of all 26 containers; 21,389,202 log lines: 0 secret values, 0 JWTs, 0 `admin123`/`mod123`; app, identity and local-platform images labelled `NOT_QUALIFIED`. |

Isolation (spec 8): from `actor-c001-1`, `172.30.1.101:5432` open (probe works);
`172.30.2.11:25520`, `172.30.2.11:8558`, `172.30.2.101:5432`, `172.30.0.41:25520`,
`172.30.2.250:443` refused/timed out; the only container that reaches both cell primaries is
`control`. The host itself reaches cell-network addresses (`172.30.1.11:8559` opened from the
host); isolation is between containers only.

**Passed twice in a row: no** (not once).

## Why: the actors burn CPU at idle

Per-container CPU over 30-60 s windows (cgroup `usage_usec`), stack idle (no calls):

| Setting | Actors (each) | local-platform | PG primaries | pg-directory | Compose total | Host busy |
|---|---|---|---|---|---|---|
| 12 actors, final JVM options | 0.69-0.87 | 0.80 | 0.65-0.73 | 0.48-0.59 | 12.85-12.90 | 15.35-15.58 of 16 |
| one cell only (6 actors) | 1.32-1.41 | 1.45 | 0.94 | 0.67 | - | 14.07 of 16 |
| 12 actors capped `--cpus 0.75` | 0.72-0.78 | 1.00 | 0.65-0.68 | 0.59 | - | 15.03 |
| 12 actors capped `--cpus 0.4` | 0.40-0.42 | 1.33 | 0.45-0.47 | 0.73 | - | 11.30 |

CPU pressure (`/proc/pressure/cpu` some avg10) 70-91 %, load average 50-100. An actor's CPU grows
with the CPU available (0.75 core when 12 share the host, 1.4 with 6), while its logged operation
rates stay the same (about 46 OUTBOX reads, 24 safety and 16 control transactions per second), so
the idle cost is not the periodic work itself. A 40 s JFR profile of one actor (cell c001 only,
1733 execution samples): **1256 samples (72 %)** are under
`NativeActorMaintenance` job -> `CallRecoveryService.sweepTracked` ->
`NativeActorComposition.recoveryGroups` -> `local(group)` for each of the 1024 recovery groups ->
`readiness.safetyReady()` -> `NativeCompatibility.actorReady/permits/peers` (rebuilding the peer
list from cluster membership and Typesafe config paths). The sweep runs every 100 ms. This is
product code (`signaling-cell-rpc`, `signaling-app`); it was not changed here.

Consequence on this 16-core shared host: the stack saturates the CPU, the clock and revocation
source reads miss their bounds (`CLOCK_SOURCE` `OUTCOME_UNKNOWN` on about 15 % of reads,
`REVOCATION` about 10 %), readiness flaps and the gateway closes sockets with
`AUTH_FRESHNESS_UNKNOWN`. CPU caps per actor do not help: at 0.75 the smoke still failed (step 3/4),
at 0.4 INVITE got no reply. An honest pass needs either the product defect fixed or a host with
clearly more free cores; neither threshold nor deadline was lowered to force one.

## Measurements

- RSS with the final `JVM_OPTS` (`-Xms256m -Xmx384m -XX:+ExitOnOutOfMemoryError
  -XX:ActiveProcessorCount=2 -XX:+UseSerialGC`): 17 JVMs 7445-8652 MiB in total, max 517-630 MiB
  (an actor), min 214-217 MiB; `docker stats` total over 24 containers 7672-8884 MiB (the higher
  figures after 3 h of uptime). With default ergonomics (JVM sees 16 CPUs): 685-921 MiB per actor.
- Memory floor: the measured 8.7 GiB plus 25 % is 10.9 GiB; the preflight default stays 12 GiB
  (not lowered: no passing run).
- Time to healthy (`make up`, including preflight and provisioning): 339 s for the one passing
  run; failures at 165, 178 and 309 s (and 269 s with the default JVM options). First-healthy order (seconds from start, passing run):
  PostgreSQL 19-21, kube-api 56-58, local-platform 61, identity 63, actors 158-222, gateway-c001
  272, gateway-c002 296. The twelve actors start within about 15 s of each other (compose starts
  them one by one); all formed their cells.

## Iteration log

1. **Run 1, `6ab9a40`.** Every actor exited `{"component":"PLATFORM","level":"ERROR","errorCode":"STARTUP_FAILED"}` and crash-looped. A verbose diagnostic copy showed `signaling-safety - Connection is not available, request timed out after 526ms (total=0, ...)` -> `Unable to determine Dialect without JDBC metadata`. Cause: Hibernate's dialect detection needs the JVM's first, cold PostgreSQL connection (TLS verify-full + SCRAM) and `DbPools` bounds it by a fixed 500 ms (`minimumIdle 0`). Measured alone on an idle host: 0.40-0.68 s; about 1 s under concurrent start; SCRAM iterations 1 vs 4096 made no difference. Fix `bb54bf6`: `launch.sh` passes `-Dhibernate.dialect=org.hibernate.dialect.PostgreSQLDialect` (the dialect Hibernate detects for PostgreSQL), so a missed metadata read is a warning and the connection being opened joins the pool. No deadline changed; the product defect (a fixed 500 ms bound on the first cold connection during startup) remains.
2. **Same run, after a diagnostic start.** `BootstrapCoordinator - Resolve attempt failed! Cause: javax.net.ssl.SSLHandshakeException: Received fatal alert: protocol_version`. Cause: the Pekko Kubernetes discovery client offers TLS 1.2 only (`tls-version = "TLSv1.2"` default), the fake kube-API required 1.3. Fix `b391884`: kube-API floor TLS 1.2 like a real API server (test RED then GREEN).
3. **Run 2, `b391884`+dialect.** Actors started and formed; `make up` failed at 269 s (`actor-c002-3 is unhealthy`), load 70-100, actors 0.7-0.9 GiB RSS. Fix `05bf35d`: `JVM_OPTS` add `-XX:ActiveProcessorCount=2 -XX:+UseSerialGC`. Run 3: `make up` passed (339 s), RSS total 7.4 GiB; smoke failed (`AUTH_FRESHNESS_UNKNOWN`).
4. **Runs 3-4 and experiments.** CPU saturation identified and profiled (section above); CPU caps tried and rejected; no further compose fix found. `acceptance.sh` corrected along the way (exit status capture, refusal cases that run through `launch.sh`, PEM-body key check against the SP3 base, binary key handling, log dump removal).

## UNVERIFIED LIVE items from earlier tasks

Verified: `make up` end to end (runs; outcome flaky as above); `guard.sh` against real containers
(pass path, fresh volume and full stack); `smoke-ca` against the real volume (three files
exported, directory 0700); `up --wait` with exited-0 one-shots (the passing run returned 0 with
both provisioners `Exited (0)`); restart policy `on-failure:5` (crash-looping actors restarted);
healthchecks and the start order (PostgreSQL -> provision-2 -> kube-api/platform/identity ->
actors -> gateways/control); kube-api on 443 as uid 10001 (served discovery after the TLS fix);
the startup steps Task 3's offline test skipped (actors loaded the rendered provider, process and
runtime files and became ready); `SMOKE_RESOLVE` name resolution (login, bootstrap and WSS via
127.0.0.1 with the right names); host reach to `172.30.0.12:9444` (TCP open from the host); the
12 GiB / `-Xmx384m` / 600 s estimates (measured above).

Still open (step 4 and 5 of the smoke never completed): first INVITE slower than 2 s
(`OUTCOME_UNKNOWN`); ACCEPT's stored code on the cross-cell path; RINGING / CALL_READY /
NEGOTIATION_GRANTED delivery and `callVersion` equalities; OFFER after NEGOTIATION_GRANTED; relay
`requestId`; CALL_TERMINATED to the caller; the `jti` revoke closing 1008 `AUTH_REVOKED` over the
admin listener; the guard's refusal path on a real leftover volume.

## Limitations and other observations

- One host, shared with other workloads; the CPU figures include their contention.
- The empty-mount refusal prints a Spring stack trace (no values) after the fixed line.
- Every PostgreSQL healthcheck (`pg_isready -h <ip>`, user `root`) logs `FATAL: no pg_hba.conf entry ... user "root"` every 5 s; harmless log noise, not changed.
- The product logs every operation at INFO (about 180 lines/s per actor): 21 M lines in 3 h.
- This run establishes **no** `LOCAL_QUALIFIED` status: criteria 1-3 failed, 4 partially.
