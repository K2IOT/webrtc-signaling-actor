# Acceptance run of the two-cell compose environment

**LOCAL_TEST_ONLY / NOT_QUALIFIED.** Outcome: **acceptance still FAILED, twice in a row with the
same result** (32 PASS, 7 FAIL each), with three actors per cell and the source database pooled.
The stack starts in about 130 s and every plane is ready, but the first INVITE of the smoke fails
in a different way each time (investigation in "Why the first INVITE fails"). The environment is **not**
`LOCAL_QUALIFIED`. Nothing here says anything about production, HA, fencing, N-1 or any capacity
figure. The latest runs are in "Pooled source database, two back-to-back runs" below; the earlier
sections describe the three-actor first run and the six-actor cells.

- Dates: 2026-10-22/23 (UTC). First run (before the fix): commits `6ab9a40` to `05bf35d`. Run after
  the fix: commit `fb249e7` (`fix(rpc): read the recovery safety gate once per sweep`); the
  automation is `deploy/compose/acceptance.sh`.
- Host: Linux x86_64, 16 cores, 32 GiB RAM, Docker 29.9.2, Compose v5.6.0, Java 21, Node 24. The
  host was shared and busy: the desktop session alone (gnome-shell, IDE, agent processes) took
  about 5 to 6 cores, and the baseline load average was 8 to 12 before every run. Another attempt
  was refused by `make up`'s own load guard (load 12 against the limit of 11.2: not skipped), and a
  host reboot killed one run mid-way; both were discarded and the stack was torn down and re-run
  from scratch.
- Images (`make build`, Maven offline, `SOURCE_COMMIT` label): `signaling-app:local` 623 MB,
  `signaling-identity:local` 445 MB, `signaling-local-platform:local` 513 MB,
  `signaling-kube-api:local` 326 MB, `signaling-provisioner:local` 1.15 GB. Only `signaling-app`
  changed for the fix; the other images came from the Docker layer cache.

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

## Why the first INVITE fails (investigation, 2026-10-23)

Method: the three-actor stack was brought up and left alone; `-Xlog:exceptions` was added to
`JVM_OPTS` (temporary, restored afterwards); one INVITE was rebuilt from the JSON operation logs of
every plane by `traceId`, and each `FencedException` / `AuthorizationRejected` was matched to its
throw site. The smoke was then run five more times on the same warm stack.

- The INVITE is **not** a single slow call and it never reaches cell c002. It fails inside c001
  after 240 to 950 ms (gateway `INVITE OUTCOME_UNKNOWN`; one `STALE_CONNECTION` close of the caller's
  socket was seen as well). `RPC_CONTROL` (session proof) takes 55 to 63 ms and commits; the command
  itself then dies on the first attempt.
- Every retry of the smoke's INVITE (same `requestId`, new `callId` minted by the gateway) ends in
  `CallCommandService.authorizedRead` -> `AuthorizationRejected` at +67 ms, by design (see
  `smoke/call-flow.mjs`). So the first attempt decides the step.
- One causal chain was captured end to end: at +237 ms `NativeCellHealthSource` records a failed
  `cell_primary` poll (`DbOverloadedException`, DB admission of the `RECOVERY` class refused it);
  `poll` then sets `cached = null`, the cell is not safety-ready, `NativeActorComposition.local(group)`
  returns empty, and at +280 ms `CallCommandHandler.check` (via `NativeParticipantRenewal.sealGrant`)
  throws `FencedException`. That is the fail-closed gate working as written.
- It is a minority cause. The same refusals (`DB_SAFETY` `BACKPRESSURE`) occur at a steady 1.5 to 3
  per minute per actor with or without an INVITE (counters 63 to 75 per actor after 25 min, +6 to
  +29 over the ten minutes that included the smoke runs). Each costs at most one 500 ms probe period,
  so roughly 5 to 10 % of INVITE attempts, not the 100 % observed.
- The main cause is latency. With the stack idle the host load average is 36 to 40 on 16 cores and
  the stack alone uses 9.5 cores (actors 0.9 to 1.0 each, mostly kernel time; PostgreSQL primaries 1.0
  and 1.45; local-platform 1.4). Single transactions that take a few ms on an idle host take 50 to
  250 ms here, so the ten or so sequential hops of the INVITE saga reach the fixed 2 s budget: warm
  runs saw `INVITE COMMITTED` after 1708 ms, `ACCEPT` after 1524 ms and two `ACCEPT OUTCOME_UNKNOWN`
  at 2001 and 1524 ms. Two of the four warm runs passed INVITE, ACCEPT and NEGOTIATE and then failed
  at OFFER with `RESYNC_REQUIRED`; one failed the callee's AUTH with `AUTHORIZATION_REJECTED`.
- Is the three-actor cell a factor? Only slightly. Fewer JVMs lowered the total (about 10.5 cores
  against 12.9), but each actor now sweeps about 340 recovery groups instead of 170, which keeps the
  recovery sweep lag at 5 to 6 s (spec target 5 s) and loads the shared `RECOVERY` admission class
  (4 slots) that the cell probe uses. The failures are not caused by the cell size.
- No fix was made. The only product candidate (not clearing the cached primary fact when the probe
  was refused before it ran) is safety-sensitive, would remove a minority of failures, and the
  acceptance would still fail on latency. The deadline and the fail-closed gates were not touched.
  The acceptance was not re-run because nothing changed: the two pooled runs above stand.

Open items found on the way: the gateway and control `NullPointerException` in
`NativeCompatibilityObservationProducer.lambda$membershipJob$1`; `security_progress` lock
contention between actors (`could not obtain lock on row`); the idle background transaction rate
(about 175 per second per actor, 5 session statements each) that drives the PostgreSQL and kernel
CPU; and the 5 to 6 s recovery sweep lag.

## Pooled source database, two back-to-back runs (latest)

Commit `8fcbffb` puts a bounded HikariCP pool (7 connections) in front of the local platform's
database. Before it, every revocation page, admin append and readiness probe opened a physical
PostgreSQL connection (TCP, TLS verify-full, SCRAM, a backend fork each time).

| Idle stack, three actors per cell | before the pool | after the pool |
|---|---|---|
| new sessions per second on the `source` database | 33.6 | **0.0** |
| transactions per second on `source` | 134 | 194 (pollers answered faster) |
| `pg-directory` (cores) | 0.87 | **0.11** |
| `local-platform` (cores) | 1.86 | 1.49 |
| whole compose stack (cores, user + system) | 10.79 | 10.48 |

The pool removes the database-side cost entirely but moves the whole stack by only about 3 %: the
estimate of 2.5 cores was too high. What remains in `local-platform` is the source protocol itself.
Sampling its threads shows about half of the busy time in Ed25519 signing of every response, about
18 % in TLS handshakes (P-256, X25519) and about 14 % in a reverse DNS lookup the JDK `HttpsServer`
does for each new connection. Both ends send `Connection: close` (the client `NativeSourceHttp` and
the server's `Responses`) as part of the source contract, so every poll is a new mTLS connection plus
one signature, nine pollers by ten hertz by two endpoints. That contract was not changed.

**Runs `pool-A` and `pool-B`** (2026-10-23 08:24 and 08:47 UTC, one after the other, each from a fresh
`make down; make up`, load guard on and not skipped; the driver waited for a 1-minute load under 9
but did not enforce an idle-CPU level this time). Same 32 PASS / 7 FAIL, the same checks passed and
failed in both:

| # | Result | Evidence |
|---|---|---|
| 1 | **PASS** | `make up` 128 s (A) and 132 s (B); 18 of 18 services healthy, 6 of 6 actors, `/ready` 200 on 9 of 9 planes and on `local-platform`. |
| 2 | **FAIL** | Login, bootstrap and WSS `AUTH_OK` x2 pass; step 4 fails inside the first INVITE: A `STALE_CONNECTION` (socket closed by the gateway), B `no ACK_COMMITTED within 30000 ms (last: ERROR OVERLOADED)`. |
| 3a | **PARTIAL** | PASS: one actor stopped, the cell stays `/ready` 200 (2 of 3), restart healthy, 3 of 3 ready. FAIL: both smokes (INVITE: `TIMEOUT/TERMINAL`, `ERROR OUTCOME_UNKNOWN`, `STALE_CONNECTION`). |
| 3b | **PARTIAL** | PASS: standby stopped, no commit, `synchronous_standby_names` unchanged, quorum back in seconds. FAIL: c001 planes `/ready` not 200 within 120 s after quorum; the smoke after (step 3, `Bye`). |
| 4 | **PARTIAL** | Missing contract, empty mount, foreign SPIFFE, malformed fingerprint refused; the well-formed wrong fingerprint still exits 1 after 23 s with the generic `STARTUP_FAILED` (FAIL). |
| 5 | not run here | Network isolation PASS. |
| 6 | **PASS** | No secrets in git, `docker inspect` env or 3.4 M log lines; images labelled `NOT_QUALIFIED`. |

Resources during the runs: JVM RSS 4474 to 4482 MiB over 11 JVMs, `docker stats` 4720 to 4730 MiB;
the 30 s measurement window after the smoke gave the whole stack 8.98 cores (A) and 9.61 (B), the
six actors 4.65 and 5.04 (largest 0.93 and 0.88), host busy 9.2 and 11.9 of 16.

**Passed twice in a row: no. Passed once: no.** `ERROR OVERLOADED`, `STALE_CONNECTION` and the 2 s
`TIMEOUT` are the same family: the INVITE saga (gateway, cell RPC, `WAKE` of the CallActor,
control route) does not finish within its deadlines even with 5 to 7 cores of the host idle.
Earlier logs of the same step showed `INVITE` 2.0 s, `RPC_CONTROL` 1.9 s and `WAKE` 1.1 s. It is
not cold JIT only (the same failure on a stack that had already served other smokes), and it has
not been run with six actors per cell on a host that can start them, so whether the three-actor
cell is a factor is unknown. This is where the next investigation should go: a trace of one
INVITE, and the fault-suite deadline settings.

## Three actors per cell, first run

Spec change by the user: `ACTORS_PER_CELL=3`, a cell is business-ready on a strict majority (2 of 3).
Production keeps six actors with a quorum of four; the size is declared only through the
LOCAL_TEST_ONLY `SIGNALING_LOCAL_CELL_ACTORS` (commits `dabd6cd`, `a39d081`). 11 JVM services, 20
containers. Review found and fixed one blocker before the run (the management binding still checked
production's six and four, so a 3-actor cell could not start); unit tests 110 + 74 + 334 green, and
`LocalRuntimeCompositionIT` (six actors, the default) 5 of 5.

**Run `three-A`** (2026-10-23 07:04 UTC, driver started it when the host was 98 % idle, load 4; the
load guard was not skipped): 31 PASS, 8 FAIL.

| # | Result | Evidence |
|---|---|---|
| 1 | **PASS** (one check mis-coded) | `make up` exit 0 in **140 s** (302 s with six actors); 18 of 18 services healthy, 2 provisioners exited 0, 20 containers; 6 of 6 actors; `/ready` 200 on 9 of 9 planes and on `local-platform`. The `all-healthy` line reported FAIL only because the script still compared against the old 26/24 counts: fixed afterwards (`N_CONTAINERS`). |
| 2 | **FAIL** | Login, bootstrap (admin -> c001, mod -> c002) and WSS `AUTH_OK` x2 pass for the first time on a full run. Step 4: `INVITE: expected RINGING/RINGING, got TIMEOUT/TERMINAL`. |
| 3a | **PARTIAL** | PASS: `docker stop actor-c001-3`, gateway, control and the two other c001 actors stay `/ready` 200 (**the 2-of-3 rule works**), restart healthy in **34 s** (108 s before), `/ready` 200 on 3 of 3. FAIL: both smokes (the INVITE, above). |
| 3b | **PARTIAL** | PASS: with `pg-c001-standby` stopped `synchronous_standby_names` unchanged, no commit, 16 backends waiting on `SyncRep`; standby healthy in 3 s, `quorum` 1 s later. FAIL: c001 planes `/ready` not 200 within 120 s after quorum; smoke after (step 3, `Bye`). |
| 4 | **PARTIAL** | Same as before: missing contract 10 s, empty mount 7 s (22 frames), foreign SPIFFE 13 s, malformed fingerprint 9 s all refused; **well-formed wrong fingerprint still exits 1 after 31 s with the generic `STARTUP_FAILED`** (FAIL). |
| 5 | not run here | Network isolation PASS (control is the only container reaching both cell primaries). |
| 6 | **PASS** | No key body or secret literal added; 38 secret values of the run found 0 times in `docker inspect` env; 2,233,843 log lines: 0 secrets, 0 JWTs, 0 test passwords; images labelled `NOT_QUALIFIED`. |

A second run (`three-B`) was started by the driver and stopped on purpose: it would have repeated
the INVITE failure and the stack was needed to find its cause. **Passed twice in a row: no. Passed
once: no.**

**Why the INVITE fails.** Gateway-c001's log shows `INVITE` taking 2.0 s (`OUTCOME_UNKNOWN`, the
2 s deadline), `RPC_CONTROL` about 1.9 s and the CallActor `WAKE` 1.1 s. Run on a warmed stack (three
more smokes) it fails the same way, so it is not only cold JIT. It is latency against a fixed
deadline on a saturated host, not a logic error: no deadline was changed. This is the open item the
earlier runs already listed ("first INVITE slower than 2 s").

**Idle cost, six actors in two cells** (stack up and idle, cgroup counters, 15 to 20 s windows):

| | cores |
|---|---|
| compose total | **10.8** (12.6 in the harness window right after the smoke) |
| actors, each | 0.8 to 1.0 (user 0.2, **system 0.55 to 0.71**) |
| local-platform | 1.69 (user 0.94, sys 0.69) |
| PostgreSQL primaries / pg-directory | 1.0 and 0.8 / 0.8 |
| host | 6 % idle, 46 to 48 % system time, 9 to 13 % softirq |

Most of the stack's CPU is kernel time. Two findings worth acting on, neither done here:
`pg-directory` serves about 118 transactions per second for the `source` database (about 63 rows
returned per transaction), which is `local-platform` (the SP2 clock and revocation source) answering
nine pollers; together they cost about 2.5 cores of a test double. And every actor spends about
three times as much CPU in the kernel as in user code (network and timer calls of the 100 ms jobs).
Memory with three-actor cells: JVM RSS 4475 MiB over 11 JVMs (max 505, min 209), `docker stats`
4720 to 5037 MiB over 18 containers (7288 and 7720 MiB with twelve actors). The 12 GiB preflight floor
is left unchanged (no passing run yet).

## Run after the fix (`fb249e7`): 31 PASS, 8 FAIL

One full run, `ACCEPTANCE_KEEP_UP=1 bash deploy/compose/acceptance.sh`, from a fresh
`make down; make up`. `make up` reached all-healthy in 302 s (24 of 24 services, 12 of 12 actors;
before the fix 1 of 4 attempts did). Then:

| # | Result | Evidence |
|---|---|---|
| 1 | **FAIL** | `make up` PASS, all-healthy PASS, twelve actors PASS, but `/ready` 200 on **0 of 15** planes (503 on all), `local-platform` 200. |
| 2 | **FAIL** | smoke: step 1 login passed, step 2 `bootstrap admin: HTTP 401`. |
| 3a | **PARTIAL** | PASS: `docker stop actor-c001-6`, the five other actors, gateway and control `/ready` 200 (86 s after the 10 s settle), restart healthy in 108 s, `/ready` 200 on 3 of 3. FAIL: smoke with one actor down (`AUTH_FRESHNESS_UNKNOWN`), smoke after restart (`HTTP 401`). |
| 3b | **PARTIAL** | PASS: with `pg-c001-standby` stopped the smoke does not commit an INVITE, `synchronous_standby_names` unchanged (`ANY 1 ("standby-c001")`), 2 backends waiting on `SyncRep`, `call_state` rows 0; standby healthy 5 s after start, `sync_state=quorum` 2 s later. FAIL: c001 planes `/ready` 200 not reached within 120 s after quorum; smoke after (`HTTP 401`). The smoke failed at step 3 (before the INVITE), so "the INVITE fails closed" is still not isolated from the general overload. |
| 4 | **PARTIAL** | PASS (exit 1, no stack frames, no secret-looking line): missing contract 23 s (`LOCAL_RUNTIME_REJECTED` `contract` / `MISSING`), foreign SPIFFE id 29 s (`IDENTITY_MISMATCH`), malformed fingerprint 17 s (`INVALID`); empty mount 14 s (`CONFIGURATION_REJECTED`, 22 stack frames without values). **FAIL, unchanged:** a well-formed wrong fingerprint is not refused at startup; the container exits 1 after 67 s with the generic `STARTUP_FAILED`. INFO: a sibling's leaf, 55 s, `STARTUP_FAILED`. |
| 5 | not run here | The SP1/SP2 ITs. Separately, after the fix: `LocalRuntimeCompositionIT` 5 of 5 on the second run (the first run failed 4 of 5 with `actor exited 1` / `STARTUP_FAILED` at host load about 9: a load-sensitive start-up, not reproduced) and unit suites green except the known flaky `CallActorStateMachineTest.restoresDurableTimerWithoutClientTraffic`. Network isolation probe: **PASS** (c001 reaches its own PostgreSQL, not the c002 Artery, management, PostgreSQL or kube-API addresses or the shared Artery address; only `control` reaches both cell primaries). |
| 6 | **PASS** | No private-key body added since `c6370f8` (16 at HEAD, pre-existing TEST_ONLY fixtures), 0 long secret literals added, no stray key files; 44 secret values of the run found 0 times in `docker inspect` env; 3,086,768 log lines: 0 secret values, 0 JWTs, 0 test-account passwords; app, identity and local-platform images labelled `NOT_QUALIFIED`. |

**Passed twice in a row: no** (not once). A second run was not made: the first failed for a
reason (host CPU contention, below) that a repeat on this host does not change.

### Second attempt after the fix, at about 8 free cores: `make up` FAILED

Requested after the 12-free-core precondition could not be met: idle CPU stayed between 37 % and
58 % (about 6 to 9 free cores) for 15 minutes, with the desktop session (mostly `gnome-shell`)
using 7 to 8 cores. One run was made on request, **recorded here as another failed attempt, not
as a pass**. Commit `9a68515` (the same code as `fb249e7`), 2026-10-23 03:56 UTC. The load guard
of `make up` (limit 11.2 for a 1-minute load; it was 12 to 15) was skipped with
`PREFLIGHT_SKIP=load`.

- `make up` exited 2 after 217 s. 14 of 24 long-running services healthy, 5 of 12 actors healthy,
  `/ready` 200 on 4 of 15 planes (and on `local-platform`). `actor-c002-6` exited
  `{"errorCode":"STARTUP_FAILED"}` after its `GET /bootstrap/seed-nodes` request was dropped while
  connecting; the other actors were still restarting under `on-failure:5`; `control` and both
  gateways never started.
- During start-up all twelve JVMs start together: the load average rose from 12 to 99 and idle CPU
  fell to 0 %. The 30 s start-up membership gate counts from process start, so it cannot be met
  at that load.
- `acceptance.sh` stopped after the up phase and tore the stack down (`make down`); no later phase
  ran, so criteria 2 to 4 and 6 were **not evaluated** (not passed). Teardown verified: no
  compose container, volume or network left; `Android-01`, `Android-02` and `minikube` untouched.
- The earlier run (host load about 8 at its start, desktop using 5 to 6 cores) did reach
  all-healthy in 302 s. Together the two runs show start-up and readiness depending on free CPU,
  not on the recovery sweep.

**Passed twice in a row: no. Passed once: no.** The environment is not `LOCAL_QUALIFIED`.

### Root cause of the CPU defect, with numbers

`CallRecoveryService.sweepTracked` calls the `groups` supplier `NativeActorComposition
.recoveryGroups()` every 100 ms. That method called `local(group)` for each of the 1024 recovery
groups outside the host's static share, and `local()` re-reads the whole safety gate
(`readiness.safetyReady()` -> `NativeActorSafetySources.liveSafety()` ->
`NativeCompatibility.actorReady()`: six `permits()` calls, each rebuilding the cluster membership
and re-parsing every peer's roles). Measured in isolation with a real 6-member cluster:
`permits()` 131 µs, `actorReady()` about 0.79 ms, so about 1000 gate reads cost about **0.79 s of
CPU per sweep**, repeating 100 ms after the previous sweep ends: about 0.9 core per idle actor,
which matches the 0.7 to 1.4 cores observed. The cost grows with groups x cluster members.

Fix (`fb249e7`): `RecoveryGroupSelector` reads the gate once per selection (closed gate, self not
Up, or no members still give an empty set) and then only asks whether a lease grant is held, which
still checks the trusted clock and lease expiry. The selection is a scheduling hint; every wake
still re-checks the full gate and group grant at the actor and in the database. `local(int)`
(209 dependents) is unchanged. Cadence, deadlines and compatibility checks are unchanged.

### CPU before and after

Per-container CPU in cores (cgroup `usage_usec`), stack idle:

| Setting | Actors (each) | Compose total | Note |
|---|---|---|---|
| before, 12 actors | 0.69 to 0.87 | 12.85 to 12.90 | host busy 15.4 to 15.6 of 16 |
| before, one cell (6 actors) | **1.32 to 1.41** | not measured | |
| **after, 12 actors, 20 s window** | 0.53 to 0.95 (mean 0.64) | 11.55 | host load 78 to 100 |
| **after, 12 actors, harness 30 s window** | mean 0.51, max 0.63 | **10.03** | host busy 15.37 of 16; PSI some avg10 80 % |
| **after, one cell (c002 side stopped)** | **0.66 to 0.71** | not measured | pg-c001-primary 1.50, local-platform 1.34, pg-directory 0.73 |

The like-for-like figure is the one-cell row: 1.32 to 1.41 before, 0.66 to 0.71 after, about half.
The 12-actor figures are limited by CPU availability, not just by demand.

**The targets (well under 0.3 core per idle actor, compose total under about 8 cores) are not
met.** What remains per actor (a per-thread breakdown of one actor, 15 s): about 0.28 core in the
virtual-thread carrier pool (`ForkJoinPool-N-worker`, the JDBC work of the periodic jobs), 0.11 in
the C2 JIT compiler, and the rest spread thinly (logging, Pekko, gRPC). At the database, per
second on the c002 primary for six actors: `cell_authority` 294 scans, `call_state` 178 (the
recovery scan), `control_outbox` 70, and each `command_result_p*` partition 46 (retention prune);
about 100 transactions per second per actor from the 100 ms safety and maintenance jobs. These are
the design's periodic jobs; lowering their cadence would change freshness and recovery bounds, so
it was **not** done here.

### Why the run still fails

With 12 actors the stack uses about 10 cores and the desktop session another 5 to 6, so the host is
saturated (busy 15.4 of 16, CPU pressure 80 %). Under that load the safety sources answer slowly:
in an actor's log (60 s) `CLOCK_SOURCE` read p50 738 ms, `REVOCATION` read p50 1015 ms, `WAKE`
read p50 550 ms, against 100 ms job periods. Clock and revocation freshness are then unknown,
the gate stays closed (`/ready` 503, `AUTH_FRESHNESS_UNKNOWN`, `HTTP 401`). Stopping the c002 side
(freeing about 5 cores) made the c001 actors report ready within 10 to 30 s (5 to 6 of 6, still
flapping at times), which supports this reading. A pass needs a host with clearly more free cores
(about 12 free, judging by this run); neither threshold nor deadline was lowered to force one.
Before the fix the same positive feedback applied more strongly: a ready gate made the sweep
expensive, which saturated the CPU, which closed the gate.

### JVM sizing

`-XX:ActiveProcessorCount=2 -XX:+UseSerialGC` is kept: the per-thread breakdown shows no GC thread
among the top consumers, and with the host saturated no measurement could tell it apart from the
default. It stays a workaround that was not re-validated on a quiet host. RSS is unchanged (below).

## Earlier run, before the fix (`6ab9a40` to `05bf35d`)

### Results per criterion (spec section 9)

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

### Why: the actors burn CPU at idle (earlier run; the sweep was fixed afterwards in `fb249e7`, see above)

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
product code (`signaling-cell-rpc`, `signaling-app`); it was not changed in that run.

Consequence on this 16-core shared host: the stack saturates the CPU, the clock and revocation
source reads miss their bounds (`CLOCK_SOURCE` `OUTCOME_UNKNOWN` on about 15 % of reads,
`REVOCATION` about 10 %), readiness flaps and the gateway closes sockets with
`AUTH_FRESHNESS_UNKNOWN`. CPU caps per actor do not help: at 0.75 the smoke still failed (step 3/4),
at 0.4 INVITE got no reply. An honest pass needs either the product defect fixed or a host with
clearly more free cores; neither threshold nor deadline was lowered to force one.

### Measurements (earlier run; RSS, memory floor and time to healthy hold after the fix: RSS total 7288 MiB over 17 JVMs, max 497 MiB, `docker stats` 7720 MiB over 24 containers, time to healthy 302 s)

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

### Iteration log

1. **Run 1, `6ab9a40`.** Every actor exited `{"component":"PLATFORM","level":"ERROR","errorCode":"STARTUP_FAILED"}` and crash-looped. A verbose diagnostic copy showed `signaling-safety - Connection is not available, request timed out after 526ms (total=0, ...)` -> `Unable to determine Dialect without JDBC metadata`. Cause: Hibernate's dialect detection needs the JVM's first, cold PostgreSQL connection (TLS verify-full + SCRAM) and `DbPools` bounds it by a fixed 500 ms (`minimumIdle 0`). Measured alone on an idle host: 0.40-0.68 s; about 1 s under concurrent start; SCRAM iterations 1 vs 4096 made no difference. Fix `bb54bf6`: `launch.sh` passes `-Dhibernate.dialect=org.hibernate.dialect.PostgreSQLDialect` (the dialect Hibernate detects for PostgreSQL), so a missed metadata read is a warning and the connection being opened joins the pool. No deadline changed; the product defect (a fixed 500 ms bound on the first cold connection during startup) remains.
2. **Same run, after a diagnostic start.** `BootstrapCoordinator - Resolve attempt failed! Cause: javax.net.ssl.SSLHandshakeException: Received fatal alert: protocol_version`. Cause: the Pekko Kubernetes discovery client offers TLS 1.2 only (`tls-version = "TLSv1.2"` default), the fake kube-API required 1.3. Fix `b391884`: kube-API floor TLS 1.2 like a real API server (test RED then GREEN).
3. **Run 2, `b391884`+dialect.** Actors started and formed; `make up` failed at 269 s (`actor-c002-3 is unhealthy`), load 70-100, actors 0.7-0.9 GiB RSS. Fix `05bf35d`: `JVM_OPTS` add `-XX:ActiveProcessorCount=2 -XX:+UseSerialGC`. Run 3: `make up` passed (339 s), RSS total 7.4 GiB; smoke failed (`AUTH_FRESHNESS_UNKNOWN`).
4. **Runs 3-4 and experiments.** CPU saturation identified and profiled (section above); CPU caps tried and rejected; no further compose fix found. `acceptance.sh` corrected along the way (exit status capture, refusal cases that run through `launch.sh`, PEM-body key check against the SP3 base, binary key handling, log dump removal).
5. **CPU defect fixed, `fb249e7`.** Reproduced and measured first (`permits()` 131 µs, `actorReady()` 0.79 ms with 6 members, about 1000 reads per sweep), then fixed test-first (`RecoveryGroupSelectorTest`: equivalence with the old per-group selection on a table of ten states, the gate read exactly once however many groups are owned, closed gate reads nothing else, gate closing after the read, fail-closed on the next selection). GitNexus `impact`: `recoveryGroups` LOW (1 direct caller), `local` CRITICAL (209 dependents, 56 flows) and therefore not edited. `detect_changes()`: only `recoveryGroups`, `recoveryService`, `NativeActorComposition`. java-reviewer: approve (LOW test findings applied); security-reviewer: no findings.
6. **Run after the fix.** First attempt refused by the load guard (load 12); a second was killed by a host reboot; the third ran to the end: 31 PASS, 8 FAIL (table above). Idle CPU about half of before in a like-for-like one-cell setting, not under the target; host contention keeps the gate closed with 12 actors.
7. **Not done / open.** (a) A second full run: attempted once at about 8 free cores and failed in the up phase (section above); two runs on a host with about 12 free cores are still outstanding. (b) A run on a quiet host (about 12 free cores). (c) Lowering the cadence of the periodic jobs would reduce the idle cost further but changes freshness and recovery bounds: a design decision, not taken. (d) The well-formed wrong fingerprint is still refused only after about 67 s with the generic `STARTUP_FAILED` (spec 9.4). (e) `LocalRuntimeCompositionIT` failed once at load 9 (`actor exited 1`) and passed on the repeat: its start-up is load-sensitive.

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
- Neither run establishes `LOCAL_QUALIFIED`: criteria 1 and 2 failed, 3 and 4 partially. A pass would establish `LOCAL_QUALIFIED` only, and nothing about production, HA, fencing, N-1 or 10M.
