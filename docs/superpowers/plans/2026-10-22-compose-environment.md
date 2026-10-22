# Compose Environment (SP3) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** `docker compose up` from a clean checkout brings up two full cells (`c001`, `c002`) with shared control, identity and signed source, and a smoke script proves login → bootstrap → WSS → cross-cell call → revoke.

**Architecture:** A one-shot `provisioner` (shell + openssl + keytool + jq + psql + Flyway CLI) writes a per-service read-only mount (`/run/signaling`) from `topology.env` in two stages: stage 1 mints PKI, keys, passwords and pod UUIDs; stage 2 migrates and seeds PostgreSQL, reads the WAL facts and renders `provider.json`, `process.json`, `runtime.yaml`, the directory snapshot and a per-service `launch.sh`. Everything it renders is validated offline by the REAL loaders (`MountedNativeContract`, `LocalProcessProfile`, `LocalPlatformConfig`) so schema drift fails a unit test, not a container start. The app, identity and local-platform images already exist or are built from the existing exec jars; a small Node fake kube-API serves Pekko discovery.

**Tech Stack:** Docker Compose ≥ 2.26 (volume `subpath`), PostgreSQL 17.6, OpenSSL 3, JDK 21 `keytool`, `jq`, Flyway CLI (same version as the app: 11.7.2, checksum-pinned), Node ≥ 20 (`ws` pinned) for `kube-api` and `smoke`, Bash, Python 3 `unittest` for the compose lint, JUnit 5.

**Spec:** `docs/superpowers/specs/2026-10-21-docker-compose-two-cell-design.md` (§3–§9). SP1: `signaling-app/.../runtime/local` and `config/native-sources.md` ("Local process profile"). SP2: `signaling-local-platform` and `config/native-sources.md` ("Local platform source"). Recipe for every rendered field: `signaling-integration-tests/src/test/java/io/webrtc/signaling/it/LocalCompositionFixture.java`, `LocalCompositionPki.java`, `LocalCompositionDatabase.java` (the working one-cell version of what this plan renders).

## Global Constraints

- Everything is `LOCAL_TEST_ONLY` / `NOT_QUALIFIED`; every artifact and log says so. No production, HA, physical-fencing, N−1 or 10M claim. Passing the acceptance criteria permits `LOCAL_QUALIFIED` only.
- No secret, private key, password or digest of a real deployment in git. Passwords and keys are generated at provision time into a named volume with mode `0440`, group `10001`. Compose files hold no password; JVM services read passwords from mount files in `launch.sh`.
- Every service: UID 10001, `read_only: true`, `tmpfs: /tmp` (128 MiB), `cap_drop: [ALL]`, `security_opt: [no-new-privileges:true]`. Images pinned by digest, never `latest`.
- PostgreSQL ≥ 17 (`postgres:17.6`); cell DBs: primary + one streaming standby, `synchronous_standby_names='ANY 1 (...)'`, `synchronous_commit=on`, `ssl_min_protocol_version=TLSv1.3`, JDBC `sslmode=verify-full`; never `localSingleWriter`.
- Pekko: six actors per cell (quorum 4), roles `az-a`/`az-b` (3+3), `SIGNALING_FORMATION_PROFILE=initial-six`; discovery only through the fake kube-API; the twelve actors start concurrently (the actors' start-up gate is 30 s from process start).
- Network isolation (spec §3.2): Artery (25520) and management (8558) exist only on `net-c001` / `net-c002`; shared RPC/HTTPS only on `net-shared`; ports that bind every interface (cell RPC, gateway WSS and relay, control HTTPS) must be reachable only from the networks that need them.
- Addresses, ports, names come from `deploy/compose/topology.env` only (compose, PKI, contracts read it); the exact tables are spec §3.2.
- Certificates: workload leafs carry EXACTLY ONE URI SAN `spiffe://signaling/local-test-only/cell/<cell>/(actor|gateway|control)[/<id>]` (actors use `/actor/<name>`: per-pod ids), TLS 1.3, EKU server+client; DNS/IP SANs as the spec lists.
- New Java files google-format only; never run `spotless:apply` repo-wide. Build with `./mvnw -o … -am`; failsafe ITs by fully qualified `-Dit.test=`.
- `bootId` of each gateway is pinned per provision: restarting only a gateway container is unsupported; recovery is `make reset` then `make up`. The README says so plainly.

## Rulings made while planning

- Source TLS: the `local-platform` leaf is issued by CA-workload (as the SP1/SP2 fixtures do), so the contract's single `tls.ca` trusts it; the separate CA-source of spec §4.6 is dropped. The DB CA stays separate (`sslrootcert` points at `db-ca.crt` in the mount, allowed by the platform's JDBC allow-list and by the contract's path confinement). Task 9 amends spec §4.6.
- `podIp` of an actor is its `net-cell` address (Artery, management and `/ready` bind it; Pekko discovery returns it); its cell-RPC entry advertised in `cells[]` is the `net-shared` address of `actor-<cell>-1` (spec §3.2). Stopping `actor-<cell>-1` therefore removes the cell's RPC entry; fault tests in this plan stop `actor-<cell>-6`.
- Hikari `connectionTimeout` is a hard-coded 500 ms (`DbPools.java:53`) and is NOT changed (production code; spec §8 forbids lowering deadlines). Compose mitigates load with staged start, `start_period`, and `restart: on-failure:5` for actors and control; gateways use `restart: "no"` because of the pinned boot id.
- The app DB role is `signaling_app` (non-superuser, member of `pg_monitor`, DML/sequence/function privileges via `grants.sql`); Flyway/seed run as the owner role `signaling_owner`. If the acceptance run exposes a missing privilege, the fix is a line in `grants.sql` (Task 10 owns that iteration).
- Compose mount: one named volume `run`, each service mounts `subpath: <service>` at `/run/signaling` read-only; no host bind mounts.
- Default JVM heap `-Xms256m -Xmx384m` (+ `-XX:+ExitOnOutOfMemoryError`) until Task 10 records measured RSS and sets the default from the measurement.
- Identity: the committed test key and accounts are kept; accounts and issuer/audience are overridden by environment so `admin` and `mod` land in different cells (a pinned test asserts the bucket → cell mapping, Task 6). Issuer `http://identity.signaling.local:8081`, audience `webrtc-signaling`.

## Review Focus

Each line is pinned by a test in the owning task.

1. Rendered `provider.json` / `process.json` / `runtime.yaml` / snapshot drift from the Java loaders: all rendered files for all 17 JVM services load through the real loaders (Task 3, Java test).
2. A leaf with zero or two URI SANs, or an actor sharing one SPIFFE id with a sibling (Task 2 shell tests; the platform's boot registry is per-identity).
3. Directory snapshot / `directory_bucket` seed / `bucket_authority` split disagree (Task 3 consistency test, Task 4 SQL check against the seeded DB).
4. A password or key reaching git, the compose file, `docker inspect` env or `ps` (Task 7 lint; launch.sh reads files, exports only the three Pekko store passwords).
5. Re-provisioning over an existing volume minting new pod UIDs/boots that disagree with running containers (Task 2: refuse unless the volume is empty; `make reset` wipes).
6. c001 actors with a route to c002 Artery/management (Task 7 lint of networks; Task 10 runtime probe).
7. Not enough RAM/CPU: `make up` pre-flight fails with a clear message instead of flaky startup (Task 9).

## File Structure

```
deploy/compose/
  topology.env                      # single source of addresses/ports/names/ids
  .env.example                      # image refs (digests), JVM heap; no secrets
  compose.yaml
  Makefile  README.md  hosts.sh  preflight.sh
  provisioner/Dockerfile  provision.sh  lib/{pki,db,render}.sh  templates/*.jq  sql/{grants,roles}.sql  tests/
  postgres/{init-primary.sh,init-standby.sh,pg_hba.conf.tmpl}
  kube-api/{Dockerfile,package.json,server.mjs,server.test.mjs}
  smoke/{package.json,package-lock.json,smoke.mjs,payloads.mjs,payloads.test.mjs}
  lint/compose_lint.py  lint/test_compose_lint.py
Dockerfile.identity                 # repo root, like Dockerfile.local-platform
signaling-integration-tests/.../ComposeRenderedContractsTest.java   # Task 3
signaling-integration-tests/.../ComposeUserBucketsTest.java         # Task 6
```

---

### Task 1: Foundation — `topology.env`, skeleton, `hosts.sh`, topology check

**Files:** Create `deploy/compose/topology.env`, `hosts.sh`, `lib/topology.sh` (sourced by every script), `tests/test_topology.sh`, `tests/run-tests.sh`.

**Interfaces:**
- Produces: `topology.env` shell-sourceable `KEY=value` file defining: cells (`CELLS="c001 c002"`), subnets (`NET_C001=172.30.1.0/24`, `NET_C002=172.30.2.0/24`, `NET_SHARED=172.30.0.0/24`), per-service addresses exactly as spec §3.2 (e.g. `ACTOR_C001_1_CELL_IP=172.30.1.11`, `ACTOR_C001_1_SHARED_IP=172.30.0.31`, `GATEWAY_C001_IP=172.30.0.21`, `CONTROL_IP=172.30.0.10`, `IDENTITY_IP=172.30.0.11`, `PLATFORM_IP=172.30.0.12`, `PG_DIRECTORY_IP=172.30.0.14`, `PG_C001_PRIMARY_IP=172.30.1.101`, `PG_C001_STANDBY_IP=172.30.1.102`, `KUBE_C001_IP=172.30.1.250`, same for c002), ports (`RPC_PORT=8443`, `RELAY_PORT=9443`, `SOURCE_PORT=9443`, `ADMIN_PORT=9444`, `HEALTH_PORT=8559`, `GATEWAY_PUBLISH_C001=8443`, `GATEWAY_PUBLISH_C002=8444`, `CONTROL_PUBLISH=8445`, `IDENTITY_PUBLISH=8081`), hostnames (`c001.signaling.local`, `c002.signaling.local`, `bootstrap.signaling.local`, `identity.signaling.local`), `ISSUER`, `AUDIENCE`, az roles per actor (actors 1–3 `az-a`, 4–6 `az-b`), `GATEWAY_ID_C001=gw-c001-a` / `GATEWAY_ID_C002=gw-c002-a`, namespaces `wsa-c001` / `wsa-c002`, `ORIGIN=https://app.signaling.local`, directory bucket split `SPLIT=8192`.
- `lib/topology.sh`: `load_topology`, `service_list` (prints the 17 JVM service names: `actor-c001-1..6`, `actor-c002-1..6`, `gateway-c001`, `gateway-c002`, `control`, plus `identity` and `local-platform` — 17 names; the 15 plane JVMs are the first 15), `ip_of <service> <network>`.
- `hosts.sh` prints (never writes) the four `/etc/hosts` lines.

- [ ] **Step 1: Write failing tests** `tests/test_topology.sh` (plain bash, `assert_eq` helper, exits non-zero on failure) — `all_addresses_unique`, `every_address_inside_its_subnet`, `kube_api_has_one_ip_per_cell_network`, `actors_have_cell_and_shared_ips`, `service_list_has_16_names`, `hosts_sh_prints_four_lines_and_writes_nothing` (run in a temp HOME, assert `/etc/hosts` unchanged by mtime).
- [ ] **Step 2: Run** `bash deploy/compose/tests/run-tests.sh` → FAIL (files missing).
- [ ] **Step 3: Implement** the files; address arithmetic helper in `lib/topology.sh` for CIDR membership using only bash/`python3 -c` ipaddress if needed.
- [ ] **Step 4: Run** → PASS.
- [ ] **Step 5: Commit** `feat(compose): add the single-source topology and its checks`.

### Task 2: Provisioner stage 1 — PKI, keys, passwords, pod UUIDs

**Files:** Create `provisioner/lib/pki.sh`, `provisioner/provision.sh` (stage dispatcher: `provision.sh stage1|stage2|all`), `provisioner/Dockerfile` (base: pinned JDK 21 image; add `openssl`, `jq`, `postgresql-client-17`, Flyway CLI 11.7.2 downloaded from Maven Central with a pinned SHA-256 recorded in the Dockerfile `ARG`); Test `provisioner/tests/test_pki.sh`.

**Interfaces:**
- Consumes: `topology.env` (Task 1).
- Produces under `$RUN_ROOT` (default `/run/signaling-src`, the `run` volume root): `_shared/` (CA certs only; never keys of CAs shipped to services), and per service `<service>/` with: `ca.crt` (CA-workload), `leaf.crt`/`leaf.key` (workload leaf per spec §4.6; gateway leaf also carries public DNS SANs `c00N.signaling.local`; control leaf carries `bootstrap.signaling.local`; actor DNS SAN `actor.<cell>.signaling.local`, IP SANs per the leaf's addresses), actors only: `remoting.p12`, `trust.p12` (CA-remoting-`<cell>`, IP SAN of the net-cell address), `remoting.pass` (random, 1 file for the three Pekko passwords), `kube-ca.crt`, `kube-token`, `privacy.key` (32 random bytes), `proof.key` (cell Ed25519 PKCS#8, per cell shared by the cell's actors), `proof.pub` for every service, `source.pub`, `jwt.pub` (copy of `signaling-identity/src/main/resources/keys/public.pem`, baked into the provisioner image), `db-ca.crt`; DB server leafs (`pg-*`) and `local-platform` source leaf (CA-workload, IP SAN `PLATFORM_IP`); `pod-uids.env` (`<SERVICE>_POD_UID=<uuid>`, one per JVM service) and `gateway-boots.env` (`GATEWAY_BOOT_C001=<uuid>` …) generated ONCE.
- Re-run rule: stage 1 exits non-zero with `PROVISION_REFUSED volume not empty` if `$RUN_ROOT/.provisioned` exists, unless `PROVISION_FORCE=1` (used only by `make reset`).
- Modes: files `0440`, directories `0750`, owner `root:10001`; keys never world-readable.

- [ ] **Step 1: Write failing tests** `tests/test_pki.sh` run against a temp `RUN_ROOT` with fast mode (RSA 2048 allowed, small validity not used): `each_workload_leaf_has_exactly_one_uri_san`, `actor_uri_san_is_per_pod_id` (`…/cell/c001/actor/actor-c001-3`), `gateway_uri_san_carries_gateway_id`, `control_leaf_has_control_uri_and_bootstrap_dns`, `gateway_leaf_has_public_dns_san`, `leaf_has_server_and_client_eku`, `leaf_chains_to_workload_ca`, `remoting_p12_has_ip_san_and_loads_with_keytool`, `proof_key_is_ed25519_and_pub_matches` (derive pub from key via `openssl pkey -pubout`), `pod_uids_are_unique_lowercase_uuids`, `no_key_is_world_readable`, `second_stage1_run_is_refused`, `force_flag_regenerates`, `no_secret_appears_in_stdout` (capture stdout/stderr of stage 1; grep for the password file contents and `PRIVATE KEY`).
- [ ] **Step 2: Run** `bash deploy/compose/provisioner/tests/test_pki.sh` → FAIL.
- [ ] **Step 3: Implement** `lib/pki.sh` (openssl `req/x509` with `-addext` for SAN/EKU; keytool `-importkeystore` for PKCS12; Ed25519 via `openssl genpkey -algorithm ED25519`) and the Dockerfile; `provision.sh` logs only file names, never contents.
- [ ] **Step 4: Run** tests → PASS. Build the image once: `docker build -t signaling-provisioner:local deploy/compose/provisioner` (context needs the identity public key and the migration SQL; copy via the Dockerfile `COPY` from paths allowed by a dedicated `deploy/compose/provisioner.dockerignore`-style context, or build with `-f` and the repo root context whitelisting only those inputs — pick one, keep `.dockerignore` hygiene, document it in the README task).
- [ ] **Step 5: Commit** `feat(compose): provision PKI, keys and pod identities with per-pod SPIFFE ids`.

### Task 3: Contract renderer and offline validation by the real loaders

**Files:** Create `provisioner/lib/render.sh`, `provisioner/templates/{provider,process-actor,process-gateway,process-control,runtime,directory,platform-config}.jq`, `provisioner/tests/test_render.sh`; Test `signaling-integration-tests/src/test/java/io/webrtc/signaling/it/ComposeRenderedContractsTest.java` (package `io.webrtc.signaling.storage`, like the SP1 harness; plain JUnit, no Docker, name `*Test`).

**Interfaces:**
- Consumes: Task 1 topology, Task 2 mount layout, WAL facts `WAL_<CELL>_SYSTEM_ID`, `WAL_<CELL>_TIMELINE` (from Task 4; in `--offline` mode render uses `WAL_*` placeholder values `1`/`1`).
- Produces per service `<service>/{provider.json,process.json,runtime.yaml,directory.json,launch.sh,env.list}` plus `local-platform/platform.json` (+ files it references) and `identity/` has no contract. `render.sh render [--offline]`.
  - `provider.json` follows the fixture field-for-field (`schemaVersion 1`, `qualification LOCAL_TEST_ONLY`, `cell`, `storageEpoch 1`, `routingEpoch 1`, `environment local-test-only`, `identity{issuer,audience,PT15M,PT30S,PT1S,PT5S,true,highWaterSource}`, `cellDatabase`, `directoryDatabase` (the loader requires it on every plane: control gets `signaling_app` credentials; actors and gateways get the real URL with the deliberately powerless role `signaling_nodir`, which has no CONNECT privilege on `directory`, so the planes that must never read the directory hold no directory credential), `tls`, `sources{clockUrl,revocationUrl,publicKeys{"local-source": source.pub}}` with `https://<PLATFORM_IP>:9443`, `proofs{keyId "local", privateKey, publicKeys{"<cell>/local"}}`, `jwtPublicKeys{"signaling-identity-key": jwt.pub}` (key id from `signaling-identity` `application.yml`), `cells[]` (entry = `actor-<cell>-1` shared IP and `RPC_PORT`, `tlsAuthority` `actor.<cell>.signaling.local`), `gateways[]` (the cell's gateway at `GATEWAY_<CELL>_IP:RELAY_PORT`, `tlsAuthority` `gateway.<cell>.signaling.local`), `directorySnapshot`, `directorySnapshotSha256`).
  - `process.json` per plane as spec §4.1 and `config/native-sources.md` "Process profile": every key present, optional `null`; actor `azRole` per `topology.env`, `cellAzRoles ["az-a","az-b"]`, `wal` (`replicas ["standby-<cell>"]`, `synchronousReplicas 1`, `maximumLagBytes 16777216`, `freshness PT1S`, `maximumReplyAge PT5S`, `localSingleWriter` is not rendered), `kube{apiHost=KUBE_<CELL>_IP, apiPort=443, caPath, tokenPath, namespace=wsa-<cell>}`, `privacyPseudonymKey`; gateway `gatewayId`/`gatewayBootId` from `gateway-boots.env`, `allowedOrigins [ORIGIN]`; control `transferCells` = the other cell with its database credentials.
  - `directory.json` two ranges split at `SPLIT` and `gatewayBoots` for BOTH gateways; the SHA-256 of its exact bytes goes into every `provider.json`.
  - `runtime.yaml`: copy of `config/production-defaults.yaml` sections with `identity.issuer/audience` bound to the real values (no `${…}` left) — the loader rejects unresolved values.
  - `launch.sh`: `exec java ${JVM_OPTS} -jar /opt/signaling/app.jar` after exporting, for actors only, `SIGNALING_REMOTING_{KEYSTORE,KEY,TRUSTSTORE}_PASSWORD="$(cat /run/signaling/remoting.pass)"`; no other secret is exported. `env.list` holds the non-secret env of §4.2 (the compose file references it via `env_file` from the volume is impossible — so compose repeats the non-secret values from `topology.env`; `env.list` exists for the Java test to compare against).
  - `platform.json` for `local-platform` per `LocalPlatformConfig` (read its keys): `cells {c001:1,c002:1}`, `keyId local-source`, `signingKey`, `source`/`admin` listeners (keystores, passwords from files, `clientTrust` bundles — workload CA for source, a separate admin CA for admin), `health` (concrete IP, 8559), `database` (jdbc URL to `pg-directory` `source` DB with `sslmode=verify-full&sslrootcert=…`, password file).

- [ ] **Step 1: Write failing tests.** `ComposeRenderedContractsTest`: generates the full mount tree by running `provision.sh stage1 && render.sh render --offline` (skip with `Assumptions` when `openssl`, `keytool`, `jq` or `bash` are missing, and fail loudly on CI-marked runs) into a `@TempDir`, then asserts for ALL 17 JVM services: `MountedNativeContract.load(provider.json, "local-minikube", "LOCAL_TEST_ONLY")` succeeds; `LocalProcessProfile` and `LocalRuntimeContract` (the SP1 loaders — use their package-visible entry points through a same-package test helper) accept `process.json` with the matching plane; `directorySnapshotSha256` equals the SHA-256 of `directory.json`; `LocalPlatformConfig.load(env, platform.json)` accepts `platform.json` (the signing key path loads via `SourceSigner.load`); snapshot ranges equal the `SPLIT` rule and cover 0..16383 with no gap; each gateway's `gatewayBootId` equals its snapshot `gatewayBoots` entry; `podUid` equals `pod-uids.env`; no rendered file contains `${`. Negative cases (each must fail the loader): drop one `process.json` key → `profile/INVALID`; flip a snapshot byte → digest mismatch `contract/INVALID`; second URI SAN added to a leaf → rejected by the cell RPC identity check where the loader exercises it (if the loader does not parse certificates, assert the openssl SAN count in `test_render.sh` instead). `test_render.sh`: `render_is_deterministic_for_equal_inputs` (same stage-1 output → byte-identical JSON), `unresolved_placeholder_is_rejected`.
- [ ] **Step 2: Run** `./mvnw -o -pl signaling-integration-tests test -am -Dtest=ComposeRenderedContractsTest -Dsurefire.failIfNoSpecifiedTests=false` and `bash deploy/compose/provisioner/tests/test_render.sh` → FAIL.
- [ ] **Step 3: Implement** `render.sh` + jq templates (jq builds all JSON; values from the sourced topology and the stage-1 outputs; the SHA-256 via `sha256sum`).
- [ ] **Step 4: Run** both → PASS.
- [ ] **Step 5: Commit** `feat(compose): render contracts, profiles and the directory snapshot, validated by the real loaders`.

### Task 4: Database stage — PostgreSQL bootstrap, Flyway, seed, WAL facts

**Files:** Create `postgres/init-primary.sh`, `postgres/init-standby.sh`, `postgres/pg_hba.conf.tmpl`, `provisioner/lib/db.sh`, `provisioner/sql/{roles.sql,grants.sql}`, `provisioner/tests/test_db.sh`.

**Interfaces:**
- Consumes: Task 2 DB leafs and `db-ca.crt`, Task 3 `render.sh` (WAL facts are exported by `db.sh` as `WAL_<CELL>_SYSTEM_ID` / `WAL_<CELL>_TIMELINE` into `$RUN_ROOT/_shared/wal.env`, then `render.sh render` runs).
- Produces: `db.sh migrate-and-seed` (stage 2 front half): per cell DB `signaling` Flyway V001–V018 from `signaling-storage/src/main/resources/db/migration` (default history table `flyway_schema_history`), `pg-directory` DB `directory` Flyway `db/directory/migration` with table `directory_schema_history`, DB `source` created and owned by `signaling_owner` (the platform migrates itself). Seeds exactly like `LocalCompositionDatabase`: `cell_authority(1,'<cell>',1,'GROUPED',1,'ACTIVE')`, 16384 `bucket_authority` rows (ACTIVE for the cell's half, FROZEN otherwise), `group_owner` 0–1023, and 16384 `directory_bucket` rows with `wss_url` `wss://c001.signaling.local:8443/ws` / `wss://c002.signaling.local:8444/ws`. Roles: `signaling_owner` (migrations), `signaling_app` (+ `pg_monitor` + grants), `signaling_nodir` (LOGIN only; `REVOKE CONNECT ON DATABASE directory FROM PUBLIC`), `replicator` (REPLICATION). `synchronous_standby_names='ANY 1 (standby-<cell>)'`, standby created with `pg_basebackup -R` and `application_name=standby-<cell>`, wait until `pg_stat_replication` shows `sync_state='quorum'`, then read `system_identifier` and the timeline exactly as the fixture does.
- PostgreSQL servers: TLS from the mount (`ssl=on`, cert/key from `pg-*` leafs, `ssl_min_protocol_version=TLSv1.3`), `max_connections=400`, `wal_level=replica`, `max_wal_senders=4`, `hba`: `scram-sha-256` only, replication entry limited to the standby address.

- [ ] **Step 1: Write failing test** `tests/test_db.sh` (needs Docker; starts the DB services only via a throwaway compose project `-p sp3dbtest` using the real compose file from Task 7 if it exists, else a minimal inline compose in the test dir; mark clearly in the script header): after `db.sh migrate-and-seed` assert in each cell DB `SELECT count(*) FROM bucket_authority = 16384`, ACTIVE count equals `SPLIT` for c001 and `16384-SPLIT` for c002, `group_owner` count 1024, `directory_bucket` count 16384 and the c001/c002 split equals `SPLIT` (Review Focus 3: compare against the ranges in `directory.json`), `flyway_schema_history` has 18 successful rows and V016's recorded checksum equals Flyway CLI's own (`flyway info`), `pg_stat_replication` shows one `quorum` standby per cell, `signaling_app` can `SELECT pg_control_system()` and `pg_current_wal_flush_lsn()` and read `pg_stat_replication` (pg_monitor) but cannot `CREATE TABLE`, `signaling_nodir` cannot connect to `directory` (and is not a member of `pg_monitor`), a TLS 1.2-only `psql` connection is refused, a connection without `sslmode=verify-full` trust fails, WAL facts non-empty and numeric.
- [ ] **Step 2: Run** `bash deploy/compose/provisioner/tests/test_db.sh` → FAIL.
- [ ] **Step 3: Implement** init scripts (run as `docker-entrypoint-initdb.d`-style or as the container command wrapper reading the mount; follow `LocalCompositionDatabase` for the command line that installs the TLS files with correct owner/mode) and `db.sh`.
- [ ] **Step 4: Run** → PASS; re-run `ComposeRenderedContractsTest` still PASS.
- [ ] **Step 5: Commit** `feat(compose): bootstrap, migrate and seed the cell, directory and source databases`.

### Task 5: Fake kube-API

**Files:** Create `kube-api/{Dockerfile,package.json,server.mjs,server.test.mjs}`.

**Interfaces:**
- `server.mjs` env: `KUBE_CELL`, `KUBE_NAMESPACE`, `KUBE_POD_IPS` (comma list), `KUBE_TOKEN_FILE`, `KUBE_TLS_CERT`, `KUBE_TLS_KEY`, `KUBE_LISTEN` (concrete IP), port 443. One process per cell (compose runs `kube-api-c001`, `kube-api-c002`; each on its cell network only — a deliberate deviation from "one `kube-api` on both networks" so cells share no discovery path; Task 9 amends spec §3.1).
- `GET /api/v1/namespaces/<ns>/pods?labelSelector=app=webrtc-signaling,plane=actor,cell=<cell>` with `Authorization: Bearer <token>` → `200` JSON `{"items":[{metadata:{name,labels},"status":{"podIP","phase":"Running","containerStatuses":[{"name":"actor","state":{"running":{}},"ready":false}]},"spec":{"containers":[{"name":"actor","ports":[{"name":"management","containerPort":8558}]}]}}]}` (shape identical to `LocalCompositionServices.pods(...)`); any other path/selector/namespace/token → 401/403/404 with an empty body; `GET /healthz` unauthenticated on the health address → 200 only after the cert and token were loaded.

- [ ] **Step 1: Write failing tests** `server.test.mjs` with `node:test` (no deps): `serves_the_exact_pod_list_shape`, `rejects_a_missing_or_wrong_token_with_401`, `rejects_wrong_selector_namespace_or_cell_with_404`, `does_not_log_the_token`, `healthz_after_load_only`, `refuses_tls_below_1_3`, `post_is_405`.
- [ ] **Step 2: Run** `node --test deploy/compose/kube-api/` → FAIL.
- [ ] **Step 3: Implement** with `node:https` only; Dockerfile `FROM node:<LTS>-slim@sha256:<digest resolved with docker buildx imagetools inspect at implementation time and recorded in .env.example>`, `USER 10001`, no npm install needed.
- [ ] **Step 4: Run** → PASS. Also build the image.
- [ ] **Step 5: Commit** `feat(compose): add the fake Kubernetes API for Pekko discovery`.

### Task 6: Identity image, account → cell mapping, image contracts

**Files:** Create `Dockerfile.identity` (root), `signaling-integration-tests/src/test/java/io/webrtc/signaling/it/ComposeUserBucketsTest.java`; Modify `.dockerignore` (add the identity exec jar allow-lines, same pattern as the platform's), `qualification/scenarios/image-contract.sh` (identity section, separate flag like `--local-platform-only`), `topology.env` (account block).

**Interfaces:**
- `Dockerfile.identity` like `Dockerfile.local-platform` (same pinned JRE digest, UID 10001, labels NOT_QUALIFIED, entrypoint `java -jar /opt/signaling/app.jar`, no env baked).
- Account block in `topology.env`: `ACCOUNT_ADMIN_USER_ID`, `ACCOUNT_MOD_USER_ID` chosen so `SessionRegistryService.bucket(userId)` of one is `< SPLIT` and the other `>= SPLIT`; compose passes them as `SIGNALING_IDENTITY_ACCOUNTS_0_USERNAME=admin`, `…_PASSWORD`, `…_USER_ID` (Spring relaxed binding of the `accounts` list; verify against `IdentityProperties`), and `SIGNALING_IDENTITY_ISSUER`/`AUDIENCE` from topology; the identity passwords are the committed test ones (`admin123`/`mod123`, TEST_ONLY) — they are not secrets and the README says so.

- [ ] **Step 1: Write failing tests** `ComposeUserBucketsTest`: loads `topology.env` (parse `KEY=value`), asserts `SessionRegistryService.bucket(adminUserId) < SPLIT` and `bucket(modUserId) >= SPLIT` (cells differ — spec §7 step 2), both ids pass `UserId`, and a hard-coded pair of known buckets pins the hash function (so the test fails if the bucket function changes). Add the identity section to `image-contract.sh`: user, entrypoint, no env enrollment, runs `--network none --read-only` and prints the Spring banner-free failure only when issuer/audience are unset? (read how `signaling-identity` behaves without env; assert whatever it does deterministically, e.g. serves 8081 — in that case use `docker run -d` + `/dev/tcp` probe with cleanup; keep the cidfile convention).
- [ ] **Step 2: Run** `./mvnw -o -pl signaling-integration-tests test -am -Dtest=ComposeUserBucketsTest -Dsurefire.failIfNoSpecifiedTests=false` → FAIL (no ids in topology).
- [ ] **Step 3: Implement** the ids (search once with a throwaway Java snippet/`jshell`; record the two chosen ids and why in `topology.env` comments), the Dockerfile, `.dockerignore`, the script section.
- [ ] **Step 4: Run** the test → PASS; build `signaling-identity:local`; `bash qualification/scenarios/image-contract.sh --identity-only signaling-identity:local` → PASS. Existing platform section still passes (`--local-platform-only`).
- [ ] **Step 5: Commit** `feat(compose): package signaling-identity and pin the account to cell mapping`.

### Task 7: `compose.yaml`, `.env.example`, lint

**Files:** Create `compose.yaml`, `.env.example`, `lint/compose_lint.py`, `lint/test_compose_lint.py`.

**Interfaces:**
- Services (26 services per spec §3.1 as built: 17 JVM, 5 PostgreSQL, two kube-api, two provisioner steps): `provisioner` (profile-less one-shot, `restart: "no"`, runs `provision.sh all`, mounts the `run` volume rw; its only privileged act is writing files — it still runs as root only for chown, `cap_drop: [ALL]` + `cap_add: [CHOWN, FOWNER, DAC_OVERRIDE]`), `pg-c001-primary|standby`, `pg-c002-primary|standby`, `pg-directory`, `kube-api-c001|c002`, `local-platform`, `identity`, `actor-<cell>-1..6`, `gateway-<cell>`, `control`. Because the provisioner needs the databases up for stage 2 while the services need stage 1 output, split it into two compose services sharing the image: `provision-1` (stage 1) → databases `depends_on: provision-1: service_completed_successfully` → `provision-2` (needs DBs healthy) → everything else `depends_on: provision-2: service_completed_successfully`. Order per spec §6: kube-api / local-platform / identity healthy, then twelve actors at once, then gateways and control.
- Per JVM service: `image: ${APP_IMAGE}`, `entrypoint: ["/bin/bash","/run/signaling/launch.sh"]`, mount `run` volume `subpath: <service>` at `/run/signaling` read-only, env from `topology.env` values (plane, mode, acknowledgement, contract/profile paths, `SIGNALING_CELL_ID`, `SIGNALING_POD_UID` = value from the mount via launch.sh? No — compose cannot read the mount, and `SIGNALING_POD_UID` is not a secret: `launch.sh` exports it from `pod-uids.env`; the same for `SIGNALING_CLUSTER_FINGERPRINT` (a fixed `TEST_ONLY` SHA-256 string in `topology.env` computed once, same for the cell), `SIGNALING_KUBE_API_HOST/PORT` (real process env, exported by launch.sh from `process.json` via `jq`), `SIGNALING_FORMATION_PROFILE=initial-six` for actors), healthcheck `bash -c` on `/dev/tcp/<ip>/8559` requiring `HTTP/1.1 200` for `GET /ready`, `start_period` 90s, `interval` 5s, `retries` 24; `restart: on-failure:5` (gateways `"no"`); `tmpfs: /tmp:size=128m,noexec,nosuid`; static IPs on the right networks; actors on `net-<cell>` AND `net-shared`; gateways publish `8443`/`8444`; control `8445`; identity `8081`; JVM options via `JVM_OPTS` from `.env`.
- `.env.example`: `APP_IMAGE`, `IDENTITY_IMAGE`, `PLATFORM_IMAGE`, `PROVISIONER_IMAGE`, `KUBE_API_IMAGE`, `POSTGRES_IMAGE=postgres:17.6@sha256:<digest>` (digests resolved at implementation time), `JVM_OPTS=-Xms256m -Xmx384m -XX:+ExitOnOutOfMemoryError`. No secrets.

- [ ] **Step 1: Write failing tests** `lint/test_compose_lint.py` (`unittest`; runs `docker compose -f … --env-file .env.example config --format json` to get the resolved model; skip with a clear message if docker compose < 2.26): `every_service_is_hardened` (user 10001 where applicable, `read_only`, `cap_drop ALL`, `no-new-privileges`, tmpfs `/tmp`), `no_image_uses_latest_or_lacks_digest`, `no_environment_value_looks_like_a_secret` (regex over names/values: PASSWORD, KEY=, PRIVATE, token) — the three Pekko store passwords must NOT appear (Review Focus 4), `every_address_matches_topology_env` (Task 1 parser), `c001_and_c002_actors_share_no_cell_network` and `artery_and_management_ports_are_not_published_or_on_net_shared`, `only_expected_ports_are_published` (8443, 8444, 8445, 8081), `depends_on_order_matches_spec_section_6`, `gateways_do_not_restart`, `healthchecks_use_the_concrete_ip` (not 0.0.0.0/localhost), `volume_mounts_use_subpath_read_only`, `service_count_is_24_plus_provisioners`.
- [ ] **Step 2: Run** `python3 -m unittest discover -s deploy/compose/lint` → FAIL.
- [ ] **Step 3: Implement** `compose.yaml` and `.env.example`; `compose_lint.py` exposes `lint(model, topology) -> list[str]` for the Makefile too.
- [ ] **Step 4: Run** → PASS; `docker compose config -q` succeeds.
- [ ] **Step 5: Commit** `feat(compose): add the two-cell compose model and its lint`.

### Task 8: Smoke test

**Files:** Create `smoke/{package.json,package-lock.json,smoke.mjs,payloads.mjs,payloads.test.mjs}`.

**Interfaces:**
- `payloads.mjs` exports pure builders `authFrame(jwt)`, `inviteFrame(...)`, `acceptFrame`, `offerFrame`, `answerFrame`, `iceFrame`, `hangupFrame` whose JSON matches `ProtocolValidator` and the client contract (`clients/native-browser`); read `ProtocolValidator`, `clients/native-browser`, and `LocalRuntimeCompositionIT`/`NativeGatewayCommandIT` for exact field names, counters-as-decimal-strings and envelope; pin them with `payloads.test.mjs` (`node:test`) against fixtures copied from the Java validator tests.
- `smoke.mjs` env: `IDENTITY_URL`, `BOOTSTRAP_URL`, `CA_FILE` (workload CA from the volume exported by `make smoke-ca`), `ORIGIN`, admin client cert/key/CA for the revoke step (exported by `make smoke-admin`); steps exactly as spec §7: login `admin`/`mod`; bootstrap each and assert different cells; WSS with `Origin`, subprotocol `webrtc-signaling.v1`, `AUTH` → `AUTH_OK`; cross-cell `INVITE` → `ACCEPT` → `OFFER`/`ANSWER`/`ICE` → `HANGUP` asserting `ACK_COMMITTED`, monotonic `callVersion` and the event at the other side; revoke via the source admin endpoint → socket closes with `AUTH_REVOKED` (code 1008). Exit non-zero with a one-line reason on any failed assertion; never print tokens.

- [ ] **Step 1: Write failing tests** `payloads.test.mjs`: `auth_frame_shape`, `invite_frame_shape_and_decimal_string_counters`, `frames_validate_against_java_fixtures` (fixtures under `smoke/fixtures/` produced from the Java validator tests; keep them TEST_ONLY), `smoke_exits_nonzero_with_reason_on_bad_login` (point at a closed port).
- [ ] **Step 2: Run** `cd deploy/compose/smoke && npm ci && node --test` → FAIL.
- [ ] **Step 3: Implement** with `ws` pinned exactly in `package.json` and lock file committed.
- [ ] **Step 4: Run** → PASS (the live run is Task 10).
- [ ] **Step 5: Commit** `feat(compose): add the cross-cell smoke test`.

### Task 9: Makefile, preflight, README, spec updates

**Files:** Create `Makefile`, `preflight.sh`, `README.md`; Modify spec §3.1/§4.6/§8/§9 and `CLAUDE.md` (targeted hunk only — the user has unrelated uncommitted edits in `CLAUDE.md`/`AGENTS.md`; stage only your hunk with `git apply --cached`, never their lines).

**Interfaces:** `make build` (`./mvnw -o -pl signaling-app,signaling-identity,signaling-local-platform -am package -DskipTests -Dspotless.check.skip=true` + `docker build` of the four images with `SOURCE_COMMIT`), `make hosts` (prints `hosts.sh`), `make up` (runs `preflight.sh`, then `docker compose up -d`), `make smoke`, `make down`, `make reset` (`down -v` + `PROVISION_FORCE`-free fresh volume), `make lint`, `make test` (shell + python + node tests). `preflight.sh` fails with a one-line cause if Docker Compose < 2.26, free RAM < 12 GiB, or load average per core > 0.7 (Review Focus 7; the host-load flake of the SP1/SP2 ITs is documented). README: how to run, stop, reset, hosts, resource requirement, the 17 JVM + 9 support containers (5 PostgreSQL, 2 kube-api, 2 provisioner steps) table, gateway-restart limitation, entry-actor limitation (`actor-<cell>-1`), no privacy deletion/epoch adoption/migration, identity passwords are TEST_ONLY, JDK `HttpsServer` limits of the source, `NOT_QUALIFIED`.

- [ ] **Step 1: Write failing tests** `tests/test_preflight.sh`: `fails_when_compose_is_too_old` (stub `docker` on PATH), `fails_below_memory_floor` (stub `/proc/meminfo` via env override), `fails_when_load_is_high`, `passes_on_a_quiet_host_stub`; `tests/test_makefile.sh`: `make -n up` shows preflight before compose; `make -n build` shows the Maven command and four docker builds.
- [ ] **Step 2: Run** → FAIL. **Step 3: Implement.** **Step 4: Run** → PASS.
- [ ] **Step 5: Update docs and commit** `docs(compose): document the environment and amend the spec for the as-built topology` (the amendments: two kube-api, CA-source dropped, per-pod SPIFFE ids, `restart` policy, Hikari 500 ms rationale).

### Task 10: Acceptance run and evidence

**Files:** Create `acceptance.sh`, `ACCEPTANCE.md` (evidence file, labelled `LOCAL_TEST_ONLY` / `NOT_QUALIFIED`), adjustments to any earlier file the run proves wrong (small, each with its own commit).

**Interfaces:** `acceptance.sh` automates spec §9: (1) `make reset && make up` → all services `healthy`, twelve actors `Up`, `/ready` 200 on gateway, control, actors; (2) `make smoke` PASS; (3a) `docker stop actor-c001-6` → cell still ready (control/gateway `/ready` 200, a new INVITE still commits), restart → recovers; (3b) `docker stop pg-c001-standby` → a critical command in c001 fails closed (no commit, error surfaced), restart → recovers; (4) bad config cases each refuse with the fixed `LOCAL_RUNTIME_REJECTED` line: wrong `SIGNALING_CLUSTER_FINGERPRINT`, a leaf with a foreign SPIFFE id (swap files in a copied volume), missing contract (empty mount) — one container each, no secrets in logs; (5) isolation probe: from an `actor-c001-*` container `/dev/tcp` to `172.30.2.11:25520` fails; (6) `git status` clean of secrets and `docker inspect` env contains none of the password file contents. It also records measured RSS per JVM (`docker stats --no-stream`) and the total, then sets the default `JVM_OPTS` in `.env.example` from the measurement.

- [ ] **Step 1: Write the failing check** — run `bash deploy/compose/acceptance.sh` against the tree as it is after Task 9 on a quiet host (preflight must pass); record the first failure(s) verbatim in `ACCEPTANCE.md` under "Iteration log".
- [ ] **Step 2: Fix** whatever it exposes in the owning earlier file (expected classes: a missing `grants.sql` privilege; an env var launch.sh forgot; a health-check start_period; actor start order). One commit per fix with the failing output in the message body.
- [ ] **Step 3: Repeat** until acceptance passes twice in a row from `make reset`. If the host cannot meet the preflight, STOP and report BLOCKED with the preflight output instead of lowering any threshold.
- [ ] **Step 4: Write `ACCEPTANCE.md`** with commands, dates, versions, measured RSS, results, and the explicit statement that this establishes `LOCAL_QUALIFIED` only and nothing about production, HA, fencing, N−1 or 10M. Fill the criteria 1–4 and 6 lines of spec §9 with the evidence names; keep §9 criterion 5 pointing at the SP1/SP2 ITs.
- [ ] **Step 5: Commit** `test(compose): record the local acceptance run`.

---

## Self-Review

- **Spec coverage:** §3 topology/networks → Tasks 1, 7; §4.1–§4.2 contract/env → Task 3 (+ launch.sh); §4.5 DB/seed/snapshot/WAL → Tasks 3, 4; §4.6 PKI → Task 2 (deviations ruled: CA-source dropped, per-pod SPIFFE ids); §4.7 formation/kube-API → Tasks 5, 7; §5 layout → File Structure; §6 order → Task 7 `depends_on`; §7 smoke → Task 8; §8 risks (30 s gate, ports binding all interfaces, bootId pin, jar staleness, resources, exec-jar evidence) → Tasks 7, 9, 10; §9 criteria 1–4, 6 → Task 10, 5 → existing ITs. Inherited notes: per-pod SPIFFE ids (Task 2), pinned gateway boot (Tasks 2, 3, 9), Hikari 500 ms (ruling + Task 7/9), identity Dockerfile (Task 6), JDBC allow-list (Task 3 uses only `sslmode`, `sslrootcert`).
- **Open dependency checked in the plan:** the Task 3 Java test reaches package-private loaders through a same-package helper (the SP1 harness already sits in `io.webrtc.signaling.storage`); Task 6 reads `IdentityProperties` for list binding; Task 8 reads `ProtocolValidator`; each says what to read.
- **Placeholders:** image digests are resolved at implementation time and recorded in `.env.example` (stated where used); no other TBDs.
- **Type/name consistency:** `topology.env` keys, `pod-uids.env`, `gateway-boots.env`, `wal.env`, `launch.sh`, `render.sh render [--offline]`, `provision.sh stage1|stage2|all`, `make` targets are used with the same names across tasks.
- **Proportion:** the plan fixes names, shapes, values and tests; bodies of scripts and templates are left to the implementer.
