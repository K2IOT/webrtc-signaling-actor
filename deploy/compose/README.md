# Two-cell compose environment

Runs the signaling flow `login -> JWT -> bootstrap -> directory -> WSS -> gateway -> actor RPC ->
Pekko -> PostgreSQL -> cross-cell call` on one Linux machine: two home cells (`c001`, `c002`) with
six actors, a gateway and a primary plus streaming standby PostgreSQL each, and a shared control,
directory PostgreSQL, identity service and local platform (clock and revocation source).

**Status: `LOCAL_TEST_ONLY`, release `NOT_QUALIFIED`.** Nothing here is evidence of production
readiness, high availability, host clock discipline, N-1 upgrade behaviour or any capacity figure.
The acceptance run of this environment (spec section 9) has not been executed yet; the environment
is not `LOCAL_QUALIFIED`. Spec: `docs/superpowers/specs/2026-10-21-docker-compose-two-cell-design.md`.

## Requirements

- Linux, Docker Compose >= 2.26 (checked by `make up`), Java 21 and Node >= 20 on the host (Maven
  wrapper and smoke client). Developed and tried only with Docker 29 and Compose 5.6.
- At least 12 GiB of free RAM (an estimate; `PREFLIGHT_MIN_MEM_GIB` adjusts the floor) and a quiet
  CPU: the database deadlines are 500 ms to 2 s, so a busy host makes checkouts and commits fail.
  `make up` refuses at more than 0.7 load per core.
- Free Docker subnets `172.30.0.0/24`, `172.30.1.0/24`, `172.30.2.0/24`.
- 26 containers: 17 JVM (15 plane JVMs: 12 actors, 2 gateways, control; plus identity and
  local-platform), 5 PostgreSQL (`pg-<cell>-primary`, `pg-<cell>-standby`, `pg-directory`), 2 fake
  kube-API (one per cell) and 2 one-shot provisioner steps.
- Ports published on `127.0.0.1` only: `8443` gateway c001, `8444` gateway c002, `8445` control,
  `8081` identity.

## Quick start

```sh
make -C deploy/compose build     # jars (offline Maven; MVN_OFFLINE=0 for online) + 5 images
make -C deploy/compose hosts     # prints the /etc/hosts lines; add them yourself
make -C deploy/compose up        # preflight, lifecycle guard, up -d --wait (up to 10 minutes)
make -C deploy/compose smoke     # cross-cell call and revoke
make -C deploy/compose down      # always removes the volumes
```

`make help` lists every target: `build`, `hosts`, `preflight`, `up`, `smoke-ca`, `smoke`, `down`,
`reset`, `lint`, `test`. `.env` is created from `.env.example` on first use and never overwritten.
Without the `/etc/hosts` lines `make smoke` resolves the four names internally.

## Topology

| Network | Subnet | Carries |
|---|---|---|
| `net-c001` | 172.30.1.0/24 | Artery 25520, management 8558, cell PostgreSQL, kube-API of c001 (internal, no egress) |
| `net-c002` | 172.30.2.0/24 | the same for c002 (internal, no egress) |
| `net-shared` | 172.30.0.0/24 | RPC and relay, bootstrap, source, identity, `pg-directory`, published ports |

c001 has no route to the Artery of c002. Control joins all three networks. Every address, port and
name comes from `topology.env`; nothing else holds one.

## How it works

1. `provision-1` writes the PKI, signing keys, passwords, pod ids and PostgreSQL mount files into
   the `run` volume; every service mounts only its own read-only subpath of it.
2. The five PostgreSQL servers start from those mounts; the standbys base-backup from the primaries.
3. `provision-2` runs Flyway, seeds the cell and directory databases, reads the WAL facts and
   renders `provider.json`, `process.json`, `runtime.yaml`, the directory snapshot and `launch.sh`
   per service, validated by the same loaders the application uses.
4. Each cell's fake kube-API answers the Pekko discovery query; local-platform and identity start.
5. The twelve actors start at once (the 30 s start-up gate counts from process start), then the
   gateways and control.
6. The application is the packaged `signaling-app` jar, selected per container by Spring profile.

## Lifecycle and limitations

- **Supported lifecycle: `make up`, use, `make down` (always `down -v`).** Provisioning runs with
  `--if-needed`, so a completed stage is skipped. A gateway that exited and is started again would
  reuse the boot id pinned at provision time, whose lease has expired, and never becomes ready.
  Restarting single containers is therefore unsupported; recover with `make reset` then `make up`.
  `make up` refuses (and says so) when the `run` volume exists while a JVM service is not running,
  or when the volume holds an `.offline` marker.
- `docker compose down` without `-v` is unsupported. Dropping only the PostgreSQL volumes while
  keeping `run` is unsupported too: it leaves empty databases with passwords from the old run.
- The RPC entry is `actor-<cell>-1` (the rendered contract names that one pod).
- No privacy deletion, epoch adoption, promotion or bucket migration: the stand-ins refuse them.
- Control holds the credentials of both cell databases (`LOCAL_TEST_ONLY` only).
- The source and admin listeners are JDK `HttpsServer`: a pre-authentication connection pins a
  worker thread until it times out, and a malformed request line is answered with `text/html`.
- Clock figures are this container's own measurement plus a fixed 100 ms allowance. They say
  nothing about host clock discipline.
- The identity accounts `admin` / `admin123` and `mod` / `mod123` and the identity signing key are
  committed `TEST_ONLY` test material; never reuse them.
- The source remembers only the last 8 replaced boots per pod, and its pod table (4096 pods) is never
  reclaimed; a restarted source is not a supported scenario. Actors carry per-pod SPIFFE ids
  (`.../actor/actor-c001-3`), so one actor cannot register a boot for a sibling's pod.
- Revoking a user is permanent in this environment; the smoke revokes the callee's token (`jti`) by
  default, which is not (`REVOKE_SCOPE=user` revokes the user).
- `/tmp` of the JVM services is a 128 MiB tmpfs that is not `noexec`: the native libraries (netty
  epoll, lz4, lmdbjava, agrona) are extracted there and must be mappable.
- Restart policy: `on-failure:5` for services, `no` for the gateways (see above) and provisioners.

## Tests

`make test` runs the shell, Node and Python suites (`tests/run-tests.sh`); `make lint` checks the
resolved compose model. `RUN_DOCKER_TESTS=1 make test` adds the Docker-gated tests (real
PostgreSQL containers, minutes). The smoke client's own tests need `npm ci` in `smoke/` once.

## Troubleshooting

- `preflight: FAIL compose|memory|load|subnets|images ...`: one line each, naming the cause.
  `PREFLIGHT_SKIP=name1,name2` skips checks; `images` means run `make build`.
- `guard: REFUSED ...`: run `make reset`, then `make up`.
- A re-run `make up` on a stack whose 17 JVM services all run skips the memory and load checks (the
  stack itself uses that RAM and CPU) and prints `PREFLIGHT_NOTE`; the other checks still run.
- A service is unhealthy: `make up` prints its last log lines. Later:
  `docker compose -p signaling-compose-local logs <service>`.
- Ports 8443 to 8445 or 8081 taken on the host: free them; the values live in `topology.env`.
