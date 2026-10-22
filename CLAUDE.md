# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Status and constraints

- Release status is **NOT_QUALIFIED**. Tasks 23–24 (external three-AZ, PKI/identity enrollment, staged capacity, deployed-candidate evidence) are open. Do not add claims of production readiness or 10M capacity.
- Identity values (issuer, audience, JWT lifetime, revocation bounds) are deliberately not fabricated. Startup fails closed on empty or unresolved values. Use reserved documentation addresses and `TEST_ONLY` markers in fixtures; never make a fixture look deployable.
- The authoritative spec, plan and execution log are under `docs/superpowers/` (`specs/`, `plans/`, `execution/`). Read them before changing contract behaviour.

## Commands

Java 21 is required. The wrapper pins Maven 3.9.11.

```sh
./mvnw verify                                                        # full reactor: unit + failsafe ITs + spotless check
./mvnw spotless:apply                                                # fix google-java-format violations
./mvnw -pl signaling-loadgen test -Dtest=CallScheduleTest            # single unit test (surefire) - verified passing
./mvnw -pl signaling-storage verify -Dit.test=NativeHomeClaimSecurityIT -Dspotless.check.skip=true   # single IT (failsafe) - verified passing
```

- **The spotless check currently fails** on about 40 pre-existing files, so `./mvnw verify` and any `verify` run stop with `BUILD FAILURE` after the tests have run. Pass `-Dspotless.check.skip=true` to run tests through `verify` until the formatting debt is cleared.

- `*IT` classes run under failsafe in `signaling-actors`, `signaling-cell-rpc`, `signaling-control-api`, `signaling-gateway`, `signaling-integration-tests`, `signaling-local-platform` and `signaling-storage`. They use Testcontainers PostgreSQL 17.6, so Docker is required.
- `signaling-integration-tests` also needs Chromium. Run `npm ci` in `signaling-integration-tests/src/test/webrtc` first; `CHROMIUM_PATH` selects the browser (default `/usr/bin/chromium`).
- Python qualification regression: `python3 -m unittest discover -s qualification/scenarios/tests` (the pinned venv setup is in `qualification/scenarios/README.md`).
- Local fault suite (Task 22): `qualification/scenarios/fault_suite.py`. It needs Linux, Docker, Node and Chromium, and takes a long time. Its output is `LOCAL TEST ONLY` evidence.
- Deployment render check: `bash qualification/scenarios/deployment-contract.sh` (needs Helm 3.19.0 and PyYAML). It checks Kubernetes object semantics only.
- Local platform source (`signaling-local-platform`, LOCAL_TEST_ONLY, see `config/native-sources.md`): `./mvnw -pl signaling-local-platform -am package` builds `signaling-local-platform/target/signaling-local-platform-0.1.0-SNAPSHOT-exec.jar`. Select its ITs with fully-qualified names: `-Dit.test=io.webrtc.signaling.localplatform.JdbcRevocationLogIT` or `...SourceServerIT`, and the composition IT in `signaling-integration-tests` with `-Dit.test=io.webrtc.signaling.storage.LocalPlatformCompositionIT` (its package is `storage`, so an `it.*` name matches nothing). Image: `docker build -f Dockerfile.local-platform --build-arg SOURCE_COMMIT=$(git rev-parse HEAD) -t signaling-local-platform:local .`, checked with `bash qualification/scenarios/image-contract.sh --local-platform-only signaling-local-platform:local`.
- Browser client (`clients/native-browser`): ES modules with `*.test.mjs` / `*.test.cjs` files. `package.json` defines no test script.

### Running the application

One jar, `signaling-app/target/signaling-app-0.1.0-SNAPSHOT-exec.jar`, serves all planes. Exactly one of the Spring profiles `gateway`, `actor` or `control` must be active. Also set `SIGNALING_IDENTITY_ISSUER` and `SIGNALING_IDENTITY_AUDIENCE`. `SIGNALING_RUNTIME_CONTRACT` may point to an absolute YAML file that overrides the packaged `production-defaults.yaml`. Schema: `config/schema/signaling-config.schema.json`.

## Architecture

**Planes.** The same Spring Boot app (`signaling-app`) composes three planes, selected by profile:
- `gateway` (`signaling-gateway`): public WSS on 8443 with subprotocol `webrtc-signaling.v1`. Input is strict JSON. It routes commands to cells over gRPC/protobuf (`signaling-cell-rpc`).
- `actor` (`signaling-actors`): Apache Pekko cluster members (remoting on 25520) that hold live call and session state. Call IDs are `<coordinatorCell>.e<routingEpoch>.<UUID>`, and the coordinator cell in the ID determines routing. A call ID provides routing only, not authorization.
- `control` (`signaling-control-api`): control-plane API, private management with mutual TLS.

**Authority of truth.** Call and session state is fenced in PostgreSQL (CloudNativePG; `signaling-storage`), which also holds authority epochs, WAL facts and negotiation grants. Public identity and routing come from authenticated server context, and critical repositories must validate them again at the store.

**Command scoping and idempotency.** INVITE is scoped to the caller's home cell. Call-bound commands are scoped `CALL:<callId>` at the original coordinator. The canonical SHA-256 intent hash covers command type, scope, authenticated issuer/jti/user and the validated payload. Transport retry generation, incarnation, trace and deadline are excluded, so a retry is the same intent. Reservations and scoped replay are backed by the native outbox. Bounded delivery and reconciliation workers run in `signaling-storage` (`worker` package).

**Identity and protocol.** `signaling-auth` validates JWTs and revocation feeds. `signaling-identity` ships as its own executable jar. `signaling-protocol` owns the generated protobuf/gRPC contracts. Its frozen previous-release fixture covers adjacent internal minor versions 0 and 1. The fixture is a schema test, not a rolling-upgrade test. User IDs are case-sensitive NFC with no trimming. Public JSON counters are decimal strings.

**Observability and contracts.** `signaling-observability` holds telemetry and its contract test. Telemetry and deployment contracts are documented under `docs/operations/` and `docs/contracts/`.

**Module dependencies** (upstream → downstream):
`signaling-protocol`, `signaling-observability` → `signaling-auth` → `signaling-storage` → `signaling-actors` / `signaling-control-api` → `signaling-cell-rpc` → `signaling-gateway` → `signaling-app`. `signaling-loadgen` depends on `signaling-auth` and `signaling-gateway`. `signaling-local-platform` depends on `signaling-protocol`, `signaling-auth` and `signaling-storage` and is not part of `signaling-app`. `signaling-integration-tests` depends on `signaling-app`, `signaling-gateway` and `signaling-storage`, and (test scope) on `signaling-local-platform`.

**Outside the reactor:**
- `clients/native-browser`: an ES module client for the WSS `v:1` contract. It takes an injected `RTCPeerConnection`. It never persists SDP or ICE. Its durable command journal is IndexedDB and holds only allow-listed intents.
- `qualification/`: Python scenario runner, fault matrix, and evidence verifier. Evidence is written to git-ignored `.superpowers/` directories or to `qualification/evidence/`.
- `deploy/`: Helm chart and CloudNativePG manifests. `runbooks/` holds operator procedures for failover, drain and PITR restore.
- `config/`: packaged production defaults, the compatibility manifest (pins an unqualified candidate), and native-source contract notes.

## Conventions specific to this repo

- Formatting is google-java-format (GOOGLE style) via spotless, but the tree already fails the check. Do not run `./mvnw spotless:apply` repo-wide for an unrelated change; it rewrites about 40 files. Match the formatting of the code around your edit.
- Do not record deployment values, digests or credentials in tracked files. Use the `TEST_ONLY` fixtures and keep real values out of git.

<!-- gitnexus:start -->
# GitNexus — Code Intelligence

This project is indexed by GitNexus as **webrtc-signaling-actor** (9181 symbols, 24105 relationships, 300 execution flows). Use the GitNexus MCP tools to understand code, assess impact, and navigate safely.

> Index stale? Run `node .gitnexus/run.cjs analyze` from the project root — it auto-selects an available runner. No `.gitnexus/run.cjs` yet? `npx gitnexus analyze` (npm 11 crash → `npm i -g gitnexus`; #1939).

## Always Do

- **MUST run impact analysis before editing any symbol.** Before modifying a function, class, or method, run `impact({target: "symbolName", direction: "upstream"})` and report the blast radius (direct callers, affected processes, risk level) to the user.
- **MUST run `detect_changes()` before committing** to verify your changes only affect expected symbols and execution flows. For regression review, compare against the default branch: `detect_changes({scope: "compare", base_ref: "main"})`.
- **MUST warn the user** if impact analysis returns HIGH or CRITICAL risk before proceeding with edits.
- When exploring unfamiliar code, use `query({search_query: "concept"})` to find execution flows instead of grepping. It returns process-grouped results ranked by relevance.
- When you need full context on a specific symbol — callers, callees, which execution flows it participates in — use `context({name: "symbolName"})`.
- For security review, `explain({target: "fileOrSymbol"})` lists taint findings (source→sink flows; needs `analyze --pdg`).

## Never Do

- NEVER edit a function, class, or method without first running `impact` on it.
- NEVER ignore HIGH or CRITICAL risk warnings from impact analysis.
- NEVER rename symbols with find-and-replace — use `rename` which understands the call graph.
- NEVER commit changes without running `detect_changes()` to check affected scope.

## Resources

| Resource | Use for |
|----------|---------|
| `gitnexus://repo/webrtc-signaling-actor/context` | Codebase overview, check index freshness |
| `gitnexus://repo/webrtc-signaling-actor/clusters` | All functional areas |
| `gitnexus://repo/webrtc-signaling-actor/processes` | All execution flows |
| `gitnexus://repo/webrtc-signaling-actor/process/{name}` | Step-by-step execution trace |

## CLI

| Task | Read this skill file |
|------|---------------------|
| Understand architecture / "How does X work?" | `.claude/skills/gitnexus/gitnexus-exploring/SKILL.md` |
| Blast radius / "What breaks if I change X?" | `.claude/skills/gitnexus/gitnexus-impact-analysis/SKILL.md` |
| Trace bugs / "Why is X failing?" | `.claude/skills/gitnexus/gitnexus-debugging/SKILL.md` |
| Rename / extract / split / refactor | `.claude/skills/gitnexus/gitnexus-refactoring/SKILL.md` |
| Tools, resources, schema reference | `.claude/skills/gitnexus/gitnexus-guide/SKILL.md` |
| Index, status, clean, wiki CLI commands | `.claude/skills/gitnexus/gitnexus-cli/SKILL.md` |

<!-- gitnexus:end -->
