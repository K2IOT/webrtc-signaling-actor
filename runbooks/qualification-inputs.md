# Inputs still required for native deployment and qualification

Release status is NOT_QUALIFIED. Local PostgreSQL, Pekko and TLS tests are not a
same-candidate production run. Do not create candidate image digests or measured
receipts from test fixtures.

## Implementation still open

The Spring launcher currently selects and validates a plane; it does not assemble
and start a complete gateway, actor or control process. Its startup guard now
rejects a configuration-only context instead of reporting successful startup. Native actor composition,
safety source adapters, RPC ingress and physical shutdown components exist and are
locally exercised. They must be joined in the launcher using explicit enrolled
sources and policies. Boot now reads `SIGNALING_RUNTIME_CONTRACT` as bounded
effective `signaling` configuration before binding; its current format is
documented in `config/native-sources.md`. This does not install source enrollment,
policies or native factories, and does not demonstrate serving a business listener.

The load generator implements ordinary WSS, recovery, reconnect, 2x burst and
native 5x destination/bucket selection. The six security drivers in
`security-abuse.yaml` remain unwired in the scenario orchestrator and are rejected
before socket startup. Bounded malformed/oversized wire primitives now observe
native gateway close codes and original physical completion over WSS. A separate
stale-generation probe now installs the original observer before opening a fresh
same-user replacement, requires same-incarnation/newer native AUTH_OK plus the
exact STALE_CONNECTION close reason, and joins both clients' original cleanup.
The YAML scenario scheduling/accounting integration remains open.
Their YAML labels cannot count as a successful security drill.

## Required external inputs

| Input | Required content |
| --- | --- |
| Identity and calling policy | Trusted issuer/audience, RSA public keys and refresh source, maximum JWT lifetime, preserved-jti refresh, revocation propagation/hard bounds, explicit calling policy and policy version/freshness |
| Authenticated native sources | Approved clock and ordered revocation HTTPS endpoints, trusted TLS and Ed25519 source identities, cell/storage epoch, pod/process identity, authoritative revocation high-water/cursor and subject/key retirement semantics |
| Internal PKI and directory | Remoting/management and RPC TLS enrollment, named actor/gateway/control workloads, internal proof signing/verification keys, native directory and current cell storage/routing epochs, approved peer capability window |
| Infrastructure and candidate | Three-AZ cluster, PostgreSQL/CNPG fencing and synchronous durability, enrolled hardware/config fingerprints, full source commit and exact actor/gateway/control OCI digests |
| Distributed clients | Real approved source hosts/IPs, hostname-verified WSS endpoints, original and same-jti refresh token inventories, native directory snapshot and approved SDP/ICE traces |
| Security and collectors | Approved revoked-jti/retired-key inventories and drill scope, external collector trust root, original native resource/timing/population and fault observations |
| Recovery witnesses | Physical old-writer fence, acknowledged operation/WAL/system/timeline ancestry, original drill scope and outside-backup epoch high-water |

Private token inventories and private keys must stay outside git and published
measurement bundles. Public keys and source/policy identities must be explicit;
test PKI, permissive fixture policies and constant healthy suppliers cannot fill
these inputs.

## Execution once the runtime and inputs exist

Use the same immutable candidate and effective configuration for 10k → 100k →
200k/cell → multi-cell → P0/P2 → P2 N-1 → 24h retained-data soak. Record original
worker source files, raw HDRs, monotonic counters/resources and UTC stop times.
Run security, dependency, chaos, DR and deployment drills against that candidate.
The burst/skew gate now requires a candidate-bound native source stage and common
observed windows, in addition to the other native checks.

Only an enrolled external collector root can attest the bundle. Run
`qualification/scenarios/verify-evidence.sh` with that explicit trust file; missing
or failed gates retain NOT_QUALIFIED. No production qualification run has been
collected in this workspace.

## Local image and OCI integrity checks

Build the already verified executable with `./mvnw -pl signaling-app -am package`.
The Dockerfile accepts `SOURCE_COMMIT` for its immutable revision label and pins
its linux/amd64 JRE21.0.8+9 base by native manifest digest. The build context allows
only that executable; runtime Secrets and inventories never enter the image.
Build locally with Docker buildx `--load`, then run
`bash qualification/scenarios/image-contract.sh LOCAL_IMAGE`. It checks actual
native metadata/JRE and runs all three profiles without a network, as UID10001,
with a read-only root. Missing identity and absent native composition must each
fail through their distinct bounded diagnostic code. Successful negative startup
smoke does not prove a complete native launcher.

Docker's classic exporter may report the image/config ID under its digest field.
That ID is not an OCI manifest digest. Preserve `docker save LOCAL_IMAGE -o
ORIGINAL.tar`, then run `python3 qualification/scenarios/export-native-image.py
ORIGINAL.tar NEW_OCI_DIRECTORY --source-commit FULL_SOURCE_COMMIT`. The exporter
retains original config/layer bytes, validates source/platform/layer identities,
rejects unsafe/multiple/unbounded inputs and never overwrites an existing layout.
It writes actual OCI manifest/index descriptors and always reports NOT_QUALIFIED.
Keep original archives and layouts outside git. Compare the exported config digest
to the original native Docker image ID and verify every descriptor hash/size.

`qualification/evidence/20261005-2302-e3630d9-3d330291c09f` records one local
TEST_ONLY build and negative startup smoke. Its intentionally incomplete manifest
fails the verifier. It contains no production qualification measurements.
