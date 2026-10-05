# Inputs still required for native deployment and qualification

Release status is NOT_QUALIFIED. Local PostgreSQL, Pekko and TLS tests are not a
same-candidate production run. Do not create candidate image digests or measured
receipts from test fixtures.

## Implementation still open

The Spring launcher currently selects and validates a plane; it does not assemble
and start a complete gateway, actor or control process. Native actor composition,
safety source adapters, RPC ingress and physical shutdown components exist and are
locally exercised. They must be joined in the launcher using explicit enrolled
sources and policies. A runtime Secret mounted by Helm is not, by itself, evidence
that the executable reads that Secret or serves its listeners.

The load generator implements ordinary WSS, recovery, reconnect, 2x burst and
native 5x destination/bucket selection. The six security drivers in
`security-abuse.yaml` remain unimplemented and are rejected before socket startup.
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
