# Distributed signaling qualification

Profiles describe requested load. They never establish measured capacity or release status.
Start with 10k, then 100k, 200k per cell and multiple cells before attempting P0/P2 10M.
Each source worker runs the same immutable candidate, scenario, seed, worker count and
ordered source-IP list. Worker index selects a disjoint half-open socket range; user
index is global socket index modulo the scenario's distinct-user count. Every session
must have its own genuine `(issuer,jti)` and signed RS256 token supplied by the approved
issuer. Multiple sessions per user are intentional. Tokens and signing private keys
must never enter evidence, command-line arguments or logs.

The worker config conforms to `config.schema.json`. Its source IP must be assigned to
the local machine; WSS endpoints require TLS1.3, trusted CA and hostname validation.
Supply pinned RSA public keys and original/refresh JSONL inventories containing
`socketIndex`, `token`. Inventory records must match the deterministic user ID
`userPrefix + (socketIndex % distinctUsers)` and have distinct session keys.
Same-jti refresh inventory is atomically replaced by the issuer workflow, not fabricated
by the generator. The worker validates issuer, audience, signature, lifetime and refresh
identity before sending. Paths to token inventories are configuration, tokens are secrets.

Build the executable worker with `./mvnw -pl signaling-loadgen -am package`.
Run the worker with `java -jar signaling-loadgen/target/signaling-loadgen-0.1.0-SNAPSHOT-runner.jar
--scenario qualification/scenarios/p2-production.yaml --config /approved/worker.json
--evidence /approved/candidate/worker-000`. Orchestration starts one process per declared
source address; it does not manufacture remote machines or source IPs. All worker ranges
and raw histograms must be merged by the evidence verifier before accepting a stage.

Latency begins at intended arrival on the monotonic clock. Late dispatch, rejected
local work and server failures remain in counters; no coordinated-omission correction
is used to hide missed intended arrivals. Keep compressed raw HDR histograms and
resource snapshots, including CPU, NIC, FD, event-loop lag and pending count/bytes.
Generator saturation invalidates capacity evidence even if the server appears healthy.
Requested sockets, established calls and throughput are distinct from observed values.

A smoke using local test PKI/identity is explicitly TEST_ONLY and cannot qualify a release.
10k/100k production smoke stages require approved WSS endpoints, issuer inventories,
distributed source hosts and resource headroom. None is substituted with a constant or
synthetic success counter. AZ loss, revocation/key retirement, dependency failure and DR
are external drills with authenticated timelines tied to the same candidate. The generator
never disconnects production infrastructure or revokes real identities on its own.

Run `bash qualification/scenarios/loadgen-contract.sh` and the module's contract tests
before starting a workload. Keep the resulting release status NOT_QUALIFIED until every
applicable same-candidate gate passes the final evidence verifier.
