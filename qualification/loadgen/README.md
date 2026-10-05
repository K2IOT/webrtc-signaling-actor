# Distributed signaling qualification

Profiles describe requested load. They never establish measured capacity or release status.
Start with 10k, then 100k, 200k per cell and multiple cells before attempting P0/P2 10M.
Each source worker runs the same immutable candidate, scenario, seed, worker count and
ordered source-IP list. Worker index selects a disjoint half-open socket range; user
index equals the socket index for the first distinct-user range. Additional sockets
map into the callee half of that range for genuine multi-session fanout. Every session
must have its own genuine `(issuer,jti)` and signed RS256 token supplied by the approved
issuer. Multiple sessions per user are intentional. Tokens and signing private keys
must never enter evidence, command-line arguments or logs.

The worker config conforms to `config.schema.json` and requires all five candidate
configuration, compatibility, identity-contract, topology and hardware fingerprints. Its source IP must be assigned to
the local machine; WSS endpoints require TLS1.3, trusted CA and hostname validation.
Supply pinned RSA public keys and original/refresh JSONL inventories containing
`socketIndex`, `token`. Inventory records must match the deterministic user ID
`userPrefix + userIndex(socketIndex, distinctUsers)` as implemented by the coordinator and have distinct session keys.
Same-jti refresh inventory is atomically replaced by the issuer workflow, not fabricated
by the generator. The worker validates issuer, audience, signature, lifetime and refresh
identity before sending. Paths to token inventories are configuration, tokens are secrets.

Build the executable worker with `./mvnw -pl signaling-loadgen -am package`.
Run the worker with `java -jar signaling-loadgen/target/signaling-loadgen-0.1.0-SNAPSHOT-runner.jar
--scenario qualification/scenarios/p2-production.yaml --config /approved/worker.json
--evidence /approved/candidate/worker-000`. Orchestration starts one process per declared
source address; it does not manufacture remote machines or source IPs. All worker ranges
and raw histograms must be merged by the evidence verifier before accepting a stage.
The worker retains the exact bytes it parsed in `source-config.json` and
`source-scenario.yaml`; summary hashes bind those snapshots even if the original input
paths change while the run is active. Index them as each worker's `configArtifact` and
`scenarioArtifact`. Inventory token contents are not copied into evidence.

Latency begins at intended arrival on the monotonic clock. Late dispatch, rejected
local work and server failures remain in counters; no coordinated-omission correction
is used to hide missed intended arrivals. Keep compressed raw HDR histograms and
resource snapshots, including CPU, NIC, FD, event-loop lag and pending count/bytes.
Every snapshot also includes live socket/caller-call gauges and cumulative workload
counts so the original time series can establish concurrency and rates.
All workers share `scheduledStartAt`; authenticated warmup must finish before that
common instant. ICE traces seal once at their actual last admitted sequence; the worker
never echoes a peer END or sends a later candidate for a sealed trace. Live traces
occupy a bounded index rather than sampling idle sockets. On reconnect, the worker
waits for a committed RESUME before reading SYNC; a superseded socket cannot start
that read or complete a current request.
NIC rates use only the interface owning the assigned source IP, with actual sample intervals.
FD headroom uses the operating system process soft limit. CPU, NIC and FD require at
least 20% headroom; event-loop lag must remain <=5ms, and pending count/bytes are also
checked against 80% of their configured bounds. Missing measurements fail closed.
Generator saturation invalidates capacity evidence even if the server appears healthy.
Call arrivals use seeded, globally permuted slots for disjoint primary caller/callee
pairs. Secondary callee sessions never contribute an additional caller stream. A busy
pair retains its original intended arrival as a failed INVITE sample. Cross-cell
counts use the actual candidate directory. Requested sockets, established calls and
throughput are distinct from observed values.

A smoke using local test PKI/identity is explicitly TEST_ONLY and cannot qualify a release.
10k/100k production smoke stages require approved WSS endpoints, issuer inventories,
distributed source hosts and resource headroom. None is substituted with a constant or
synthetic success counter. AZ loss, revocation/key retirement, dependency failure and DR
are external drills with authenticated timelines tied to the same candidate. The generator
never disconnects production infrastructure or revokes real identities on its own.

Run `bash qualification/scenarios/loadgen-contract.sh` and the module's contract tests
before starting a workload. Keep the resulting release status NOT_QUALIFIED until every
applicable same-candidate gate passes the final evidence verifier.

Worker `finishedAt` is the original workload stop, captured before socket/executor
cleanup. `observed.workloadDurationNanos` measures that same stop from the original
monotonic start; `cleanupFinishedAt` records teardown separately. The evidence
verifier requires their original UTC/monotonic windows to agree within250ms and
rejects missing stop metadata or cleanup finishing before the workload. Cleanup
time cannot enlarge the measured stage duration.

An INVALIDATED snapshot or original relay RESYNC_REQUIRED retires the current
ICE trace and requests a native restart. The worker sends a new OFFER only
after a committed NEGOTIATION_GRANTED and a matching native snapshot; unknown
outcomes fail the worker. Reconnects use reproducible source-seeded full jitter
with an exponential ceiling from500ms to30s and preserve the original remaining
HTTP Retry-After minimum (bounded delta/date hints up to24h).

Ordinary traffic, the configured 2x/60s burst and native 5x skew are implemented.
The setup microburst accelerates both call arrivals and their setup-frame arrivals
for the original first60s, preserving the §35 `C×S` workload relationship and
recording transient population rather than requiring double steady concurrency.
A seeded actual directory cell receives five times its original callee population
share of intended INVITEs; an occupied native bucket outside that cell receives
five times its own share. Remaining demand uses the cold population. Selectors
use original global arrival ordinals, so worker partitioning does not change
wire targets or request IDs. Caller scheduling and original latency clocks remain
unchanged. Impossible target shares fail preflight instead of reducing demand.
A multiplier of1 disables that hot scope; bucket-only skew works within one cell.

Original summaries and generator time series include `observed.skew`/`workload.skew`
with selected cell/bucket, actual target population sizes and cumulative INVITE
attempt counters. Disabled scopes are null with zero counters. These observations
must establish achieved skew; YAML labels and a local distribution test cannot
qualify capacity. Normal busy/server/admission failures remain failures.

`VirtualClient.probe` now supplies bounded malformed-JSON and oversized-frame
wire primitives. Each receipt preserves the original intended monotonic time,
socket generation and native close code; only an observed matching 1002/1009 is
`PROTOCOL_REJECTED`. A close before dispatch, an unrelated/missing close code,
transport failure, rejected admission and an expired original two-second deadline
remain distinct. Probe results never enter ordinary traffic latency/error counters.
The original write and actual original socket close must both settle before
physical completion releases global count/byte credit or permits a new generation.
The gateway preserves the decoder's original close write and bounds unknown-write
socket cleanup to one second, without claiming that the write succeeded.

These primitives are tested over real TLS1.3 with TEST_ONLY RS256 authority and
production decoder/validator handlers. The scenario orchestrator and enrolled
security observations still need all six drivers before security qualification.

Requested security abuse profiles still fail preflight before opening sockets.
Their presence in YAML cannot produce an ordinary workload presented as security
coverage. Genuine approved source machines and identity inventories remain
required for staged qualification.

For an original 5x source scenario, the evidence verifier now requires that native
skew body in both summary and every retained generator sample. It checks actual
effective target-population bounds, exact selected scope, disabled scope semantics,
original attempt accounting and monotonic counters bounded by the original summary.
Missing, rebound, boolean, regressing or inflated observations fail verification.
These checks establish source consistency; aggregate achieved 5x demand over a
qualified simultaneous source window still needs its own measured gate. Sources
also cannot claim an unsupported security profile as an ordinary successful run.
