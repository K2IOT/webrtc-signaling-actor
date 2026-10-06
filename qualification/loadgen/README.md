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
The gateway derives fixed rejection codes from the original native decoder error,
rejects further admission immediately and gives the peer up to one second to read
that response before forced transport cleanup. Native size/UTF-8 validation still
rejects the frame; framework automatic close is replaced by that bounded owner.
Cleanup never reports a successful write. A client socket closeFuture is only
physical proof: classification waits for the original inbound frame or final
pipeline inactivity, so pending TLS input cannot be hidden by early closure.
Known AUTH/security reasons use fixed1008 enums, separate from an unknown verifier
or source failure. No token, jti or source detail enters those replies.

`SECURITY_CLOSURE` passively observes an already authenticated original socket
within a five-second generator observation window. It sends no command and
performs no issuer/key drill. A recognized fixed1008 reason records native
revocation, token expiration, generic authorization rejection or stale connection;
freshness-unknown remains `SOURCE_UNKNOWN`. Unknown reason strings are discarded.
Neither a reason nor this generator timeout proves an issuer propagation bound:
the orchestrator still needs approved scope, original source witnesses and the
configured identity SLO. Observation keeps its one global credit until original
socket cleanup and never enters ordinary evidence counters.

These primitives are tested over real TLS1.3 with TEST_ONLY RS256 authority and
production decoder/validator handlers. The scenario orchestrator and enrolled
security observations still need all six drivers before security qualification.

The worker now schedules `malformed` and `oversized` modes. `abuseFraction`
selects a deterministic fraction of the original offered setup-frame arrival
stream (the relay schedule), shared across workers by global arrival ordinal.
Selected arrivals send a raw probe in place of an ordinary relay frame. Call,
registration and refresh clocks retain their original schedules. The fraction
must be numeric, positive, at most one and representable in millionths; modes
must be unique. Up to16 local socket checks select an idle authenticated client
without renewing its intended arrival. Failure to admit or complete a probe is
published as a source failure and blocks worker success.

`observed.security` and resource time-series `security` publish separate mode
and outcome counters. `logicalRejected` records the matching native close within
the original two-second window; `verifiedRejected` also requires its original
write/socket physical receipt. `pendingPhysical` and `cleanupUnknown` preserve
unknown cleanup. Invalid probe traffic does not enter authorized-operation HDRs
or their success/error denominator. Authorized reconnect/AUTH traffic retains
ordinary accounting. WSS smoke tests exercise both actual worker modes with
TEST_ONLY issuer keys and registration; they do not qualify real capacity.

`staleGeneration` is also scheduled on that original arrival stream. It requires
an idle authenticated original session with a native binding. A replacement uses
the same approved inventory, source IP, TLS and endpoint; its authorized AUTH and
connection remain in ordinary accounting. Its native same-incarnation/newer
AUTH_OK and original stale closure must both be observed inside the original
five-second window. The receipt retains the replacement ACK's original time;
a late ACK cannot defeat an overdue timer. Verified source rejection joins both
owned sockets and every original write. Reconnect of the original session waits
for that drill's physical receipt.

`securityReplacementSlots` reports actual admitted owner slots, their capacity,
pending count and peak separately from the logical stage socket gauge. Capacity
is at most16 and at most `localSocketLimit - assignedSocketCount`. No spare local
socket budget means GENERATOR_LIMITED without opening a replacement. Slot release
requires original physical cleanup; terminal drain freezes new replacements.

`slowConsumer` is scheduled on the original arrival stream. It owns the actual
client read pause, resumes at original intended+10s and accepts only native
1013/RESYNC_REQUIRED inside original12s. `readPauseVerified` retains actual
pause/resume verification separately from logical/physical rejection. Silence
or an unrelated close remains unknown. At workload stop, new admissions and
business followups freeze while original observations keep their deadlines;
physical drain follows logical settlement. The separate cleanup interval cannot
enlarge the published workload duration. Native queue pressure fixtures are
TEST_ONLY writability fault injection, not measured kernel capacity or isolation.

`revokedJti` and `retiredSigningKey` remain unsupported by scenario scheduling. The complete `security-abuse.yaml` profile still fails
preflight until all requested modes and their approved inputs are implemented.

Profiles requesting any remaining unsupported mode still fail preflight before opening sockets.
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
