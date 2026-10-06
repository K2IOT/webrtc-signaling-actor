# Same-candidate release evidence

A directory named `YYYYMMDD-HHMM-git7-imageDigest12` identifies an immutable candidate.
The image component is the first12 hex characters of the actor OCI **manifest digest**,
not a jar hash, mutable tag or Docker configuration/image ID. The manifest binds all
three plane image digests, full source commit, configuration/compatibility/identity
fingerprints, topology, hardware and declared measured envelope.

Every §49.9 gate requires original machine artifacts, timestamps, dataset cardinality,
raw histograms and fault timelines where applicable. Hash every original artifact.
A trusted external collector signs the canonical manifest using its approved Ed25519
key. Verify against an external trust file; a public key supplied by the evidence bundle
cannot authorize itself. Test-only roots and test-only evidence cannot qualify a release.

For the full 10M envelope, staged capacity evidence is mandatory: 10k, 100k,
200k/cell, multi-cell, P0 10M, P2, P2+N-1 and at least 24h retained-data soak.
A smaller measured envelope requires an indexed approved capacity ADR and its
applicable staged tests, P0/P2, N-1 and 24h soak; it can produce only
`PRODUCTION_QUALIFIED:<envelope>`. All 12 gates remain mandatory at either scale.
The declared envelope must include sockets, distinct users, established calls, call
attempts/s, mean call duration, setup frames/s, registrations/s and cross-cell ratio. Source worker saturation or missing CPU/NIC/FD/
event-loop measurements blocks a capacity claim. Safety violations always block release.
Signaling emulation does not prove real media recovery/preservation. Reports and averages
cannot substitute for original measurements or independent fencing/WAL/restore receipts.

Until the exact approved candidate, real three-AZ infrastructure, issuer/PKI contracts
and authenticated measurement receipts exist, status remains NOT_QUALIFIED. This
repository's local PostgreSQL/Pekko/TLS tests are correctness evidence and never replace
those production inputs. The verifier and its unit fixtures must not be confused with
a completed production qualification run.

Create an isolated Python environment and install `qualification/requirements.txt`.
Run the verifier with `QUALIFICATION_PYTHON=/path/to/venv/bin/python bash
qualification/scenarios/verify-evidence.sh /approved/candidate --trust-file
/approved/external-collector-trust.json`. Run unit tests with that same Python using
`python -m unittest discover -s qualification/scenarios/tests`.

The verifier decodes original bounded Java HDR V2 compressed files in microseconds
and compares all declared sample counts and p50/p95/p99/p99.9 values. Each worker
needs indexed summary, aggregate HDR, nonempty phase HDR and generator JSONL artifacts.
`phaseArtifacts` maps each nonempty phase name to its indexed original HDR file.
Worker summaries bind the exact candidate, images, all five fingerprints, source IP,
host and disjoint socket range. The verifier recomputes CPU/NIC/FD/queue headroom
from original samples; a declared `headroom: true` cannot override a failed measurement.
Unmeasured intervals and missing decoder dependencies block qualification.

For native `malformed`/`oversized` profiles, the verifier binds the original
`abuseFraction` to `offeredSetupFrameArrivals` and checks separate aggregate/mode
logical, physical and finite outcome counters in the summary and original JSONL.
Counts cannot regress, exceed the final source, change scope or include boolean
values. Successful source evidence requires observed traffic and no pending or
unknown receipts. These counts do not substitute for the security gate's trusted
drill observations or enter ordinary availability/HDR denominators. The remaining
four scheduler modes are rejected as unimplemented.

`tests/fixtures/TEST_ONLY_latency.hdr` is a four-sample Java-generated unit fixture.
It is never release evidence and contains no credentials or production measurements.

Original worker JSONL also records live authenticated sockets, established caller calls
and cumulative call/cross-cell/relay/registration/reconnect counts for every sample.
The verifier requires integer counts, bounds gauges by the original socket range and
summary peak, rejects counter regression and rejects any counter exceeding its original
summary. Samples must cover the start and entire duration without hidden gaps.
Simultaneous stage capacity also requires the actual collector to align all worker
intervals and native cell measurements; separate peaks do not establish P2 concurrency.

`resourceMeasurements` requires all sixteen native resource metrics. Each value is a
finite nonnegative number, or a map of cell/plane/pool labels with numeric leaves;
maps have at most256 labels per level and at most four nested levels. Strings,
booleans, empty maps, negative values and nonfinite values are rejected. `cpu` is a
fraction of the enrolled CPU limit and `activeCallPreservation` is a preserved-call
fraction, both in0..1. Memory values use bytes, FD uses counts, network/WAL/storage
use bytes/s, row rate uses rows/s, DB pools use counts, and queue/lag/reconnect values
use milliseconds. These type checks do not establish raw collector provenance,
simultaneous P2 load or any production gate on their own.

Every worker descriptor also indexes `configArtifact` and `scenarioArtifact` containing
the exact original source input bytes. Their SHA-256 values must match the original
worker summary's `configHash` and `scenarioHash`. The verifier independently binds the
source candidate, host, literal source IP, seed, test-only flag, deterministic partition,
local socket limit, requested scenario targets and original scheduled start. Preserve
input paths as originally used; copying or redacting the file must not change the bytes
behind those hashes. Token inventory contents are separate secrets and are not indexed.

Original worker UTC runs must fit their own stage within250ms and all workers must
retain the same scheduled start instant. Monotonic samples must stay inside the
original scheduled-start-to-finish window; integer duration allows at most1.25s for
flooring/export, and samples allow250ms clock uncertainty.

P2 and P2 N-1 additionally require a common source observation window of at least
300s with simultaneous live socket/caller-call targets. Original counter differences
in that window must meet actual call, relay and registration rates and cross-cell
ratio. A streaming merge keeps one current sample per source, advances every equal
timestamp together and rejects stale observations older than2.25s. For24h soak the
source window must span at least86395.5s, allowing two2.25s endpoint sampling margins;
the independently required original UTC run and retained dataset still span>=86400s.
These discrete source observations supplement the approved native collectors. They
do not establish actual media preservation, distinct-user measurements, call-lifetime
mean or physical cluster topology, which still require original native evidence.

Indexed drill receipts (`faultTimeline`, `physicalFenceReceipt`,
`acknowledgedWalReceipt`, `outsideBackupEpochHighWaterReceipt`) must be bounded
JSON objects. Their root carries `testOnly:false`, the full candidate ID, source
commit, three image digests and all five fingerprints matching the manifest.
An original nested `testOnly:true` blocks qualification even when the manifest
is signed and its own flag is false. These provenance checks do not replace
validation of native fencing, WAL, epoch or drill timeline semantics.

`tests/complete_bundle_fixture.py` creates a complete, temporary unit input for
the verifier's 12-gate positive path, including bounded HDR and source samples.
Its disposable signing key and production-shaped flags are parser test inputs;
its measurements and drill payloads are synthetic. The fixture is never an
enrolled collector or a candidate evidence bundle.

The `burst-skew` gate requires its own `metrics.stages.burst-skew` descriptor,
with the original candidate-bound workers, inputs, HDRs, resource observations
and UTC interval. A signed checklist without that stage fails. The verifier joins
original source clocks, requires identical native hot scope/population metadata,
and rejects missing, rebound, boolean, regressing or gapped counters. A common
>=300s window must hold live socket/caller-call targets and the declared call
rate. Destination and bucket attempt shares must each be 4.5–5.5 times their
original population share, with >=100 expected hot attempts per scope. This
sampling tolerance applies to offered-load distribution; native safety gates
retain their strict semantics.

The original first60s setup burst must show twice the baseline setup-frame rate
with continuously observed live sockets. Endpoint polling margins are <=2.25s,
so the measured interior must cover >=55.5s and reach >=57.75s; the original UTC
run must still include the complete burst. Source clock gaps cannot hide missing
load. These observations supplement enrolled native measurements, including
actual directory/population provenance, fanout, slow consumers, malicious valid
traffic and shedding. They do not establish those other checks or production
qualification. The complete positive fixture remains temporary and UNIT ONLY.
