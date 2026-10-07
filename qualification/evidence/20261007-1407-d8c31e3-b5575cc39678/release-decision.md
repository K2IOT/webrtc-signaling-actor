# NOT_QUALIFIED

Candidate: `20261007-1407-d8c31e3-b5575cc39678`.
Source: `d8c31e38746d21ffe658930bcee4f3f56f68c0e1`.
OCI manifest: `sha256:b5575cc3967889054825b0f272f354ffcf1cbc7843d6694e8da7b2fd710d9a2f`.

This immutable TEST_ONLY record contains one local native image build and six
negative startup cases. Native nonroot/read-only startup, pinned JRE21.0.8+9,
missing identity and missing native plane were checked. Original Docker config
identity differs from the OCI manifest; all original config/layer hashes and the
image executable were verified against the executable built by clean verify.
The same executable image is selected independently for actor/gateway/control.

Fresh local Java verification passed671 tests in143 reports, with zero
failures/errors/skips. Python69 verifier/exporter tests and the static loadgen
contract passed. These results supply no production qualification gate.

Main now constructs enrolled actor sources, backends, regions, RPC ingress,
fixed safety scheduling/private health and ordered native shutdown hooks.
Spring context stop initiates Pekko shutdown, retaining physical work, leases,
source transports and DB pools until native release is proven. Different source
cell/storage/pod tuples or scheduling ActorSystems reject before backend install.
Actual mTLS/PostgreSQL/Pekko fixtures are TEST_ONLY. Complete managed process
formation, mounted enrollment and gateway/control factories remain open.
One earlier relay operation returned OUTCOME_UNKNOWN; unchanged reproductions
and subsequent full runs passed, without changing its original1s deadline.
The earlier availability cause remains unestablished.

The revokedJti and retiredSigningKey schedules still require approved scoped
source/drill contracts. No10k/100k distributed stage, production three-AZ HA/DR,
N-1 or24h soak was collected. No collector root or qualification fingerprint
is invented. All12 trusted same-candidate gates are absent. Required inputs
remain in `runbooks/qualification-inputs.md`; release stays NOT_QUALIFIED.
