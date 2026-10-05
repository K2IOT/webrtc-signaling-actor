# NOT_QUALIFIED

Candidate: `20261005-2302-e3630d9-3d330291c09f`.
Source: `e3630d92a5dea58cf00d8af928f05c0031ae36d5`.
OCI manifest: `sha256:3d330291c09faa4a5a0946dcaf2bab1cc0c4d60d7900af0d93e4cb9172b93b64`.

This is a local native image build and negative startup smoke record. It is
TEST_ONLY evidence and cannot qualify production. The same executable image
is intended for three separately selected profiles; image digest equality does
not establish a functioning or qualified deployment of those planes.

The pinned JRE image builds and runs as UID/GID10001 on a read-only root with
network disabled. All three profiles reject missing identity configuration and
reject a valid-identity context without installed native resources. The OCI
export preserves original config/layer bytes; every blob hash/size and the
native Docker config identity were verified. Docker image/config ID is kept
separate from the actual OCI manifest digest.

Local verification passed557 Java tests across131 fresh reports and63 Python
tests, with no failures/errors/skips. These results cover local contracts; they
are not measured production functional, safety, capacity, security or DR gates.

Complete native Main assembly and six security drivers remain unimplemented.
No approved runtime/source/identity/PKI/configuration/hardware enrollment,
distributed10k/100k smoke, same-candidate HA/fencing/DR, N-1 or24h retained-data
run was collected. No collector signature or qualification fingerprints are
invented. Required gates and measurements are absent.

The manifest intentionally remains incomplete and marked TEST_ONLY. The
verifier must return NOT_QUALIFIED. Supply the inputs in
`runbooks/qualification-inputs.md`, complete the remaining implementation, and
run all staged/drill gates on a new immutable candidate before qualification.

No image was pushed, no deployment performed and no branch merged.
