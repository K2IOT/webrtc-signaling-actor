# NOT_QUALIFIED

Candidate: `20261007-1059-59288bf-be7f7ebe585b`.
Source: `59288bffcbd5abd734f7d99a5339e1cbd5bbbbc0`.
OCI manifest: `sha256:be7f7ebe585b3a36720c9f5b4a50728d809395b6a6fc77071a44a48dead65afb`.

This immutable TEST_ONLY record covers one local native image build and six
negative startup cases. Actual native metadata, nonroot/read-only startup,
pinned JRE and absence-of-enrollment/native-runtime rejection were checked.
Original Docker config identity is separate from the OCI manifest; the export
retains the original config/layer bytes and their actual descriptor hashes.
The same executable image is selected independently for actor/gateway/control.

Fresh local Java verification passed 659 tests in 142 reports with
zero failures/errors/skips. These observations do not establish production
functional, safety, capacity, security, HA, DR or deployment gates.

Main now binds native actor policies to explicit durable revocation and live
clock beans after enrollment definitions. Actual native mTLS/PostgreSQL/Pekko
fixtures exercise this binding. Complete plane factories and runtime/source/
policy enrollment remain open. Four security modes are scheduled; revokedJti
and retiredSigningKey still require approved scoped source/drill integration.
No distributed10k/100k run, production three-AZ fault/HA/DR, N-1 or24h soak was
collected. No external collector root or qualification fingerprints are invented.

Required inputs remain in `runbooks/qualification-inputs.md`. All12 trusted
same-candidate gates are absent. The verifier must return NOT_QUALIFIED.
