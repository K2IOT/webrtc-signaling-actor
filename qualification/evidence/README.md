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

Staged capacity evidence is mandatory:10k,100k,200k/cell,multi-cell,P010M,P2,P2+N-1
and at least24h retained-data soak. Source worker saturation or missing CPU/NIC/FD/
event-loop measurements blocks a capacity claim. Safety violations always block release.
Signaling emulation does not prove real media recovery/preservation. Reports and averages
cannot substitute for original measurements or independent fencing/WAL/restore receipts.

Until the exact approved candidate, real three-AZ infrastructure, issuer/PKI contracts
and authenticated measurement receipts exist, status remains NOT_QUALIFIED. This
repository's local PostgreSQL/Pekko/TLS tests are correctness evidence and never replace
those production inputs. The verifier and its unit fixtures must not be confused with
a completed production qualification run.
