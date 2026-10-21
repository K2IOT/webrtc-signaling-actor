# TEST_ONLY PKI fixtures

These certificates are `TEST_ONLY` fixtures for `PeerIdentityTest` (`LOCAL_TEST_ONLY`, `NOT_QUALIFIED`).
They are not deployable and identify nothing real. Trust domain `signaling/local-test-only` is a
placeholder; the subject of every certificate carries `O=TEST_ONLY`.

Produced once by `generate.sh` with OpenSSL (EC P-256, 100-year validity so tests never expire).
The script keeps only the public certificates; all private keys were generated in a temporary
directory and deleted, so no key material is committed. Two throw-away CAs exist: `workload-ca`
and `admin-ca`. Each leaf name states its SAN shape (zero URI SANs, two URI SANs, foreign trust
domain, unknown cell, admin path, workload path, ...).

Re-running the script creates new CAs, so regenerate the whole directory, never single files.
