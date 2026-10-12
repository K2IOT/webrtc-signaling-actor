# Pending native local tests

Run from the checkout on Ubuntu 24.04 with Java21, Node20+, Python3/venv and an accessible Docker daemon:

```sh
./qualification/scenarios/local/run-pending-native.sh
```

If only Docker is installed on Ubuntu24.04, run `./qualification/scenarios/local/provision-ubuntu.sh`. It installs Java21, Python/venv and browser system libraries using apt (sudo required), downloads a pinned Node24.14.0 runtime into git-ignored `.superpowers/local-tools` only when Node20+ is absent, verifies its official SHA256 checksum, then runs the same gates. It keeps the existing Docker daemon and containers. The runner selects Java21 through JAVA_HOME and does not install global Node packages.

The script installs pinned Node/Python test dependencies into existing test directories and a git-ignored virtualenv, selects installed Chrome/Chromium (override `CHROMIUM_PATH`), and downloads pinned Playwright Chromium if none is installed. On minimal Ubuntu, install browser system libraries with `npx playwright install-deps chromium` from `signaling-integration-tests/src/test/webrtc` using an account with apt privileges. Missing prerequisites stop the run; they are never recorded as PASS.

Testcontainers provisions PostgreSQL17.6, synchronous WAL standby and disposable networks. Native fixtures provision test identities, actor memberships, gateway leases and fault permits; tests inject faults only into their own containers/child JVMs. Existing containers/clusters are untouched. Browser tests use real Chromium IndexedDB and RTCPeerConnection. The adapter receipt test intentionally holds the answer (preventing DTLS), drops the first real ICE candidate at a test relay boundary, and verifies missing ranges, unchanged retry identity and actual end-marker ingestion. This boundary test does not replace native gateway authorization or deployed WSS interoperability. The original qualification collector runs the entire Maven reactor and retains hashed source identity, XML, fault receipts and logs; browser/qualification regression/Spotless logs sit beside its evidence. An output directory is unique for every invocation. Failed runs remain available.

This is **TEST ONLY**, on one Docker host. Simulated AZ roles do not establish physical AZ isolation. Production DB/WAL/fence/backup providers, mTLS/key rotation, privacy retention, deployed client versions, large complete fault cohorts, mobile interoperability and P2 capacity/24-hour soak need deployed environment evidence. `LOCAL_SUITE_PASSED` always retains `productionQualification: NOT_QUALIFIED`.
