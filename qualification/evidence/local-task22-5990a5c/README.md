# Task 22 original local evidence

**LOCAL TEST ONLY — productionQualification: NOT_QUALIFIED.** This is not a deployed candidate or a trusted production release manifest.

Verified source: `5990a5c3ab3957e33342b37644a3219ae7b05c54`, clean checkout.
Source fingerprint: `0e4d0b4cbb819a98a075be8e54dccec04774211c0397bf28cacfd58dce0f84bc`.

## Observed gates

- Selected `./mvnw -B -pl signaling-integration-tests -am clean verify`: **630 tests/143 reports**, zero failures/errors/skips; finished 2026-10-09 15:05:35 +07. Its original collector result was FAILED because of jqwik's simple testcase classname; it has not been relabelled.
- Corrected collector's final `./mvnw -B clean verify`: **758 tests/166 reports**, zero failures/errors/skips; finished 2026-10-09 15:24:31 +07. Collector completed `LOCAL_SUITE_PASSED` on unchanged clean source.
- Pinned Python qualification regression: **78 tests PASS** (74.269s), including nine collector tests and the RED→GREEN jqwik identity regression.
- One fresh read-only whole-request review: no Critical/Important/Minor findings. Its full-gate condition is now satisfied by the original final collector artifact.

The 24-record matrix has 21 local PASS records and three external-only records. Seven external obligations remain NOT_RUN (including four records with both local and external scope); they are not local passes or production qualification.

## Retained bytes

- [evidence.json](evidence.json): original collector output, method bindings and SHA-256 of every report/receipt.
- [run.json](run.json): original command, Java version, interval, exit code, source commit/fingerprint and dirty=false.
- [fault-matrix.yaml](fault-matrix.yaml): original matrix used in the final run.
- [raw-artifacts.tar.gz](raw-artifacts.tar.gz): 166 original XML reports, five actual fault receipts and the original `maven.log`, with their repository-relative paths preserved. Archive bytes were reopened and checked against every original evidence hash before retention.
- [bundle.json](bundle.json): hashes of retained metadata, archive, Python log and review notes.
- [python-tests.log](python-tests.log), [review.md](review.md): regression output and review/rulings.

Extract the archive into a separate directory to inspect original report/receipt paths. The collector output was copied unchanged; later documentation commits do not change the verified code SHA or pretend that the suite ran on them.

## Limits and follow-up

Replicas share one Docker host/network; cluster actors use loopback TCP and a test transport hook. The rejoin schedule establishes native replacement authority before restarting the old address with a new UID. **Immediate concurrent same-address restart/recovery availability remains unverified.**

Three-AZ WAL/HA-controller fencing, approved production identity/source/PKI enrollment, mixed P2/AZ-loss capacity, deployed security/DR, client-matrix qualification and 24-hour soak remain Tasks 23–24 gates. Preserve this bundle as local correctness input; do not submit it as production qualification evidence.
