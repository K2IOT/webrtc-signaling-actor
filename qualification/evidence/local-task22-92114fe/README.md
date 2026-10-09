# Task 22 concurrent restart/recovery evidence

**LOCAL TEST ONLY — productionQualification: NOT_QUALIFIED.**

Verified clean source: `92114fe0b8ab63f3b614adc685ab5951958c6832`.
Source fingerprint: `1fbddb99154b75bb834e58a37f6020cdec7e91ea6af048fa84e9d84b17b816d2`.

## Observed gates

- Full `./mvnw -B clean verify`: **759 tests/166 XML reports**, zero failures/errors/skips; finished **2026-10-09 20:08:21 +07**, Maven 16m37s. Collector: `LOCAL_SUITE_PASSED`, unchanged source fingerprint, six fresh fault receipts.
- Pinned Python qualification regression: **78 tests PASS** (69.076s); focused collector regression: nine tests PASS.
- Focused final composition Failsafe run passed at 19:50:48 +07; final full run also passed the same composition contract.
- One fresh independent follow-up review plus supplemental corrective reviews: one Important duplicate-ANSWER assertion gap, fixed and verified; no remaining identified findings. Review did not run tests; the executor resolved its pending full-gate condition using original collector artifacts.

The 25-record fault matrix has **22 local PASS** records and three external-only records. **Seven external obligations remain NOT_RUN**; neither local PASS nor this bundle provides production qualification.

## Concurrent schedule

Both takeover-first and immediate restart are required. The concurrent schedule isolates the native shard holder in a six-member cluster with five survivors, restarts the **same canonical address with a new UID before native takeover**, and asserts no replacement epoch immediately before restart.

Original concurrent receipt: native epoch **7 → 8**, **three same-call read queries/two UNKNOWN reads**, recovery observed **27.206s** after fault injection. Stale pulse is fenced and stale idempotent release preserves the exact replacement token. The fixture retains native quorum configuration; its fault-relative **60-second local bound is not a production RTO gate**. This observation does not qualify the production ≤30s business recovery requirement.

The composition fixture now renews its native 15-second gateway tenure during cluster startup and checks same-operation ACCEPT/relay retries with current proofs, physical completion and exactly one recipient frame. These are fixture corrections; no production runtime, TTL, quorum or authorization rule was weakened.

## Retained bytes

- [evidence.json](evidence.json), [run.json](run.json), [fault-matrix.yaml](fault-matrix.yaml): unchanged original collector output, source/run binding and exact executed matrix.
- [raw-artifacts.tar.gz](raw-artifacts.tar.gz): **166 original XML reports, six fault receipts and original Maven log** with paths preserved. Every archived report/receipt was reopened and matched to the original SHA-256 before retention.
- [prior-failed-runs.tar.gz](prior-failed-runs.tar.gz): original failed 150cba1 full-run metadata/log, deliberately stopped 6ddf1c6 run and focused failure/correction logs. They remain failed/stopped, never relabelled PASS.
- [python-tests.log](python-tests.log), [review.md](review.md), [bundle.json](bundle.json): regression output, reviewer/executor rulings and hashes of retained files.

The [5990a5c bundle](../local-task22-5990a5c/README.md) remains immutable historical evidence. This bundle supersedes its deferred concurrent-rejoin scope. Later documentation commits are not claimed as the source tested in this run.

## Remaining gates

Loopback TCP/test fault hook and one Docker host do not certify deployed transport, three-AZ behavior, one-AZ/four-survivor recovery, production workflow recovery/RTO, approved identity/PKI/source enrollment, P2/N-1 capacity, deployed security/DR or 24-hour soak. **Tasks 23–24 remain open; production stays NOT_QUALIFIED.**
