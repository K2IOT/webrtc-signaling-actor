# Task 22 concurrent rejoin review and executor rulings

One fresh whole-follow-up read-only reviewer assessed ccd6e0b..150cba1, then reviewed corrective fixture changes. Static concurrent changes approved with zero Critical/Important/Minor findings. Full verification was explicitly pending, never inferred from static approval.

## Corrective review

- Native gateway fixture renewal and same-ID ACCEPT retry: approved, no findings. Native 15-second TTL is unchanged; fixed-delay renewal waits for physical completion, propagates failure, and is joined on close. UNKNOWN renewal stops/fails the fixture rather than issuing a fresh operation.
- Relay retry review found one Important coverage gap: a duplicate ANSWER could remain on the caller channel after its first frame was consumed. Corrected at 92114fe: assert each recipient queue is empty after every validated relay frame, retaining explicit duplicate OFFER receipt equality. Focused Failsafe gate passed at 2026-10-09T19:50:48+07:00. Executor verified this exact assertion closes the reported gap; no additional reviewer was substituted.
- Relay and ACCEPT retries retain exact original command/request ID/hash and obtain fresh native session proofs. Only typed DbOutcomeUnknownException (ACCEPT) or exact OUTCOME_UNKNOWN reply (relay) permits another attempt. Unexpected authorization/other errors fail. Eight attempts and a 15-second attempt-start cutoff are bounded local fixture behavior; an in-flight attempt/cleanup can finish after that cutoff. Physical completion is awaited in finally before retry.

## Declined behavior and executor rulings

- Full gate: reviewer did not run tests or judge the unfinished collector. Executor must require original full PASS with unchanged source fingerprint before completion.
- Production business recovery and <=30-second RTO: local PingCall/native authority plus 60-second fault deadline do not qualify workflow hydration, participant validation or deadline recovery. Leave production NOT_QUALIFIED.
- One-AZ/four-survivor availability, simultaneous P2 work/capacity: six-member fixture isolates one member, leaving five. Tasks 23–24 still require broader topology/load qualification.
- Deployed transport/discovery/three-AZ: local TCP/test hook and same-host systems do not qualify production mTLS/Kubernetes/physical AZ faults.
- Independent unknown COMMIT, ICE ordering and revocation contracts: full suite includes existing tests; unchanged production implementations are not independently recertified by this follow-up.
- Uninterrupted delivery/automatic mutation replay: spec48 is at-most-once with bounded same-identity reconciliation. New rejoin helper retries pure same-call reads, not mutation replay. Composition fixture retries do not implement deployed client retry ownership.
- Production dedup and gateway UNKNOWN reconciliation: review identified missing test coverage, not proven runtime duplication; fixture correction does not certify production schedulers.

## Implementation rulings and costs

- Both takeover-first and immediate same-address fresh-UID restart schedules are required; concurrent schedule asserts unchanged native epoch immediately before restart. Cost: additional real cluster test time.
- Preserve native quorum (majority-min-cap 5/write-plus 3/read-plus 5) using six members/five survivors, not smaller quorums. Cost: extra local ActorSystems and resource use.
- Treat lost cached shard-home read as UNKNOWN, retry only AskTimeout using the same CallId, max 20 reads per stage with <=2-second asks under one fault-relative 60-second deadline. Pinned ShardRegion.scala flushes buffered messages before terminated home cache removal. Cost: local availability is bounded, not uninterrupted or production RTO qualified.
- Require higher native epoch, fenced stale pulse and exact replacement token after idempotent stale release. Cost: extra authoritative SQL probes; boolean stale-release success is not mutation success.
- Original 150cba1 full gate FAILED NativeActorCompositionIT UNAUTHORIZED; focused original fixture repro expired its never-renewed 15-second gateway lease. Repair native test renewal, preserve TTL/fencing. Additional focused UNKNOWN outcomes required bounded same-ID fixture retry; keep all failed logs without relabeling them.
- A full run on 6ddf1c6 was deliberately stopped when review found missing duplicate-ANSWER assertion. It is not PASS evidence.
- Commit immutable code checkpoints before full gate; deliver docs/evidence afterward with verified SHA explicit. Cost: multiple local commits; docs commit is not claimed as tested source.
- Reuse existing branch and keep ignored whole-plan scratch because Tasks 23–24 remain pending. No push/deployment or production qualification is authorized by this Task22 fix.

## Final executor resolution

The original full clean collector on clean source 92114fe0b8ab63f3b614adc685ab5951958c6832 finished 2026-10-09T20:08:21+07:00:759 tests/166 reports, zero failures/errors/skips, six fresh fault receipts, unchanged fingerprint, LOCAL_SUITE_PASSED. The pending full verification condition is now met. The one Important duplicate-ANSWER finding was fixed by the per-recipient empty-queue assertion and passed both focused and full gates. All production/topology/RTO declines remain unqualified.
