# Task 2 scoped rereview — round 1

Verdict: APPROVE the scoped correction. Prior HIGH is addressed; no new concrete breakage found in the correction.

`PostgresWalFacts` now collects the writer's actual `synchronous_standby_names` in both identity samples, retains an immutable parsed policy, and rejects a changed setting/policy. The restricted parser bounds required count and identity list, rejects ambiguous duplicate/case aliases, wildcard/legacy/unsupported grammar, and impossible counts. Empty policy remains explicit local mode only.

`NativeWalSource.usable()` requires exact configured required-ack count and admitted identity-set equality before considering available members. ANY requires quorum candidates; FIRST requires exactly the configured number of sync members and potential remaining members. The original reproducer, `ANY 1 (a,b)` with enrollment requiring two acknowledgements, now fails the required-count predicate regardless of candidate count or observed lag.

The correction preserves the existing byte/freshness/role/epoch predicates and original physical completion/drain ownership. Readiness remains an observation of current policy and replica availability, rather than a durability receipt for a particular commit.

Scope: current `PostgresWalFacts`, `NativeWalSource`, and appended round-1 implementation report only. No code edits, tests, builds, database probes, deployment or runtime qualification performed. Earlier documented external integration and unrun qualification boundaries remain.
