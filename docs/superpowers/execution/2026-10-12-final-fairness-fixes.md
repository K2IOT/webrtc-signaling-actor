# Final broad-review fairness fix wave

2026-10-12. Addresses F1/F2 from `final-review.md`; source delivery pending independent scoped review. Only `CallRecoveryService.java` and `OutboxRepository.java` changed in this wave. Existing dirty remediation work remains in place. No tests, test source edits, builds, benchmarks, commits or pushes were performed by this worker.

## Pre-edit impact

GitNexus upstream impact ran for both classes and every edited existing method. CallRecoveryService: class LOW with no indexed callers; dispatch LOW, direct sweep and six indexed sweep flows (acquisition, release, overload, outcome unknown, safety, retry). OutboxRepository: LOW, three direct class consumers (CallCommandService, CallWorkflowService, OutboxDispatcher), native composition transitively. `pages`, `sweepTracked`, `scanOutboxBatchTracked` were UNKNOWN/not found in the stale index; direct source confirms the recovery service's own sweep/pages chain and repository claim adapter plus OutboxDispatcher.pollTracked. Root received these results before edits. No HIGH/CRITICAL impact result was returned. The original review findings themselves remain HIGH until independent acceptance.

## F1: bounded retained group continuation

Recovery now has fixed arrays of 1024 call cursors and 1024 frozen upper boundaries, a 1024-bit completion set, and one rotating group position. V001's `group_number` domain is 0..1023; the existing partition validation rejects values outside that supported domain. Hash version stays in each ordered SQL key and frozen upper boundary, so different positive hash versions do not require an unbounded map or silently overwrite distinct version positions. Existing fresh cell and native ownership/actor authority checks remain authoritative.

Each page selects the next eligible group and rotates to another group. Within that group, the first locally quota-refused hint stops cursor advancement: the persisted keyset position remains immediately before it. Already physically owned calls can be traversed without a duplicate wake. All previously admitted wake owners, reconciliation, physical cleanup, drain and authority rules remain unchanged.

For the review's five-call example, the first four admissions leave the cursor at call four; call five is first on the next visit with available capacity. Completing the prefix cannot reset this cursor. At the global 128 cap, pages yield immediately without waiting for wake completion or retaining scanner SQL ownership, and preserve the next group's turn; after capacity returns, the earlier groups cannot always restart first. A saturated group rotates away so other groups retain service. Unresolved originals are never discarded to fabricate capacity; all-global UNKNOWN saturation necessarily prevents new wake admissions until real capacity is available.

Limits remain 32 SQL pages of at most 512 hints per invocation, two seconds, four retained wakes per group and 128 globally. Small partitions can be visited repeatedly within the existing invocation budget, including newly settled groups; there is no one-visit-per-group cap. Group completion requires actual traversal past the accepted/owned prefix, and an overall cycle requires all selected groups to complete. Refused hints are represented by positions, not an unbounded per-call backlog.

Self-review covered same-group suffixes across reset, >32 groups competing for global credits, permanently pending wakes in one group, all-global saturation, disappearing/terminal/nonowned rows, varying partitions, multiple hash versions, immediate completions and end-of-budget partial pages. SQL failures preserve the affected group's call position while rotation permits other groups to progress.

## F2: one shared candidate budget

The repository allocates the requested 1..128 candidates between durable repair and committed hints before either query. With hints present, each receives floor(limit/2), and an atomic alternating turn assigns the odd slot. Limit 1 therefore alternates durable and hint turns. Without hints, durable repair receives the full limit.

Only the hint share is passed to the committed-ID query. The combined candidate map, distinct bucket barriers, claim input and returned count cannot exceed the requested limit (128 maximum), including when the two sources do not overlap. Duplicate candidates reduce work. The final claim query orders by durable next_attempt/event_id, with no strict preferred priority; since it considers at most limit total IDs, every unlocked eligible candidate has room. The existing 1 MiB payload bound and rollback-on-failed-barrier path remain unchanged.

The durable frozen-clock cursor, identity CAS wrap, pre-barrier advancement, guarded authority, SKIP LOCKED, dispatch generation/incarnation, excluded original deliveries, and exact application receipt logic are unchanged. Dispatcher and OutboxHints require no API change. The existing dispatcher can dequeue more ephemeral hints than this transaction's share; surplus hints safely fall back to the guaranteed durable repair path, like existing overflow/lost hints. Post-COMMIT wake signaling remains installed.

Self-review covered sustained 128 fresh hints against an older due event, legal limits 1/2/127/128, no hints, duplicate/stale/excluded hints, frozen and contended buckets, cursor wrap, and transient barrier rollback. Durable progress remains conditional on eventual authority/lock availability; no starvation guarantee is asserted against perpetual conflicting authority ownership.

## Remaining gates

Parent owns production-only compilation and static checks. Independent scoped F1/F2 review is required before broad source acceptance. No SQL/runtime execution, tests, throughput or latency measurements were performed. Per-group SQL query shape, group skew, 60k/P2 oldest age and five-second sweep targets need actual measurement. A nominal cycle is not proof of actor hydration. Historical compile11 or old test results do not validate this source wave.

All prior external acceptance gaps remain: actual identity/client/provider enrollment, authenticated remote compatibility collectors, WAL/fault-cohort/media observations, browser/native integration, PostgreSQL plans and migration/fault drills, production resource profiles and end-to-end receipt/rollback/drain qualification. No fake provider, client or SLI qualification was added.
