# Final fairness scoped rereview

Reviewed 2026-10-12 using `final-fix-delta.diff`, `final-fix-report.md`, current source and the affected native callers. Scope is F1/F2 from `final-review.md` and breakage introduced by this two-file fix wave. **Verdict: source accepted — both HIGH findings addressed; no new concrete CRITICAL/HIGH/MEDIUM defect established in this delta.** This supersedes the two open findings in the broad review, while retaining its external and unrun acceptance boundaries.

## F1 — addressed

`CallRecoveryService.java:60-64` retains fixed 1024-element cursor and upper-bound arrays plus a fixed completion bitset and group turn. This stays within the schema's 0..1023 group domain. Hash version remains in the SQL ordering/boundary rather than being discarded by the array index.

At `:324-326`, a new hint refused by the global or per-group wake quota stops the current group before that hint counts as processed. `:272-275` advances only through the preceding accepted/already-owned prefix and retains the group's upper boundary. In the original five-call scenario, the cursor therefore remains at call four and call five is first after capacity returns; the same first four cannot reset and reacquire the whole group quota ahead of it.

`pages` rotates to another selected group (`:190-195`) after every bounded query, including a group that could not dispatch more work. The global-full branch (`:188-189`) yields before advancing the next group turn, preventing later groups from losing their turn repeatedly to the global prefix. Empty, terminalized and ownership-filtered pages can complete without inventing actor authority. Partition changes retain finite positions and eventual wrap; each SQL page still validates current cell/ownership and each wake still follows existing fresh actor/call authority.

The scanner does not wait for pending wakes to finish. Existing four-per-group/128-global owners, reconciliation, original physical receipts and drain remain intact. Passing an already owned call does not submit a duplicate. There is no per-call retry backlog. The invocation remains bounded to 32 pages, at most 512 hints per query and the original two-second budget; small partitions can receive multiple visits within that budget.

This closes the repeated-prefix starvation identified in F1. It does not promise progress while all 128 original physical owners remain unresolved, nor does it treat an index visit as a successful hydration. Per-group query overhead, quota-limited revisits, placement skew, competing RECOVERY work and real wake latency still require the full 60k/P2 oldest-age measurement. The old one-512-row-page-per-second restriction remains removed. No measured five-second sweep, hydration rate or fault recovery target is asserted.

## F2 — addressed

`OutboxRepository.java:134-140` divides one requested candidate budget between durable repair and committed hints before querying. With continuous hints and even native limit 128, each gets 64 candidate slots. The alternating odd slot gives both sources turns at legal limit 1. Without hints, the durable scan receives the full limit. The two queries can contribute at most `limit` distinct identities in total; duplicates, stale hints and excluded original deliveries only reduce that number.

At `:178-190`, the claim orders by durable `(next_attempt,event_id)` with no preferred-first tier and retains a limit at least as large as the already bounded candidate union. Consequently fresh hints cannot displace an eligible durable candidate merely by taking every claim slot. The original sustained-128-hints scenario now leaves a guaranteed durable repair share, and candidate/bucket work no longer grows to 256 identities for a requested 128.

The existing frozen-ceiling keyset cursor/CAS wrap, sorted bucket barriers, rollback on failed barrier, fresh eligibility, SKIP LOCKED, dispatch generation/incarnation, one-MiB payload check and physical/application-receipt ownership are preserved. Dispatcher/OutboxHints APIs did not change. Dequeued hints beyond the selected share fall back to durable repair, consistent with hints being lossy acceleration; no durable event is deleted by truncating that ephemeral share. Progress still depends on eventual valid authority and available locks, as before.

## Evidence and retained limits

Only CallRecoveryService and OutboxRepository changed in the supplied wave. Reviewed exact before/after delta, current numbered source, unchanged handoff/physical release, native scheduler/partition caller, dispatcher hint consumption and durable cursor/claim helpers. No tests, builds, benchmarks, source edits, deployments, commits or spawns were performed by this reviewer.

Parent reports production compile12 PASS: 12 modules, 17.620 s, finished `2026-10-12T05:56:57+07:00`, `/tmp/webrtc-audit-remediation-compile-12.log`; no test sources compiled or tests run. This review does not independently reproduce that build.

The broad review's source acceptances now have no remaining concrete reported source finding. Its external identity/client/provider enrollment, authenticated compatibility collectors, WAL/fault-cohort/media observations, PostgreSQL/query-plan execution, native browser integration, saturation/drain/rollback/fault drills and production qualification remain open. Source acceptance does not close those gates.
