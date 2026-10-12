# Accounting P1 fixes — inline source review

User invoked Superpowers review/fix; review reception used source verification before edits. Work remains inline without subagents and under the retained no-test-compilation/execution constraint. GitNexus upstream impact attempted on both new classes: UNKNOWN/unindexed. Direct caller review: explicit native enrollment bean, native sink callbacks, capture/fault operator API; no public enrollment route. Only NativeRecoveryAccounting and DurableRecoveryAccounting behavior changed in this continuation; runbook updated.

## Fixes and reviewed boundaries

- Workflow convergence has explicit30s in durable deadline SQL and producer admission. Media remains30s; PROCESS/AZ reattachment60s/180s. Active-preservation60s is an observation window, not a claimed product qualification target. Fault timestamp retries do not move deadlines. A convergence observation after30s cannot enter the queue or win durable success.
- Membership is bounded to64 immutable trusted manifests, each<=128calls. Exact call, SLI eligibility, surviving participant incarnation and original fault window are checked before queue admission. Unrelated traffic returns before queue accounting or loss. No queue size increase, sampling, eviction or fabricated denominator.
- Capture membership publishes only after successful SQL logical COMMIT plus original physical completion. Repeated concurrent capture/fault admissions reuse the original operation while its logical/physical completion is unresolved. Immutable UUID/manifest/timestamp mismatches fence before reuse.
- Fault scope arms before SQL submission, retaining genuine concurrent observations; processing waits for fault resolution and processes only proven-started scopes. Failed/unknown starts remain loss/UNKNOWN. Ready cohorts bypass waiting cohorts, including splitting one queue item into ready/waiting target sets within the original32-entry bound.
- Each queued item carries its immutable cohort target set. Observation SQL is restricted to those IDs and still revalidates cell/epoch/group/grant/current call, original version/round, participant/full-route facts and fixed deadlines. In-flight items never attach to a subsequently created cohort.
- Queue saturation, individual logical/physical observation failure and shutdown loss retain only related cohort IDs. Generic unavailable callbacks conservatively affect only currently armed windows. Totals worker failure invalidates metric freshness instead of creating durable loss for unrelated/future cohorts.
- Loss SQL updates only named cohorts in matching storage epoch/privacy binding with unresolved members. Fully successful cohorts are not poisoned by redundant late loss. Pending loss before fault resolution is retained, not acknowledged prematurely. A successful physical totals transaction acknowledges its loss snapshot; genuine historical UNKNOWN stays durable and therefore still affects the cumulative aggregate. Future cohort rows are not marked UNKNOWN from an old loss.
- Counts, queue, original physical-owner and journal limits remain64/128/32/16 and8192members; unresolved physical owners remain retained through drain. Raw manifests exist only in bounded volatile provider enrollment; durable journal retains HMAC digests only. After restart the provider must replay exact manifests/fault timestamps; source gaps remain UNKNOWN, not reconstructed from hashes.

Scoped inline source review accepts the two P1 remedies. This is implementer review, not independent review. Runtime SQL concurrency, restart replay, fault/queue load and P2 acceptance remain unrun. External complete fault providers, privacy/retention policy, deployed client/server receipt versions and production evidence remain open. No commit/push/deploy.

## Fresh final validation

- Root spotless:apply PASS4.684s,13:48:12+07:00.
- Root compile with maven.test.skip=true PASS all12modules22.464s,13:48:37+07:00.
- Root spotless:check PASS3.170s,13:48:43+07:00.
- Browser3modules node --check PASS; git diff --check PASS.
- GitNexus detect_changes(all):624symbols/209flows/99trackedfiles,CRITICAL; compare(main):6614symbols/298flows/530files,CRITICAL. Includes prior implementation/formatting/user edits; new accounting files remain unindexed and were reviewed directly. No graph-backed zero-impact claim.

Logs: /tmp/webrtc-accounting-accepted-format.log, /tmp/webrtc-accounting-accepted-compile.log, /tmp/webrtc-accounting-accepted-spotless.log. Earlier passing compile logs predate final race/loss refinements; the accepted logs cover final Java source.
