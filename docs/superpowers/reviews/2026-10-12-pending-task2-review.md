# Task 2 independent source review

Verdict: BLOCK — one HIGH correctness finding.

## HIGH: quorum candidates are treated as required durable acknowledgers

Location: `signaling-app/src/main/java/io/webrtc/signaling/app/runtime/NativeWalSource.java:35-37`; collection in `signaling-storage/src/main/java/io/webrtc/signaling/storage/PostgresWalFacts.java:35-37`.

`usable()` counts every `sync_state=quorum` connection as a synchronous replica. PostgreSQL's quorum state denotes a candidate for quorum replication; it does not establish the configured number of acknowledgements required for commit. For example, writer configuration `ANY 1 (a,b)` reports both candidates as quorum. Enrollment requiring two synchronous replicas, with both candidates streaming and within the byte/freshness bound, therefore passes even though commits require only one standby acknowledgement. Zero observed lag during a quiet poll cannot repair that policy mismatch for later commits.

Collect and validate the actual writer synchronous acknowledgement policy alongside both identity samples. Bind its mode, required count, and admitted identities to enrollment; reject unsupported/unparsed/changing policies. Alternatively deny quorum mode until that proof exists. Preserve the independent observed byte lag bound and do not reinterpret candidate count as durable acknowledgement count.

Primary evidence: [PG17 replication statistics](https://www.postgresql.org/docs/17/monitoring-stats.html#MONITORING-PG-STAT-REPLICATION-VIEW) and [PG17 synchronous_standby_names](https://www.postgresql.org/docs/17/runtime-config-replication.html#GUC-SYNCHRONOUS-STANDBY-NAMES).

## Reviewed boundaries

- Original `SqlTransactions.submitTracked` physical completion is retained through logical timeout/failure. Exceptional physical completion does not retire source ownership; source drain remains unresolved. Only one admitted source poll can remain active.
- SQL replica projection is bounded to nine rows with overflow rejection; immutable observations retain start-based monotonic freshness. Writer role/cell/epoch/schema/system identity and actual flush-position filename timeline are sampled twice. Missing/null/partial/stale observations deny readiness; checkpoint timeline is not used.
- WAL policy has a separate live safety/readiness gate. Missing enrollment closes that gate; explicit local mode requires the local-only profile. Auto-configuration import and pre-scheduling binding are present.
- Mounted production loading has no local loader fallback. It enforces a single plane, production identity, bounded mounted files, real-path containment, exact digest references and strict JSON parsing. Opaque provider content/signature/expiry validation remains an explicitly documented external integration boundary.
- Renderer emits Secret references and provider-specific credential shapes, preserving render-only status. It does not manufacture physical fence, witness, restore or installation evidence.

Read source, Task2 plan/report, contract documentation and relevant owner/scheduler call paths. No code edits, tests, builds, database probes, deployment, or runtime qualification were performed. Direct standby replay/timeline proof and provider-specific evidence activation remain documented incomplete boundaries and must not be represented as completed production integrations.
