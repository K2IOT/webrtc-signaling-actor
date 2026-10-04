# PostgreSQL authority failover

Topology: CloudNativePG **1.27.0**, PostgreSQL **17.6**, exact amd64 digests in `config/compatibility-manifest.yaml`, three instances in three AZs; one synchronous standby in another AZ, `synchronous_commit=on`, `postgresql.synchronous.dataDurability=required`. The directory is a separate native cluster with the same durability requirement. Local transaction tests are not an HA certification.

1. Close new authority admission on primary uncertainty, lost synchronous durability, clock uncertainty or fencing/source lag. Existing authorized relay/media may continue only inside original bounded windows. Collect the old writer system identity/timeline, synchronous standby identity/AZ, acknowledged commit/WAL watermark and candidate/config/topology.
2. Prove the promotion candidate contains **all acknowledged WAL** using the durable failover witness; do not infer this from a healthy pod or a replay lag gauge alone. Check `pg_stat_replication.sync_state`, `synchronous_standby_names`, `synchronous_commit` and eligible cross-AZ standbys on the native writer. Do not switch to asynchronous/remote_write durability.
3. Physically fence the old writer using the deployment's enrolled cloud/hardware fencing system. Retain its authenticated receipt bound to cluster/system identity, old writer, storage epoch, fence operation and target timeline. A shutdown request, DNS change, Kubernetes annotation or CNPG status is not physical fencing proof. Loss of receipt keeps authority closed.
4. Supervise promotion using the pinned controller and approved candidate. Recheck current native writer identity/timeline, acknowledged-WAL containment, cross-AZ synchronous standby, clock bounds and fresh authenticated fencing proof before opening authority. Reconcile uncertain original operation IDs; never automatically replay a call start with a new ID after COMMIT/ACK uncertainty.
5. Record fault start, admission close, physical fence, promotion, first durable native recovery, first successful control and original-operation reconciliation. Measure separately infrastructure readiness, first-cold-request latency, workflow convergence, active-call preservation and media interruption. No lease/deadline extensions for scoring.

Read-only diagnosis (use the enrolled mTLS PostgreSQL connection and approved secret handling):

```sql
SELECT pg_is_in_recovery(), current_setting('synchronous_commit'), current_setting('synchronous_standby_names');
SELECT application_name, state, sync_state, flush_lsn, replay_lsn FROM pg_stat_replication;
```

No automatic failover action is performed by local qualification scripts. A real one-AZ/primary-failure drill and fencing mechanism evidence are mandatory release inputs.
