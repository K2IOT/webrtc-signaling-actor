# Task 1 source implementation report

Status: source-ready for independent review and root production compilation. No tests, test edits, builds, benchmarks, commits, pushes or deployments performed by this worker.

## Owned changes

- `signaling-protocol/src/main/proto/signaling_internal.proto`: appended read-only `CompatibilityObservationIngress` and request/reply/descriptor messages; no existing wire fields changed.
- `signaling-cell-rpc/src/main/java/io/webrtc/signaling/rpc/CellRpcClient.java`, `CellRpcServer.java`, `GatewayRelayRpcClient.java`, `GatewayRelayRpcServer.java`: authenticated observation endpoint/client methods, destination/nonce/current native gateway boot binding, CONTROL admission, original `Flight` physical completion ownership; observation service added to accepted pre-call dispatch CONTROL catalog.
- New RPC `CompatibilityObservationService.java`, `CompatibilityObservationValidation.java`.
- New runtime `NativeCompatibilityObservationEnrollment.java`, `NativeCompatibilityObservationProducer.java`, `NativeCompatibilityCollectors.java`, `NativeCompatibilityObservationConfiguration.java`, `NativeCompatibilityObservationLifecycle.java`, `NativeRollbackCollector.java`.
- Runtime `NativeCompatibility.java`: local schema snapshot accessor and stable UUID assigned when feature admission is disabled. Existing gates retained.
- App `META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports`: appended observation auto-configuration only, preserving other workers' entries.

## Concrete execution

Read-only observations require existing native mTLS environment/role authentication and the client's actor-cell or gateway-workload certificate binding. Gateway request and response bind the server's immutable constructor boot. Client request nonce is echoed and checked. No business capability gate is consulted by these observations. No public mutation/admin endpoint was added.

Actor producers read actual authenticated reachable Up Pekko members and their enrolled version/executable/fingerprint/features/AZ roles. Invalid or incomplete descriptors deny; no executable version is hardcoded. Schema producer uses the existing tracked writer query and installed V016 resource checksum. RPC replies retain the original schema expiry separately from membership expiry.

Explicit `NativeCompatibilityObservationEnrollment` supplies the actual installed build descriptor, approved adjacent release window and topology. Bounds are 50 cells, seven actors per cell, 128 immutable gateway boots, 16KiB replies and five-second freshness. A fixed polling job retains all original native RPC physical stages, removes failed target snapshots and never starts another invocation while an original remains physically unresolved. Native local gateway facts also populate the gateway's own boot map without a capability bootstrap cycle. Actor ingress's existing gateway client is used when no standalone gateway-client bean exists.

An independent Spring SmartLifecycle wraps its own bounded NativeWorkerScheduler; it is not a NativeWorkerScheduler bean, avoiding ambiguity with the actor's single lifecycle-owned scheduler. Membership, polling, optional schema and optional rollback jobs start without business readiness. Stop completion only follows actual worker physical completion; transport and SQL owners retain their existing lifecycle responsibility.

## Rollback semantics and enrollment

`NativeRollbackCollector.Enrollment` requires explicit source/target executable hashes, exact retained migration checksums, full process and permanent physical-owner rosters, and real writer cell/storage epochs. Map keys must exactly match the approved policy; absent policy does not install a successful fact producer.

Trusted native operator `beginFence()` disables each enrolled process gate, captures its stable admission UUID, and invokes every original owner's permanent drain. These must be actual original owner drains (actor/shard work, maintenance, RPC, DB and external writers), not sampled busy counters or replacement futures. Every drain must settle successfully under the original fence before reads qualify. Observation SQL requires a separately retained observation owner, because business DB owners must be permanently fenced/drained.

Writer inspection runs tracked RECOVERY transactions, binds actual writer cell/epoch/non-recovery state, obtains the native exclusive advisory transaction barrier, checks complete Flyway checksums, inspects actual root relation catalog, and executes bounded EXISTS probes over the fixed durable-work catalog. There is no full-table count/scan and no dynamic policy-supplied SQL identifier. Any actual durable row lacks executable provenance and produces UNKNOWN rather than zero. Unknown/new tables, including the separately reserved Task4 V017 table until a future explicit provenance policy exists, also remain UNKNOWN. Zero is emitted only when the complete known durable catalog is actually empty; schema retention must independently match the approved target checksum policy.

Roster completeness and target artifact/checksum policy remain trusted operator enrollment inputs. No remote fleet admission-fence control protocol, cloud provider fence, target-build provenance, successful deployment or qualification was invented. Without actual complete process/owner handles and target policy, rollback stays unavailable. No V017 changes were made.

## Source review and impact evidence

GitNexus prior to existing edits: CellRpcServer LOW (3 direct dependents, actor runtime hooks flow), GatewayRelayRpcServer LOW (3), CellRpcClient MEDIUM (8, actor runtime hooks flow), GatewayRelayRpcClient LOW (1). Both server `start` methods LOW. NativeCompatibility and disableFeatureAdmission returned UNKNOWN/not indexed; direct source and caller/configuration searches were used and root was informed. No HIGH/CRITICAL result occurred. No commit was made, so no commit scope check was invoked.

Self-review traced server interceptor -> read-only service -> producer and client Flight -> physical terminal callback -> collector -> compatibility gates. Confirmed native gateway self-bootstrap, writer schema expiry retention, original physical ownership across logical timeout, bounded fixed topology, and UNKNOWN for nonempty/new durable schemas. Root compilation and independent Java/security review remain required. Runtime mTLS, multi-cell/boot rollout, shutdown faults, rollback fleet exercise and all acceptance tests remain unrun.

## Independent review fix round 1

Addressed the HIGH observation executor finding in `task1-review.md`: `GatewayRelayRpcServer.start` now selects relay `cpu` only when the full method name exactly equals generated `GatewayRelayIngressGrpc.getDeliverRelayMethod()`. All other registered methods, including compatibility observations and gateway control delivery, select reserved `controlCpu`. The bounded pre-call dispatcher, executor capacities, mTLS checks, admission and original physical owners remain unchanged.

Pre-edit GitNexus: anonymous `getExecutor` UNKNOWN/not indexed; enclosing `start` LOW, with direct `NativeGatewayIngress` constructor and indirect `nativeGatewayIngress` configuration, no indexed affected execution flows. Root received the risk and HIGH finding before mutation. Direct source inspection confirmed the generated relay descriptor comparison and CONTROL fallback. No tests, builds or commits performed. Independent review recheck remains required.
