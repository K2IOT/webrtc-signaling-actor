# Pekko native shutdown alternative

Status: `PINNED_CANDIDATE_UNQUALIFIED`. This milestone dependency alternative
does not approve deployment, mixed-version operation or production capacity.

Pekko 1.1.3 can leave the original ActorSystem termination receipt pending after
all application and cluster shutdown phases have run. A native six-node TLS
test forces the original inbound-control completion notification to reach
SystemMessageDelivery before the transport abort. Its snapshot shows inbound
completion, five pending outbound control completions and a terminating system
guardian. The unchanged 65s retirement wait fails. Control database ownership
and the original process receipt are not the cause.

Pinned upstream source explains this ordering: InboundControlJunction.postStop
reports Success(Done), SystemMessageDelivery completes its downstream on that
notification, and Artery's TLS connection uses IgnoreComplete. Transport
shutdown then waits indefinitely for the outbound stream completion. Inspected
stable source releases through 1.7.1 retain this behavior.

[Pekko 2.0.0-M4 ArteryTransport](https://github.com/apache/pekko/blob/v2.0.0-M4/remote/src/main/scala/org/apache/pekko/remote/artery/ArteryTransport.scala)
provides a supported shutdown-streams-timeout before transport unbind, with an
[upstream stalled-stream test](https://github.com/apache/pekko/blob/v2.0.0-M4/remote/src/test/scala/org/apache/pekko/remote/artery/ArteryTransportShutdownSpec.scala).
The application still joins its original getWhenTerminated receipt: timeout
initiates native teardown and is not accepted as proof of physical retirement.

The candidate pins the core BOM to 2.0.0-M4 and Management/bootstrap/Kubernetes
discovery and HTTP to 2.0.0-M1. Dependency-tree verification confirms core
cluster, discovery, stream, remoting, PKI and testkits all resolve to M4.
Independent property overrides are insufficient because Management transitives
can select core M1. Management 1.1.1 also references removed core classes and
cannot remain with core M4.

Artery flush and stream waits are each explicitly 1s, positive and capped at 1s
by production configuration validation. The original 5s termination phase and
65s graph are unchanged. Test JVMs, the separate native Main fixture and the
container provide Agrona's required
`--add-exports=java.base/jdk.internal.misc=ALL-UNNAMED`. Local executable launches
outside that container need the same option:

```sh
java --add-exports=java.base/jdk.internal.misc=ALL-UNNAMED -jar app.jar
```

This is a major-version milestone migration. The
[upstream migration guide](https://github.com/apache/pekko/blob/v2.0.0-M4/docs/src/main/paradox/migration/migration-guide-1.x-2.x.md)
declares binary incompatibility. A 1.1.3-to-M4 rolling or mixed-fleet upgrade is
not qualified. The default M4 TCP magic sends PEKK, which 1.1.3 does not accept;
no application framing fallback is supplied. Release requires a reviewed
framework migration and measured drained/fenced cluster replacement, or a
stable upstream backport followed by all affected gates. Native tests cannot
approve that migration by themselves.

M4 also releases the framework lease from Shard.postStop. Existing placement
observation and root drain can therefore request release concurrently. Release
must preserve the original pending SQL receipt, physical cleanup and deadline;
lease, native sharding and original root-drain tests remain required.

Local reproduction and candidate test outputs are recorded in the inline ledger
and tracked execution checkpoint. Historical OCI/evidence candidates remain
immutable and do not cover this dependency alternative. Tasks22–24 remain
IN_PROGRESS/NOT_QUALIFIED until their native and external qualification
contracts are satisfied.
