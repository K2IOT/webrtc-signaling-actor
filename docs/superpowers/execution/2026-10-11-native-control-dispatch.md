# Native control pre-call dispatch — source contract

## Scope and qualification

Round 2 changes only `CellRpcServer`, `GatewayRelayRpcServer`, and new `NativeRpcDispatch`.
This is a source implementation and pinned bytecode/source audit. No tests or builds were run by
this worker. Relay saturation, transport cancellation, and upgrade behavior remain runtime gates.

## Why the original supplier was insufficient

The pinned grpc-core 1.75.0 `ServerImpl.streamCreatedInternal` submits `MethodLookup` to the default
executor before invoking `ServerCallExecutorSupplier`. Its separate `ContextCloser` also uses that
default executor. A shared relay default executor therefore blocks CONTROL before the body executor
selection. Merely selecting a control executor in the supplier does not close this gap.

Source: [ServerImpl.java, grpc-java v1.75.0](https://github.com/grpc/grpc-java/blob/v1.75.0/core/src/main/java/io/grpc/internal/ServerImpl.java).

## Supported registrations and pinned schedule audit

Both servers register the helper through public `ServerBuilder.executor(Executor)` and
`addStreamTracerFactory(ServerStreamTracer.Factory)` APIs. Their existing public `callExecutor`
supplier remains responsible for protobuf/business callbacks, authentication, telemetry, and backend
invocation. No work runs inline on a transport thread.

The source audit establishes these synchronous boundaries:

1. `ServerStreamTracer.filterContext` runs inside `streamCreatedInternal`, before submitting the
   initial `SerializingExecutor`. Method lookup then installs the lane body executor before
   `HandleServerCall`, request parsing, interceptors, or business work run.
2. `AbstractServerStream.TransportState.closeListener` invokes `StatsTraceContext.streamClosed`
   before `JumpToApplicationThreadServerStreamListener.closedInternal` submits `ContextCloser`.
   Cancellation notifications are therefore attributed separately and executed off the event loop.

Source: [AbstractServerStream.java, grpc-java v1.75.0](https://github.com/grpc/grpc-java/blob/v1.75.0/core/src/main/java/io/grpc/internal/AbstractServerStream.java).

The helper uses only the public registration APIs, but its attribution contract deliberately depends
on these pinned internal scheduling shapes. It requires the grpc-core manifest version `1.75.0` at
construction. The task class name, submitting stack, and synchronous scope must match that audited
shape; otherwise work uses the bounded unclassified path. This contract requires a new source audit
before upgrading grpc-core, rather than silently assuming the schedule remains compatible.

## Bounds and lane isolation

Each server owns independent CONTROL and RELAY pools for both LOOKUP and CANCEL: two platform
threads and 128 queued tasks for each lane/phase. Unclassified work has separate one-thread,
32-task pools for each phase. Existing body pools remain independent. Relay body, lookup, and
cancellation queues therefore cannot consume CONTROL pool threads or queue entries.

Attribution markers use semaphore-bounded maps: 128 markers each for CONTROL and RELAY, and 32 for
unclassified work. Keys contain only thread identity, phase, and nested synchronous scope depth;
values contain only expiry time. Each submission consumes its matching marker exactly once. Nested
callbacks use distinct scope keys; sibling callbacks replace only their own scope. Deep or unmatched
stacks use the unclassified path. A fixed 100 ms cleanup timer expires orphaned markers after two
seconds, including exceptions between tracing and submission. There is no ThreadLocal state and no
retained request, route, context, header, or payload in this registry.

Pool or marker exhaustion supplies no receipt authority. Queue admission rejects rather than
running caller business/cancellation work inline. Unclassified scheduling and marker exhaustion are
bounded degradation paths, not production saturation qualification.

## Original physical ownership and drain

Neither marker expiry nor a request deadline releases queued/running executor work. Drain first
joins existing RPC admission and actual server transport termination, then shuts down and joins all
six pre-call producer pools and the marker timer while body consumers remain open. Only then does it
shut down and join the original body pools. Construction without a started
listener also joins these owners. Failed or timed-out termination remains unproven and fails drain;
it never synthesizes physical completion or an application receipt.

## Round 3: producer-before-consumer shutdown

The scoped round 2 review accepted the original dispatch isolation fix and identified a MEDIUM
shutdown handoff race. A queued `MethodLookup` switches its `SerializingExecutor` and submits
remaining `HandleServerCall`/closed callbacks to a body pool. Closing body pools before joining
pre-call producers can reject and abandon that handoff even after the transport has terminated.

Both servers now preserve this order under the same total two-second post-admission deadline:

1. Await original transport termination.
2. Shut down and join all pre-call producer pools and the marker timer, keeping body pools open.
3. Shut down and join both body pools with the remaining deadline.
4. Report successful drain only after all actual owners terminate.

If producer termination cannot be proven, drain fails and destination body pools remain open for
still-owned handoffs. Marker shutdown removes attribution records and restores their semaphore
permits immediately, even when a subsequent join fails. A concurrent marker insertion rechecks
shutdown and removes only its own record; conditional removal prevents double permit release.
This metadata cleanup never cancels or completes queued/running work and grants no receipt authority.

No tests or builds were run by this worker. Source formatting and diff whitespace checking are
separate from pending final compilation, fresh review, queued-handoff shutdown execution, and runtime
saturation qualification.
