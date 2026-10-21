package io.webrtc.signaling.rpc;

import io.grpc.*;
import io.grpc.netty.NettyServerBuilder;
import io.grpc.stub.StreamObserver;
import io.netty.handler.ssl.SslContext;
import io.webrtc.signaling.observability.OperationObservation;
import io.webrtc.signaling.observability.SignalingMetrics;
import io.webrtc.signaling.protocol.RuntimeCapabilities;
import io.webrtc.signaling.protocol.internal.*;

import javax.net.ssl.SSLSession;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.function.Function;

/**
 * Authenticated direct destination ingress; no caller-supplied peer identity is trusted.
 */
public final class CellRpcServer implements AutoCloseable {
    private static final Context.Key<Peer> PEER = Context.key("authenticated-cell-peer");
    private final String cell, environment;
    private final int requestedPort;
    private final SslContext tls;
    private final RpcAdmission admission;
    private final Backend backend;
    private final Function<ControlEvent, CompletionStage<InternalReply>> deliver;
    private final NativeRpcDispatch preCall =
            new NativeRpcDispatch(
                    "cell-rpc-dispatch",
                    Set.of(CellIngressGrpc.getRelayNegotiationMethod().getFullMethodName()),
                    CellRpcAuthorization.SERVICES);
    private final ExecutorService executor =
            new ThreadPoolExecutor(
                    4,
                    4,
                    0,
                    TimeUnit.SECONDS,
                    new ArrayBlockingQueue<>(256),
                    Thread.ofPlatform().daemon().name("cell-rpc-cpu-", 0).factory(),
                    new ThreadPoolExecutor.AbortPolicy());
    private final ExecutorService controlExecutor =
            new ThreadPoolExecutor(
                    4,
                    4,
                    0,
                    TimeUnit.SECONDS,
                    new ArrayBlockingQueue<>(256),
                    Thread.ofPlatform().daemon().name("cell-rpc-control-cpu-", 0).factory(),
                    new ThreadPoolExecutor.AbortPolicy());
    private final CompletableFuture<Void> drained = new CompletableFuture<>();
    private Server server;
    private NativeSessionHandler sessions;
    private volatile RuntimeCapabilities capabilities = RuntimeCapabilities.DENY;
    private volatile java.util.function.Supplier<CompatibilityObservationReply> compatibilityFacts =
            () -> null;
    private volatile OperationObservation observations = OperationObservation.NONE;
    private NativeControlRouteResolver controlRoutes;
    private boolean draining;

    public CellRpcServer(
            String cell,
            String environment,
            int port,
            SslContext tls,
            RpcAdmission admission,
            Backend backend,
            Function<ControlEvent, CompletionStage<InternalReply>> deliver) {
        if (cell == null
                || !cell.matches("[a-z][a-z0-9-]{0,23}")
                || environment == null
                || !environment.matches("[a-z0-9-]{1,32}")
                || port < 0
                || port > 65535) throw new IllegalArgumentException("Invalid RPC listener identity");
        this.cell = cell;
        this.environment = environment;
        requestedPort = port;
        this.tls = Objects.requireNonNull(tls);
        this.admission = Objects.requireNonNull(admission);
        this.backend = Objects.requireNonNull(backend);
        this.deliver = Objects.requireNonNull(deliver);
    }

    private static boolean allowed(Peer peer, Operation operation) {
        return CellRpcAuthorization.admits(peer, CellRpcAuthorization.surface(operation));
    }

    private static long maximum(Operation op) {
        return op == Operation.RELAY ? 1000 : 2000;
    }

    private static void retireAfter(
            CompletionStage<?> physical, CompletionStage<?> replyProcessed, RpcAdmission.Ticket ticket) {
        physical
                .thenCombine(replyProcessed.handle((v, e) -> null), (a, b) -> null)
                .thenRun(ticket::close);
    }

    private static InternalReply eventError(ControlEvent event, String error) {
        return InternalReply.newBuilder()
                .setOperationId(event.getEventId())
                .setCallId(event.getCallId())
                .setStatus("REJECTED")
                .setErrorCode(error)
                .build();
    }

    private static InternalReply error(InternalCommand c, String code) {
        return InternalReply.newBuilder()
                .setOperationId(c.getOperationId())
                .setStatus("REJECTED")
                .setErrorCode(code)
                .setCallId(c.getCallId())
                .build();
    }

    private static void respond(StreamObserver<InternalReply> reply, InternalReply value) {
        reply.onNext(value);
        reply.onCompleted();
    }

    private static SessionReply sessionError(SessionCommand command, String error) {
        return SessionReply.newBuilder()
                .setOperationId(command.getOperationId())
                .setStatus("REJECTED")
                .setErrorCode(error)
                .build();
    }

    private static void sessionReply(StreamObserver<SessionReply> observer, SessionReply reply) {
        observer.onNext(reply);
        observer.onCompleted();
    }

    public CellRpcServer compatibilityFacts(
            java.util.function.Supplier<CompatibilityObservationReply> source) {
        compatibilityFacts = Objects.requireNonNull(source);
        return this;
    }

    public CellRpcServer compatibility(RuntimeCapabilities capabilities) {
        this.capabilities = Objects.requireNonNull(capabilities);
        return this;
    }

    public CellRpcServer observe(OperationObservation observations) {
        this.observations = Objects.requireNonNull(observations);
        return this;
    }

    public CellRpcServer controlRoutes(NativeControlRouteResolver resolver) {
        if (server != null) throw new IllegalStateException("Already started");
        controlRoutes = Objects.requireNonNull(resolver);
        return this;
    }

    public CellRpcServer sessions(NativeSessionHandler handler) {
        if (server != null) throw new IllegalStateException("Already started");
        sessions = Objects.requireNonNull(handler);
        return this;
    }

    public synchronized CellRpcServer start() throws java.io.IOException {
        if (draining) throw new IllegalStateException("RPC server draining");
        if (server != null) throw new IllegalStateException("RPC server already started");
        server =
                NettyServerBuilder.forPort(requestedPort)
                        .sslContext(tls)
                        .maxInboundMessageSize(98304)
                        .maxConcurrentCallsPerConnection(128)
                        .executor(preCall)
                        .addStreamTracerFactory(preCall.tracer())
                        .callExecutor(
                                new ServerCallExecutorSupplier() {
                                    @Override
                                    public <Q, A> java.util.concurrent.Executor getExecutor(
                                            ServerCall<Q, A> call, Metadata headers) {
                                        return call.getMethodDescriptor()
                                                .getFullMethodName()
                                                .endsWith("/RelayNegotiation")
                                                ? executor
                                                : controlExecutor;
                                    }
                                })
                        .addService(
                                new CompatibilityObservationService(
                                        cell,
                                        "",
                                        "",
                                        admission,
                                        PEER::get,
                                        peer ->
                                                CellRpcAuthorization.admits(
                                                        peer, CellRpcAuthorization.Surface.COMPATIBILITY_OBSERVATION),
                                        () -> compatibilityFacts.get()))
                        .intercept(NativeRpcTelemetry.server())
                        .intercept(
                                new ServerInterceptor() {
                                    @Override
                                    public <Q, A> ServerCall.Listener<Q> interceptCall(
                                            ServerCall<Q, A> call, Metadata headers, ServerCallHandler<Q, A> next) {
                                        Peer peer = extract(call.getAttributes().get(Grpc.TRANSPORT_ATTR_SSL_SESSION));
                                        if (!CellRpcAuthorization.admitsCall(
                                                peer, call.getMethodDescriptor().getFullMethodName())) {
                                            call.close(
                                                    Status.PERMISSION_DENIED.withDescription("UNAUTHORIZED"), new Metadata());
                                            return new ServerCall.Listener<>() {
                                            };
                                        }
                                        return Contexts.interceptCall(
                                                Context.current().withValue(PEER, peer), call, headers, next);
                                    }
                                })
                        .addService(new Ingress())
                        .addService(new SessionService())
                        .addService(new ControlRoutes())
                        .build()
                        .start();
        return this;
    }

    public int port() {
        if (server == null) throw new IllegalStateException("RPC server not started");
        return server.getPort();
    }

    private Peer extract(SSLSession session) {
        return RpcTlsIdentity.extract(session, environment);
    }

    private void execute(Operation op, InternalCommand c, StreamObserver<InternalReply> reply) {
        Peer peer = PEER.get();
        if (!allowed(peer, op)) {
            respond(reply, error(c, "UNAUTHORIZED"));
            return;
        }
        if (c.getType().equals("RenewReservation")
                && !capabilities.permits(cell, RuntimeCapabilities.NATIVE_RENEWAL_V1)) {
            respond(reply, error(c, "UNAVAILABLE"));
            return;
        }
        if (c.getType().equals("INVITE")
                && !capabilities.permitsNew(cell, RuntimeCapabilities.FULL_CONNECTION_ACTIVE)) {
            respond(reply, error(c, "UNAVAILABLE"));
            return;
        }
        if ((op == Operation.EXECUTE || op == Operation.SYNC)
                && !capabilities.permits(cell, RuntimeCapabilities.FULL_CONNECTION_ACTIVE)
                || c.getType().equals("QueryParticipation")
                && (!capabilities.permits(cell, RuntimeCapabilities.HOME_PROOF_HIGH_WATER_V1)
                || !capabilities.permits(
                peer.cell(), RuntimeCapabilities.HOME_PROOF_HIGH_WATER_V1))) {
            respond(reply, error(c, "UNAVAILABLE"));
            return;
        }
        if (!cell.equals(c.getDestinationCell())) {
            respond(reply, error(c, "WRONG_CELL"));
            return;
        }
        try {
            if (!c.getTraceId().isEmpty() && !c.getTraceId().matches("[a-f0-9]{32}")
                    || c.getSchemaMajor() != 1
                    || c.getSchemaMinor() > 1
                    || c.getSchemaMinor() < 0
                    || c.getSerializedSize() > 98304
                    || c.getRemainingBudgetMs() <= 0
                    || c.getOperationId().length() != 36
                    || c.getPayloadHash().size() != 32)
                throw new IllegalArgumentException("Invalid RPC envelope");
            UUID.fromString(c.getOperationId());
            new io.webrtc.signaling.protocol.Identity.CallId(c.getCallId());
        } catch (IllegalArgumentException invalid) {
            respond(reply, error(c, "INVALID_MESSAGE"));
            return;
        }
        long remaining = Math.min(c.getRemainingBudgetMs(), maximum(op));
        Deadline deadline = Context.current().getDeadline();
        if (deadline == null) {
            respond(reply, error(c, "INVALID_DEADLINE"));
            return;
        }
        remaining = Math.min(remaining, deadline.timeRemaining(TimeUnit.MILLISECONDS));
        if (remaining <= 0) {
            respond(reply, error(c, "OUTCOME_UNKNOWN"));
            return;
        }
        RpcAdmission.Ticket ticket;
        try {
            ticket =
                    admission.acquire(
                            op == Operation.RELAY ? RpcAdmission.Lane.RELAY : RpcAdmission.Lane.CONTROL,
                            c.getSerializedSize());
        } catch (RpcAdmission.Overloaded e) {
            respond(reply, error(c, "OVERLOADED"));
            return;
        }
        try {
            var receipt =
                    OperationObservation.safeStart(
                            observations,
                            op == Operation.RELAY
                                    ? SignalingMetrics.Operation.RPC_RELAY
                                    : SignalingMetrics.Operation.RPC_CONTROL);
            RpcOperation<InternalReply> operation;
            try (var scope = receipt.activate()) {
                operation =
                        NativeRpcTelemetry.observe(
                                backend.executeTracked(op, c, peer, Duration.ofMillis(remaining)),
                                receipt,
                                value ->
                                        value == null
                                                ? SignalingMetrics.Outcome.OUTCOME_UNKNOWN
                                                : NativeRpcTelemetry.outcome(
                                                value.getErrorCode().isEmpty()
                                                        ? value.getStatus()
                                                        : value.getErrorCode()));
            } catch (Exception unknown) {
                receipt.logical(SignalingMetrics.Outcome.OUTCOME_UNKNOWN);
                throw new RuntimeException(unknown);
            }
            var replied = new CompletableFuture<Void>();
            retireAfter(operation.physicalCompletion(), replied, ticket);
            operation
                    .logical()
                    .whenComplete(
                            (value, failure) -> {
                                try {
                                    if (failure != null) {
                                        if (Status.fromThrowable(failure).getCode() == Status.Code.UNAVAILABLE)
                                            reply.onError(Status.UNAVAILABLE.asRuntimeException());
                                        else
                                            respond(
                                                    reply,
                                                    error(
                                                            c,
                                                            failure instanceof IllegalArgumentException
                                                                    ? "INVALID_MESSAGE"
                                                                    : "OUTCOME_UNKNOWN"));
                                    } else if (!c.getOperationId().equals(value.getOperationId()))
                                        respond(reply, error(c, "OUTCOME_UNKNOWN"));
                                    else respond(reply, value);
                                } finally {
                                    replied.complete(null);
                                }
                            });
        } catch (RuntimeException e) {
            ticket.close();
            respond(reply, error(c, "OUTCOME_UNKNOWN"));
        }
    }

    private void deliver(ControlEvent event, StreamObserver<InternalReply> reply) {
        var peer = PEER.get();
        if (!allowed(peer, Operation.DELIVER)) {
            respond(reply, eventError(event, "UNAUTHORIZED"));
            return;
        }
        var deadline = Context.current().getDeadline();
        try {
            var call = new io.webrtc.signaling.protocol.Identity.CallId(event.getCallId());
            if (!peer.cell().equals(call.coordinatorCell())) {
                respond(reply, eventError(event, "UNAUTHORIZED"));
                return;
            }
            UUID.fromString(event.getEventId());
            var destination = event.getDestination();
            new io.webrtc.signaling.protocol.Identity.UserId(destination.getUserId());
            new io.webrtc.signaling.protocol.Identity.SessionKey(
                    destination.getIssuer(), destination.getJti());
            RpcBusinessHandler.uuid(destination.getIncarnation());
            RpcBusinessHandler.uuid(destination.getConnectionId());
            if (deadline == null
                    || deadline.isExpired()
                    || event.getSerializedSize() > 98304
                    || event.getMetadata().size() > 8192
                    || event.getCallVersion() < 1
                    || destination.getConnectionGeneration() < 1
                    || event.getAuthorityBucketId() < 0
                    || event.getAuthorityBucketId() > 16383
                    || !event.getType().matches("[A-Z_]{1,64}")) throw new IllegalArgumentException();
        } catch (IllegalArgumentException invalid) {
            respond(reply, eventError(event, "INVALID_MESSAGE"));
            return;
        }
        RpcAdmission.Ticket ticket;
        try {
            ticket = admission.acquire(RpcAdmission.Lane.CONTROL, event.getSerializedSize());
        } catch (RpcAdmission.Overloaded full) {
            respond(reply, eventError(event, "OVERLOADED"));
            return;
        }
        try {
            deliver
                    .apply(event)
                    .whenComplete(
                            (value, error) -> {
                                try {
                                    if (error != null
                                            || value == null
                                            || value.getAckCommitted()
                                            || !value.getOperationId().equals(event.getEventId())
                                            || !value.getCallId().equals(event.getCallId()))
                                        respond(reply, eventError(event, "OUTCOME_UNKNOWN"));
                                    else respond(reply, value);
                                } finally {
                                    ticket.close();
                                }
                            });
        } catch (RuntimeException error) {
            ticket.close();
            respond(reply, eventError(event, "OUTCOME_UNKNOWN"));
        }
    }

    /**
     * Permanently unbind admission and wait for both native cleanup and actual transport termination.
     */
    public CompletionStage<Void> settleAdmitted() {
        return admission.settleAdmitted();
    }

    public synchronized CompletionStage<Void> drain() {
        if (!draining) {
            draining = true;
            if (server != null) server.shutdown();
            admission
                    .drain()
                    .whenComplete(
                            (settled, failure) -> {
                                if (failure != null) {
                                    drained.completeExceptionally(failure);
                                    return;
                                }
                                Thread.startVirtualThread(
                                        () -> {
                                            try {
                                                long end = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
                                                if (server != null && !server.awaitTermination(2, TimeUnit.SECONDS))
                                                    throw new TimeoutException("RPC transport termination unproven");
                                                // Lookup producers can still hand off queued callbacks to these body pools.
                                                preCall.shutdownAndAwait(end);
                                                executor.shutdown();
                                                controlExecutor.shutdown();
                                                for (var cpu : List.of(executor, controlExecutor)) {
                                                    long remaining = end - System.nanoTime();
                                                    if (remaining <= 0
                                                            || !cpu.awaitTermination(remaining, TimeUnit.NANOSECONDS))
                                                        throw new TimeoutException("RPC CPU termination unproven");
                                                }
                                                drained.complete(null);
                                            } catch (InterruptedException interrupted) {
                                                Thread.currentThread().interrupt();
                                                drained.completeExceptionally(interrupted);
                                            } catch (TimeoutException unknown) {
                                                drained.completeExceptionally(unknown);
                                            }
                                        });
                            });
        }
        return drained.minimalCompletionStage();
    }

    @Override
    public void close() {
        drain();
    }

    public enum Operation {
        RESERVE,
        CLAIM,
        RELEASE,
        EXECUTE,
        DELIVER,
        RELAY,
        SYNC
    }

    @FunctionalInterface
    public interface Backend {
        CompletionStage<InternalReply> execute(
                Operation operation, InternalCommand command, Peer peer, Duration remainingBudget);

        default RpcOperation<InternalReply> executeTracked(
                Operation operation, InternalCommand command, Peer peer, Duration remainingBudget) {
            var result = execute(operation, command, peer, remainingBudget);
            return new RpcOperation<>(result, result);
        }
    }

    public record Peer(String cell, String role, String workloadId) {
        public Peer(String cell, String role) {
            this(cell, role, "");
        }
    }

    private final class SessionService extends SessionIngressGrpc.SessionIngressImplBase {
        @Override
        public void mutateSession(SessionCommand command, StreamObserver<SessionReply> reply) {
            if (!CellRpcAuthorization.admits(PEER.get(), CellRpcAuthorization.Surface.SESSION)) {
                sessionReply(reply, sessionError(command, "UNAUTHORIZED"));
                return;
            }
            var deadline = Context.current().getDeadline();
            if (sessions == null
                    || deadline == null
                    || deadline.isExpired()
                    || !capabilities.permits(cell, RuntimeCapabilities.SESSION_S1)
                    || command.getType().equals("REGISTER")
                    && !capabilities.permitsNew(cell, RuntimeCapabilities.SESSION_S1)) {
                sessionReply(reply, sessionError(command, "UNAVAILABLE"));
                return;
            }
            RpcAdmission.Ticket ticket;
            try {
                ticket = admission.acquire(RpcAdmission.Lane.CONTROL, command.getSerializedSize());
            } catch (RpcAdmission.Overloaded full) {
                sessionReply(reply, sessionError(command, "OVERLOADED"));
                return;
            }
            try {
                var observation =
                        OperationObservation.safeStart(observations, SignalingMetrics.Operation.RPC_CONTROL);
                var operation =
                        NativeRpcTelemetry.observe(
                                sessions.execute(
                                        command,
                                        PEER.get(),
                                        Duration.ofMillis(
                                                Math.max(
                                                        1, Math.min(2000, deadline.timeRemaining(TimeUnit.MILLISECONDS))))),
                                observation,
                                value ->
                                        value == null
                                                ? SignalingMetrics.Outcome.OUTCOME_UNKNOWN
                                                : NativeRpcTelemetry.outcome(
                                                value.getErrorCode().isBlank()
                                                        ? value.getStatus()
                                                        : value.getErrorCode()));
                var replied = new CompletableFuture<Void>();
                retireAfter(operation.physicalCompletion(), replied, ticket);
                operation
                        .logical()
                        .whenComplete(
                                (value, failure) -> {
                                    try {
                                        if (failure != null
                                                || value == null
                                                || !value.getOperationId().equals(command.getOperationId()))
                                            sessionReply(reply, sessionError(command, "OUTCOME_UNKNOWN"));
                                        else sessionReply(reply, value);
                                    } finally {
                                        replied.complete(null);
                                    }
                                });
            } catch (RuntimeException invalid) {
                ticket.close();
                sessionReply(reply, sessionError(command, "OUTCOME_UNKNOWN"));
            }
        }
    }

    private final class ControlRoutes extends ControlRouteIngressGrpc.ControlRouteIngressImplBase {
        @Override
        public void resolveControlRoute(
                ControlRouteRequest request, StreamObserver<ControlRouteReply> reply) {
            var deadline = Context.current().getDeadline();
            var error =
                    ControlRouteReply.newBuilder()
                            .setEventId(request.getEventId())
                            .setCallId(request.getCallId())
                            .setCallVersion(request.getCallVersion())
                            .setStatus("REJECTED")
                            .setErrorCode("OUTCOME_UNKNOWN")
                            .build();
            if (!CellRpcAuthorization.admits(PEER.get(), CellRpcAuthorization.Surface.CONTROL_ROUTE)) {
                reply.onNext(error.toBuilder().setErrorCode("UNAUTHORIZED").build());
                reply.onCompleted();
                return;
            }
            RpcAdmission.Ticket ticket;
            if (!capabilities.permits(cell, RuntimeCapabilities.NATIVE_CONTROL_RECEIPT_V1)
                    || controlRoutes == null
                    || deadline == null
                    || deadline.isExpired()
                    || request.getRemainingBudgetMs() <= 0
                    || request.getSerializedSize() > 4096) {
                reply.onNext(error);
                reply.onCompleted();
                return;
            }
            try {
                ticket = admission.acquire(RpcAdmission.Lane.CONTROL, request.getSerializedSize());
            } catch (RpcAdmission.Overloaded full) {
                reply.onNext(error);
                reply.onCompleted();
                return;
            }
            try {
                var observation =
                        OperationObservation.safeStart(observations, SignalingMetrics.Operation.RPC_CONTROL);
                var operation =
                        NativeRpcTelemetry.observe(
                                controlRoutes.resolve(
                                        request,
                                        PEER.get(),
                                        Duration.ofMillis(
                                                Math.max(
                                                        1,
                                                        Math.min(
                                                                2000,
                                                                Math.min(
                                                                        request.getRemainingBudgetMs(),
                                                                        deadline.timeRemaining(TimeUnit.MILLISECONDS)))))),
                                observation,
                                value ->
                                        value == null
                                                ? SignalingMetrics.Outcome.OUTCOME_UNKNOWN
                                                : NativeRpcTelemetry.outcome(
                                                value.getErrorCode().isBlank()
                                                        ? value.getStatus()
                                                        : value.getErrorCode()));
                var replied = new CompletableFuture<Void>();
                retireAfter(operation.physicalCompletion(), replied, ticket);
                operation
                        .logical()
                        .whenComplete(
                                (value, failure) -> {
                                    try {
                                        reply.onNext(failure == null && value != null ? value : error);
                                        reply.onCompleted();
                                    } finally {
                                        replied.complete(null);
                                    }
                                });
            } catch (RuntimeException notStarted) {
                ticket.close();
                reply.onNext(error);
                reply.onCompleted();
            }
        }
    }

    private final class Ingress extends CellIngressGrpc.CellIngressImplBase {
        @Override
        public void reserveUser(InternalCommand c, StreamObserver<InternalReply> r) {
            execute(Operation.RESERVE, c, r);
        }

        @Override
        public void claimAccept(InternalCommand c, StreamObserver<InternalReply> r) {
            execute(Operation.CLAIM, c, r);
        }

        @Override
        public void releaseIfCallVersion(InternalCommand c, StreamObserver<InternalReply> r) {
            execute(Operation.RELEASE, c, r);
        }

        @Override
        public void executeCallCommand(InternalCommand c, StreamObserver<InternalReply> r) {
            execute(Operation.EXECUTE, c, r);
        }

        @Override
        public void relayNegotiation(InternalCommand c, StreamObserver<InternalReply> r) {
            execute(Operation.RELAY, c, r);
        }

        @Override
        public void syncCall(InternalCommand c, StreamObserver<InternalReply> r) {
            execute(Operation.SYNC, c, r);
        }

        @Override
        public void deliverControlEvent(ControlEvent c, StreamObserver<InternalReply> r) {
            deliver(c, r);
        }
    }
}
