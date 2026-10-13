package io.webrtc.signaling.rpc;

import io.grpc.*;
import io.grpc.netty.NettyChannelBuilder;
import io.grpc.stub.StreamObserver;
import io.netty.handler.ssl.SslContext;
import io.webrtc.signaling.observability.OperationObservation;
import io.webrtc.signaling.observability.SignalingMetrics;
import io.webrtc.signaling.protocol.RuntimeCapabilities;
import io.webrtc.signaling.protocol.internal.*;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;

/**
 * Two lazy channels/destination, isolated flow control, one owner of bounded same-identity retry.
 */
public final class CellRpcClient implements AutoCloseable {
    private final Map<ChannelKey, ChannelSlot> slots = new HashMap<>();
    private final Map<String, Endpoint> destinations;
    private final RpcTlsContexts.ClientTls tls;
    private final String environment;
    private final RpcAdmission admission;
    private final ConcurrentHashMap<ChannelKey, ManagedChannel> channels = new ConcurrentHashMap<>();
    private final ExecutorService callbacks =
            new ThreadPoolExecutor(
                    4,
                    4,
                    0,
                    TimeUnit.SECONDS,
                    new ArrayBlockingQueue<>(256),
                    Thread.ofPlatform().daemon().name("cell-rpc-client-", 0).factory(),
                    new ThreadPoolExecutor.AbortPolicy());
    private final ExecutorService controlCallbacks =
            new ThreadPoolExecutor(
                    4,
                    4,
                    0,
                    TimeUnit.SECONDS,
                    new ArrayBlockingQueue<>(256),
                    Thread.ofPlatform().daemon().name("cell-rpc-control-client-", 0).factory(),
                    new ThreadPoolExecutor.AbortPolicy());
    private final ScheduledThreadPoolExecutor retryTimers =
            new ScheduledThreadPoolExecutor(
                    1, Thread.ofPlatform().daemon().name("cell-rpc-retry").factory());
    private final Set<Flight<?>> active = new HashSet<>();
    private final CompletableFuture<Void> drained = new CompletableFuture<>(),
            settled = new CompletableFuture<>();
    private volatile boolean closed;
    private volatile OperationObservation observations = OperationObservation.NONE;
    private volatile RuntimeCapabilities capabilities = RuntimeCapabilities.DENY;

    /**
     * Test-only raw context adapter; production supplies cell-bound handshake verification.
     */
    CellRpcClient(Map<String, Endpoint> destinations, SslContext tls, RpcAdmission admission) {
        this("test", destinations, cell -> tls, admission);
    }

    public CellRpcClient(
            String environment,
            Map<String, Endpoint> destinations,
            RpcTlsContexts.ClientTls tls,
            RpcAdmission admission) {
        if (environment == null
                || !environment.matches("[a-z0-9-]{1,32}")
                || destinations.isEmpty()
                || destinations.size() > 50
                || destinations.keySet().stream().anyMatch(c -> !c.matches("[a-z][a-z0-9-]{0,23}")))
            throw new IllegalArgumentException("Invalid bounded destination topology");
        this.environment = environment;
        this.destinations = Map.copyOf(destinations);
        this.tls = Objects.requireNonNull(tls);
        this.admission = Objects.requireNonNull(admission);
        retryTimers.setRemoveOnCancelPolicy(true);
    }

    private static <T> RpcOperation<T> completed(T value) {
        return new RpcOperation<>(
                CompletableFuture.completedFuture(value).minimalCompletionStage(),
                CompletableFuture.completedFuture(null).minimalCompletionStage());
    }

    private static SessionReply sessionError(SessionCommand command, String error) {
        return SessionReply.newBuilder()
                .setOperationId(command.getOperationId())
                .setStatus("REJECTED")
                .setErrorCode(error)
                .build();
    }

    private static InternalReply error(InternalCommand c, String code) {
        return InternalReply.newBuilder()
                .setOperationId(c.getOperationId())
                .setCallId(c.getCallId())
                .setStatus("REJECTED")
                .setErrorCode(code)
                .build();
    }

    public CellRpcClient observe(OperationObservation observations) {
        this.observations = Objects.requireNonNull(observations);
        return this;
    }

    public CellRpcClient compatibility(RuntimeCapabilities capabilities) {
        this.capabilities = Objects.requireNonNull(capabilities);
        return this;
    }

    public int channelCount() {
        return channels.size();
    }

    private ManagedChannel channel(String cell, RpcAdmission.Lane lane) {
        var key = new ChannelKey(cell, lane);
        final ChannelSlot slot;
        synchronized (this) {
            if (closed) throw new IllegalStateException("RPC client draining");
            if (!destinations.containsKey(cell))
                throw new IllegalArgumentException("Unknown destination");
            slot = slots.computeIfAbsent(key, ignored -> new ChannelSlot());
        }
        // Serialize only this destination/lane. TLS creation must never hold the process owner lock.
        synchronized (slot) {
            if (slot.channel != null) return slot.channel;
            if (closed) throw new IllegalStateException("RPC client draining");
            Endpoint endpoint = destinations.get(cell);
            var built =
                    NettyChannelBuilder.forAddress(endpoint.host(), endpoint.port())
                            .overrideAuthority(endpoint.tlsAuthority())
                            .sslContext(tls.context(cell))
                            .disableRetry()
                            .maxInboundMessageSize(98304)
                            .executor(lane == RpcAdmission.Lane.CONTROL ? controlCallbacks : callbacks)
                            .intercept(NativeRpcTelemetry.client())
                            .intercept(
                                    new ClientInterceptor() {
                                        @Override
                                        public <Q, A> ClientCall<Q, A> interceptCall(
                                                MethodDescriptor<Q, A> method, CallOptions options, Channel next) {
                                            var delegate = next.newCall(method, options);
                                            return new ForwardingClientCall.SimpleForwardingClientCall<>(delegate) {
                                                @Override
                                                public void start(ClientCall.Listener<A> listener, Metadata headers) {
                                                    super.start(
                                                            new ForwardingClientCallListener.SimpleForwardingClientCallListener<>(
                                                                    listener) {
                                                                boolean rejected;

                                                                @Override
                                                                public void onHeaders(Metadata metadata) {
                                                                    var peer =
                                                                            RpcTlsIdentity.extract(
                                                                                    delegate
                                                                                            .getAttributes()
                                                                                            .get(Grpc.TRANSPORT_ATTR_SSL_SESSION),
                                                                                    environment);
                                                                    if (peer == null
                                                                            || !peer.cell().equals(cell)
                                                                            || !peer.role().equals("actor")) {
                                                                        rejected = true;
                                                                        delegate.cancel("UNAUTHORIZED", null);
                                                                    } else super.onHeaders(metadata);
                                                                }

                                                                @Override
                                                                public void onMessage(A message) {
                                                                    if (!rejected) super.onMessage(message);
                                                                }

                                                                @Override
                                                                public void onClose(Status status, Metadata trailers) {
                                                                    super.onClose(
                                                                            rejected
                                                                                    ? Status.PERMISSION_DENIED.withDescription("UNAUTHORIZED")
                                                                                    : status,
                                                                            trailers);
                                                                }
                                                            },
                                                            headers);
                                                }
                                            };
                                        }
                                    })
                            .build();
            synchronized (this) {
                slot.channel = built;
                channels.put(key, built);
                // A construction admitted before drain still belongs to its original flight.
                if (closed) built.shutdown();
            }
            return built;
        }
    }

    private synchronized <T> Flight<T> begin(RpcAdmission.Lane lane, int bytes) {
        if (closed) return null;
        var flight = new Flight<T>(admission.acquire(lane, bytes), lane);
        active.add(flight);
        return flight;
    }

    public CompletionStage<InternalReply> call(
            CellRpcServer.Operation op, InternalCommand original, Duration budget) {
        return callTracked(op, original, budget).logical();
    }

    public RpcOperation<InternalReply> callTracked(
            CellRpcServer.Operation op, InternalCommand original, Duration budget) {
        long started = System.nanoTime();
        if (original.getType().equals("INVITE")
                && !capabilities.permitsNew(
                original.getDestinationCell(), RuntimeCapabilities.FULL_CONNECTION_ACTIVE))
            return completed(error(original, "UNAVAILABLE"));
        if (original.getType().equals("RenewReservation")
                && !capabilities.permits(
                original.getDestinationCell(), RuntimeCapabilities.NATIVE_RENEWAL_V1))
            return completed(error(original, "UNAVAILABLE"));
        if (original.getType().equals("QueryParticipation")
                && !capabilities.permits(
                original.getDestinationCell(), RuntimeCapabilities.HOME_PROOF_HIGH_WATER_V1))
            return completed(error(original, "UNAVAILABLE"));
        if ((op == CellRpcServer.Operation.EXECUTE || op == CellRpcServer.Operation.SYNC)
                && !capabilities.permits(
                original.getDestinationCell(), RuntimeCapabilities.FULL_CONNECTION_ACTIVE))
            return completed(error(original, "UNAVAILABLE"));
        if (closed
                || budget == null
                || budget.isNegative()
                || budget.isZero()
                || original.getRemainingBudgetMs() <= 0)
            return completed(error(original, "OUTCOME_UNKNOWN"));
        if (!destinations.containsKey(original.getDestinationCell()))
            return completed(error(original, "WRONG_CELL"));
        var lane =
                op == CellRpcServer.Operation.RELAY ? RpcAdmission.Lane.RELAY : RpcAdmission.Lane.CONTROL;
        long nanos =
                Math.min(
                        budget.toNanos(),
                        Math.min(
                                Duration.ofSeconds(lane == RpcAdmission.Lane.RELAY ? 1 : 2).toNanos(),
                                TimeUnit.MILLISECONDS.toNanos(original.getRemainingBudgetMs())));
        Flight<InternalReply> flight;
        try {
            flight = begin(lane, original.getSerializedSize());
        } catch (RpcAdmission.Overloaded full) {
            return completed(error(original, "OVERLOADED"));
        }
        if (flight == null) return completed(error(original, "OUTCOME_UNKNOWN"));
        var traced =
                original.getTraceId().isEmpty()
                        ? original.toBuilder().setTraceId(flight.observation.traceId()).build()
                        : original;
        try (var scope = flight.observation.activate()) {
            attempt(op, traced, started + nanos, 0, lane, flight);
        } catch (Exception failed) {
            flight.logical.complete(error(original, "OUTCOME_UNKNOWN"));
        } finally {
            flight.ended();
        }
        return flight.operation();
    }

    private void attempt(
            CellRpcServer.Operation op,
            InternalCommand original,
            long end,
            int attempt,
            RpcAdmission.Lane lane,
            Flight<InternalReply> flight) {
        long remaining = end - System.nanoTime();
        if (closed || remaining <= 0) {
            flight.logical.complete(error(original, "OUTCOME_UNKNOWN"));
            return;
        }
        var transport = channel(original.getDestinationCell(), lane);
        remaining = end - System.nanoTime();
        if (closed || remaining <= 0) {
            flight.logical.complete(error(original, "OUTCOME_UNKNOWN"));
            return;
        }
        var deadline = Deadline.after(remaining, TimeUnit.NANOSECONDS);
        var command =
                original.toBuilder()
                        .setRemainingBudgetMs(Math.max(1, TimeUnit.NANOSECONDS.toMillis(remaining)))
                        .build();
        var stub =
                CellIngressGrpc.newStub(transport)
                        .withOption(NativeRpcTelemetry.TRACE_ID, flight.observation.traceId())
                        .withDeadline(deadline)
                        .withWaitForReady();
        flight.opened();
        var terminal = new java.util.concurrent.atomic.AtomicBoolean();
        var observer =
                new StreamObserver<InternalReply>() {
                    boolean received;

                    @Override
                    public void onNext(InternalReply value) {
                        received = true;
                        flight.logical.complete(
                                original.getOperationId().equals(value.getOperationId())
                                        && original.getCallId().equals(value.getCallId())
                                        ? value
                                        : error(original, "OUTCOME_UNKNOWN"));
                    }

                    @Override
                    public void onCompleted() {
                        if (!terminal.compareAndSet(false, true)) return;
                        try {
                            if (!received) flight.logical.complete(error(original, "OUTCOME_UNKNOWN"));
                        } finally {
                            flight.ended();
                        }
                    }

                    @Override
                    public void onError(Throwable failure) {
                        if (!terminal.compareAndSet(false, true)) return;
                        try {
                            var code = Status.fromThrowable(failure).getCode();
                            var delay =
                                    !received && code == Status.Code.UNAVAILABLE && !closed
                                            ? RpcRetryBackoff.delayNanos(
                                            attempt,
                                            ThreadLocalRandom.current().nextDouble(),
                                            end - System.nanoTime())
                                            : OptionalLong.empty();
                            if (delay.isPresent()) {
                                // Retain the original ticket across the delayed dispatch as well as every transport
                                // stream.
                                flight.opened();
                                try {
                                    retryTimers.schedule(
                                            () -> {
                                                try {
                                                    CellRpcClient.this.attempt(op, original, end, attempt + 1, lane, flight);
                                                } catch (RuntimeException rejected) {
                                                    flight.logical.complete(error(original, "OUTCOME_UNKNOWN"));
                                                } finally {
                                                    flight.ended();
                                                }
                                            },
                                            delay.getAsLong(),
                                            TimeUnit.NANOSECONDS);
                                } catch (RejectedExecutionException notStarted) {
                                    flight.logical.complete(error(original, "OUTCOME_UNKNOWN"));
                                    flight.ended();
                                }
                            } else
                                flight.logical.complete(
                                        error(
                                                original,
                                                code == Status.Code.PERMISSION_DENIED || code == Status.Code.UNAUTHENTICATED
                                                        ? "UNAUTHORIZED"
                                                        : "OUTCOME_UNKNOWN"));
                        } finally {
                            flight.ended();
                        }
                    }
                };
        try {
            switch (op) {
                case RESERVE -> stub.reserveUser(command, observer);
                case CLAIM -> stub.claimAccept(command, observer);
                case RELEASE -> stub.releaseIfCallVersion(command, observer);
                case EXECUTE -> stub.executeCallCommand(command, observer);
                case RELAY -> stub.relayNegotiation(command, observer);
                case SYNC -> stub.syncCall(command, observer);
                default -> throw new IllegalArgumentException("Use event delivery method");
            }
        } catch (RuntimeException failed) {
            observer.onError(failed);
        }
    }

    public CompletionStage<InternalReply> deliver(
            String destination, ControlEvent event, Duration budget) {
        return deliverTracked(destination, event, budget).logical();
    }

    public RpcOperation<InternalReply> deliverTracked(
            String destination, ControlEvent event, Duration budget) {
        long started = System.nanoTime();
        var unknown =
                InternalReply.newBuilder()
                        .setOperationId(event.getEventId())
                        .setCallId(event.getCallId())
                        .setErrorCode("OUTCOME_UNKNOWN")
                        .build();
        if (closed
                || !destinations.containsKey(destination)
                || budget == null
                || budget.isNegative()
                || budget.isZero()) return completed(unknown);
        Flight<InternalReply> flight;
        try {
            flight = begin(RpcAdmission.Lane.CONTROL, event.getSerializedSize());
        } catch (RpcAdmission.Overloaded full) {
            return completed(unknown.toBuilder().setErrorCode("OVERLOADED").build());
        }
        if (flight == null) return completed(unknown);
        try {
            long end = started + Math.min(budget.toNanos(), Duration.ofSeconds(2).toNanos());
            var transport = channel(destination, RpcAdmission.Lane.CONTROL);
            long remaining = end - System.nanoTime();
            if (closed || remaining <= 0) {
                flight.logical.complete(unknown);
                return flight.operation();
            }
            var deadline = Deadline.after(remaining, TimeUnit.NANOSECONDS);
            var stub =
                    CellIngressGrpc.newStub(transport)
                            .withOption(NativeRpcTelemetry.TRACE_ID, flight.observation.traceId())
                            .withDeadline(deadline);
            flight.opened();
            var terminal = new java.util.concurrent.atomic.AtomicBoolean();
            var observer =
                    new StreamObserver<InternalReply>() {
                        @Override
                        public void onNext(InternalReply value) {
                            flight.logical.complete(
                                    event.getEventId().equals(value.getOperationId())
                                            && event.getCallId().equals(value.getCallId())
                                            ? value
                                            : unknown);
                        }

                        @Override
                        public void onError(Throwable failure) {
                            if (terminal.compareAndSet(false, true))
                                try {
                                    flight.logical.complete(unknown);
                                } finally {
                                    flight.ended();
                                }
                        }

                        @Override
                        public void onCompleted() {
                            if (terminal.compareAndSet(false, true))
                                try {
                                    flight.logical.complete(unknown);
                                } finally {
                                    flight.ended();
                                }
                        }
                    };
            try {
                stub.deliverControlEvent(event, observer);
            } catch (RuntimeException failed) {
                observer.onError(failed);
            }
        } catch (RuntimeException failed) {
            flight.logical.complete(unknown);
        } finally {
            flight.ended();
        }
        return flight.operation();
    }

    public RpcOperation<ControlRouteReply> controlRouteTracked(
            ControlRouteRequest request, Duration budget) {
        if (!capabilities.permits(
                request.getDestinationCell(), RuntimeCapabilities.NATIVE_CONTROL_RECEIPT_V1))
            return new RpcOperation<>(
                    CompletableFuture.failedFuture(
                            new IllegalStateException("Native control capability unavailable")),
                    CompletableFuture.completedFuture(null));
        if (closed
                || budget == null
                || budget.isZero()
                || budget.isNegative()
                || request.getRemainingBudgetMs() <= 0
                || request.getSerializedSize() > 4096
                || !destinations.containsKey(request.getDestinationCell()))
            return new RpcOperation<>(
                    CompletableFuture.failedFuture(new IllegalStateException("Control route unavailable")),
                    CompletableFuture.completedFuture(null));
        final Flight<ControlRouteReply> flight;
        final long end =
                System.nanoTime()
                        + Math.min(
                        Duration.ofSeconds(2).toNanos(),
                        Math.min(
                                budget.toNanos(),
                                TimeUnit.MILLISECONDS.toNanos(request.getRemainingBudgetMs())));
        try {
            flight = begin(RpcAdmission.Lane.CONTROL, request.getSerializedSize());
        } catch (RuntimeException rejected) {
            return new RpcOperation<>(
                    CompletableFuture.failedFuture(rejected), CompletableFuture.completedFuture(null));
        }
        if (flight == null)
            return new RpcOperation<>(
                    CompletableFuture.failedFuture(new IllegalStateException("Draining")),
                    CompletableFuture.completedFuture(null));
        flight.logical.orTimeout(Math.max(1, end - System.nanoTime()), TimeUnit.NANOSECONDS);
        try {
            var transport = channel(request.getDestinationCell(), RpcAdmission.Lane.CONTROL);
            long remaining = end - System.nanoTime();
            if (remaining <= 0) throw new IllegalStateException("Original route deadline");
            var stub =
                    ControlRouteIngressGrpc.newStub(transport)
                            .withOption(NativeRpcTelemetry.TRACE_ID, flight.observation.traceId())
                            .withDeadlineAfter(remaining, TimeUnit.NANOSECONDS);
            flight.opened();
            var terminal = new java.util.concurrent.atomic.AtomicBoolean();
            var observer =
                    new StreamObserver<ControlRouteReply>() {
                        public void onNext(ControlRouteReply value) {
                            if (value.getRoute().size() > 4096
                                    || !value.getEventId().equals(request.getEventId())
                                    || !value.getCallId().equals(request.getCallId())
                                    || value.getCallVersion() != request.getCallVersion()
                                    || !value.getSourceCell().equals(request.getDestinationCell())
                                    || value.getStorageEpoch() != request.getStorageEpoch()
                                    || value.getDirectoryEpoch() != request.getDirectoryEpoch()
                                    || !value.getStatus().equals("ROUTE")
                                    || !value.getErrorCode().isEmpty())
                                flight.logical.completeExceptionally(
                                        new IllegalStateException("Invalid native control route"));
                            else flight.logical.complete(value);
                        }

                        public void onError(Throwable failure) {
                            if (terminal.compareAndSet(false, true)) {
                                try {
                                    flight.logical.completeExceptionally(failure);
                                } finally {
                                    flight.ended();
                                }
                            }
                        }

                        public void onCompleted() {
                            if (terminal.compareAndSet(false, true)) {
                                try {
                                    flight.logical.completeExceptionally(
                                            new IllegalStateException("Missing route receipt"));
                                } finally {
                                    flight.ended();
                                }
                            }
                        }
                    };
            try {
                stub.resolveControlRoute(
                        request.toBuilder()
                                .setRemainingBudgetMs(Math.max(1, TimeUnit.NANOSECONDS.toMillis(remaining)))
                                .build(),
                        observer);
            } catch (RuntimeException unknown) {
                flight.logical.completeExceptionally(unknown);
            }
        } catch (RuntimeException failed) {
            flight.logical.completeExceptionally(failed);
        } finally {
            flight.ended();
        }
        return flight.operation();
    }

    public CompletionStage<SessionReply> session(SessionCommand command, Duration budget) {
        return sessionTracked(command, budget).logical();
    }

    public RpcOperation<SessionReply> sessionTracked(SessionCommand command, Duration budget) {
        if (command.getType().equals("REGISTER")
                && !capabilities.permitsNew(command.getDestinationCell(), RuntimeCapabilities.SESSION_S1))
            return completed(sessionError(command, "UNAVAILABLE"));
        if (!capabilities.permits(command.getDestinationCell(), RuntimeCapabilities.SESSION_S1))
            return completed(sessionError(command, "UNAVAILABLE"));
        long started = System.nanoTime();
        if (closed
                || budget == null
                || budget.isNegative()
                || budget.isZero()
                || command.getRemainingBudgetMs() <= 0
                || !destinations.containsKey(command.getDestinationCell()))
            return completed(sessionError(command, "OUTCOME_UNKNOWN"));
        Flight<SessionReply> flight;
        try {
            flight = begin(RpcAdmission.Lane.CONTROL, command.getSerializedSize());
        } catch (RpcAdmission.Overloaded full) {
            return completed(sessionError(command, "OVERLOADED"));
        }
        if (flight == null) return completed(sessionError(command, "OUTCOME_UNKNOWN"));
        try {
            long nanos =
                    Math.min(
                            Duration.ofSeconds(2).toNanos(),
                            Math.min(
                                    budget.toNanos(), TimeUnit.MILLISECONDS.toNanos(command.getRemainingBudgetMs())));
            long end = started + nanos;
            var transport = channel(command.getDestinationCell(), RpcAdmission.Lane.CONTROL);
            long remaining = end - System.nanoTime();
            if (closed || remaining <= 0) {
                flight.logical.complete(sessionError(command, "OUTCOME_UNKNOWN"));
                return flight.operation();
            }
            var deadline = Deadline.after(remaining, TimeUnit.NANOSECONDS);
            var stub =
                    SessionIngressGrpc.newStub(transport)
                            .withOption(NativeRpcTelemetry.TRACE_ID, flight.observation.traceId())
                            .withDeadline(deadline);
            flight.opened();
            var terminal = new java.util.concurrent.atomic.AtomicBoolean();
            var observer =
                    new StreamObserver<SessionReply>() {
                        @Override
                        public void onNext(SessionReply value) {
                            flight.logical.complete(
                                    command.getOperationId().equals(value.getOperationId())
                                            ? value
                                            : sessionError(command, "OUTCOME_UNKNOWN"));
                        }

                        @Override
                        public void onError(Throwable failure) {
                            if (terminal.compareAndSet(false, true))
                                try {
                                    var code = Status.fromThrowable(failure).getCode();
                                    flight.logical.complete(
                                            sessionError(
                                                    command,
                                                    code == Status.Code.PERMISSION_DENIED
                                                            || code == Status.Code.UNAUTHENTICATED
                                                            ? "UNAUTHORIZED"
                                                            : "OUTCOME_UNKNOWN"));
                                } finally {
                                    flight.ended();
                                }
                        }

                        @Override
                        public void onCompleted() {
                            if (terminal.compareAndSet(false, true))
                                try {
                                    flight.logical.complete(sessionError(command, "OUTCOME_UNKNOWN"));
                                } finally {
                                    flight.ended();
                                }
                        }
                    };
            try {
                stub.mutateSession(command, observer);
            } catch (RuntimeException failed) {
                observer.onError(failed);
            }
        } catch (RuntimeException failed) {
            flight.logical.complete(sessionError(command, "OUTCOME_UNKNOWN"));
        } finally {
            flight.ended();
        }
        return flight.operation();
    }

    /**
     * Bootstrap observation uses authenticated CONTROL transport without a business capability gate.
     */
    public RpcOperation<CompatibilityObservationReply> observeCompatibility(
            String target, Duration budget) {
        final Flight<CompatibilityObservationReply> flight;
        final CompatibilityObservationRequest request;
        final long end;
        try {
            if (budget == null
                    || budget.isNegative()
                    || budget.isZero()
                    || budget.compareTo(Duration.ofSeconds(2)) > 0
                    || !destinations.containsKey(target))
                throw new IllegalArgumentException("Unenrolled observation destination/budget");
            end = System.nanoTime() + budget.toNanos();
            request =
                    CompatibilityObservationRequest.newBuilder()
                            .setDestinationCell(target)
                            .setNonce(UUID.randomUUID().toString())
                            .build();
            flight = begin(RpcAdmission.Lane.CONTROL, request.getSerializedSize());
            if (flight == null) throw new IllegalStateException("Draining");
        } catch (RuntimeException rejected) {
            return new RpcOperation<>(
                    CompletableFuture.failedFuture(rejected), CompletableFuture.completedFuture(null));
        }
        flight.logical.orTimeout(Math.max(1, end - System.nanoTime()), TimeUnit.NANOSECONDS);
        try {
            var transport = channel(target, RpcAdmission.Lane.CONTROL);
            long remaining = end - System.nanoTime();
            if (remaining <= 0) throw new IllegalStateException("Original observation deadline");
            var stub =
                    CompatibilityObservationIngressGrpc.newStub(transport)
                            .withDeadlineAfter(remaining, TimeUnit.NANOSECONDS);
            flight.opened();
            var terminal = new java.util.concurrent.atomic.AtomicBoolean();
            var observer =
                    new StreamObserver<CompatibilityObservationReply>() {
                        CompatibilityObservationReply received;

                        public void onNext(CompatibilityObservationReply value) {
                            if (received != null || !CompatibilityObservationValidation.valid(request, value))
                                flight.logical.completeExceptionally(
                                        new IllegalStateException("Invalid observation"));
                            else received = value;
                        }

                        public void onError(Throwable error) {
                            if (terminal.compareAndSet(false, true)) {
                                try {
                                    flight.logical.completeExceptionally(error);
                                } finally {
                                    flight.ended();
                                }
                            }
                        }

                        public void onCompleted() {
                            if (terminal.compareAndSet(false, true)) {
                                try {
                                    if (received == null)
                                        flight.logical.completeExceptionally(
                                                new IllegalStateException("Missing observation"));
                                    else flight.logical.complete(received);
                                } finally {
                                    flight.ended();
                                }
                            }
                        }
                    };
            try {
                stub.observeCompatibility(request, observer);
            } catch (RuntimeException ambiguous) {
                flight.logical.completeExceptionally(ambiguous);
            }
        } catch (RuntimeException notDispatched) {
            flight.logical.completeExceptionally(notDispatched);
        } finally {
            flight.ended();
        }
        return flight.operation();
    }

    public synchronized CompletionStage<Void> settleAdmitted() {
        return CompletableFuture.allOf(
                        active.stream().map(f -> f.physical).toArray(CompletableFuture[]::new))
                .minimalCompletionStage();
    }

    /**
     * Permanently stop new streams. Existing bounded streams keep their credit until terminal
     * callbacks.
     */

    public synchronized CompletionStage<Void> drain() {
        if (!closed) {
            closed = true;
            channels.values().forEach(ManagedChannel::shutdown);
            // Capture channels only after original flights (including lazy construction) retire.
            settled.whenComplete(
                    (ignored, failure) ->
                            RpcTransportDrain.await(
                                    settled,
                                    List.copyOf(channels.values()),
                                    drained,
                                    callbacks,
                                    controlCallbacks,
                                    retryTimers));
            if (active.isEmpty()) settled.complete(null);
        }
        return drained.minimalCompletionStage();
    }

    @Override
    public void close() {
        drain();
    }

    public record Endpoint(String host, int port, String tlsAuthority) {
        public Endpoint {
            if (host == null
                    || host.isBlank()
                    || port < 1
                    || port > 65535
                    || tlsAuthority == null
                    || tlsAuthority.isBlank())
                throw new IllegalArgumentException("Invalid trusted RPC endpoint");
        }
    }

    private record ChannelKey(String cell, RpcAdmission.Lane lane) {
    }

    private static final class ChannelSlot {
        ManagedChannel channel;
    }

    private final class Flight<T> {
        final RpcAdmission.Ticket ticket;
        final OperationObservation.Receipt observation;
        final CompletableFuture<T> logical = new CompletableFuture<>();
        final CompletableFuture<Void> physical = new CompletableFuture<>();
        int streams =
                1; // Dispatch sentinel: synchronously completed streams cannot retire unfinished dispatch.

        Flight(RpcAdmission.Ticket ticket, RpcAdmission.Lane lane) {
            this.ticket = ticket;
            observation =
                    OperationObservation.safeStart(
                            observations,
                            lane == RpcAdmission.Lane.RELAY
                                    ? SignalingMetrics.Operation.RPC_RELAY
                                    : SignalingMetrics.Operation.RPC_CONTROL);
            logical.whenComplete(
                    (value, error) -> {
                        String code =
                                value instanceof InternalReply v
                                        ? (v.getErrorCode().isEmpty() ? v.getStatus() : v.getErrorCode())
                                        : value instanceof SessionReply v
                                        ? (v.getErrorCode().isEmpty() ? v.getStatus() : v.getErrorCode())
                                        : value instanceof ControlRouteReply v
                                        ? (v.getErrorCode().isEmpty() ? v.getStatus() : v.getErrorCode())
                                        : "READ";
                        observation.logical(
                                error == null
                                        ? NativeRpcTelemetry.outcome(code)
                                        : SignalingMetrics.Outcome.OUTCOME_UNKNOWN);
                    });
            physical.whenComplete(
                    (v, e) -> {
                        if (e == null) observation.physical(true);
                    });
        }

        synchronized void opened() {
            streams++;
        }

        synchronized void ended() {
            if (--streams != 0) return;
            ticket.close();
            synchronized (CellRpcClient.this) {
                active.remove(this);
                physical.complete(null);
                if (closed && active.isEmpty()) settled.complete(null);
            }
        }

        RpcOperation<T> operation() {
            return new RpcOperation<>(
                    logical.minimalCompletionStage(), physical.minimalCompletionStage());
        }
    }
}
