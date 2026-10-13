package io.webrtc.signaling.gateway;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.StreamReadConstraints;
import com.fasterxml.jackson.core.StreamReadFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.ByteString;
import io.netty.util.ReferenceCountUtil;
import io.webrtc.signaling.auth.AuthorizationStatus;
import io.webrtc.signaling.protocol.Identity.CallId;
import io.webrtc.signaling.protocol.internal.ControlEvent;
import io.webrtc.signaling.protocol.internal.InternalReply;
import io.webrtc.signaling.rpc.CellRpcServer;
import io.webrtc.signaling.rpc.ControlApplicationReceipt;
import io.webrtc.signaling.rpc.RpcOperation;
import io.webrtc.signaling.storage.SessionRepository;

import java.nio.ByteBuffer;
import java.time.Clock;
import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.function.BooleanSupplier;

/**
 * Destination boundary behind authenticated delivery ingress. Write receipt is distinct from
 * application receipt/COMMIT.
 */
public final class GatewayDeliveryStream {
    private static final ObjectMapper JSON =
            new ObjectMapper(
                    JsonFactory.builder()
                            .streamReadConstraints(
                                    StreamReadConstraints.builder().maxNestingDepth(16).maxStringLength(8192).build())
                            .enable(StreamReadFeature.STRICT_DUPLICATE_DETECTION)
                            .build());
    private final ConnectionRegistry registry;
    private final GatewayServices services;
    private final Clock clock;
    private final Executor cpu;
    private final NativeControlReceiptTracker receipts;
    private final BooleanSupplier trustedClock;

    public GatewayDeliveryStream(
            ConnectionRegistry registry, GatewayServices services, Clock clock, Executor cpu) {
        this(registry, services, clock, cpu, new NativeControlReceiptTracker(256, 4194304), () -> true);
    }

    public GatewayDeliveryStream(
            ConnectionRegistry registry,
            GatewayServices services,
            Clock clock,
            Executor cpu,
            NativeControlReceiptTracker receipts,
            BooleanSupplier trustedClock) {
        this.registry = Objects.requireNonNull(registry);
        this.services = Objects.requireNonNull(services);
        this.clock = Objects.requireNonNull(clock);
        this.cpu = Objects.requireNonNull(cpu);
        this.receipts = Objects.requireNonNull(receipts);
        this.trustedClock = Objects.requireNonNull(trustedClock);
    }

    private static RpcOperation<ControlApplicationReceipt> rejected(Throwable error) {
        return new RpcOperation<>(
                CompletableFuture.failedFuture(error), CompletableFuture.completedFuture(null));
    }

    private static UUID uuid(ByteString bytes) {
        if (bytes.size() != 16) throw new IllegalArgumentException("Invalid delivery identity");
        ByteBuffer buffer = bytes.asReadOnlyByteBuffer();
        return new UUID(buffer.getLong(), buffer.getLong());
    }

    private static InternalReply reply(ControlEvent event, String status, String error) {
        return InternalReply.newBuilder()
                .setOperationId(event.getEventId())
                .setCallId(event.getCallId())
                .setCallVersion(event.getCallVersion())
                .setStatus(status)
                .setErrorCode(error)
                .build();
    }

    /**
     * Compatibility adapter; production uses the tracked authenticated ingress below.
     */
    public CompletionStage<InternalReply> send(ControlEvent event) {
        var binding = registry.binding(uuid(event.getDestination().getConnectionId()));
        if (!current(binding, event))
            return CompletableFuture.completedFuture(reply(event, "REJECTED", "STALE_BINDING"));
        return sendTracked(event, binding.route(), Duration.ofSeconds(2))
                .logical()
                .thenApply(value -> reply(event, "APPLICATION_RECEIVED", ""))
                .exceptionally(error -> reply(event, "REJECTED", "OUTCOME_UNKNOWN"));
    }

    public RpcOperation<ControlApplicationReceipt> send(
            ControlEvent event, SessionRepository.Route route, CellRpcServer.Peer peer, Duration budget) {
        try {
            if (peer == null
                    || !peer.role().equals("actor")
                    || peer.workloadId().isBlank()
                    || !peer.cell().equals(new CallId(event.getCallId()).coordinatorCell()))
                return rejected(new IllegalStateException("UNAUTHORIZED"));
            return sendTracked(event, route, budget);
        } catch (RuntimeException invalid) {
            return rejected(invalid);
        }
    }

    private RpcOperation<ControlApplicationReceipt> sendTracked(
            ControlEvent event, SessionRepository.Route route, Duration budget) {
        final long end;
        final ConnectionRegistry.Binding binding;
        final NativeControlReceiptTracker.Pending issued;
        try {
            if (budget == null
                    || budget.isNegative()
                    || budget.isZero()
                    || budget.compareTo(Duration.ofSeconds(2)) > 0
                    || event.getCallVersion() < 1
                    || event.getMetadata().size() > 8192
                    || !event.getType().matches("[A-Z_]{1,64}")
                    || event.getAuthorityBucketId() < 0
                    || event.getAuthorityBucketId() > 16383)
                throw new IllegalArgumentException("INVALID_MESSAGE");
            end = System.nanoTime() + budget.toNanos();
            binding = registry.binding(route.connectionId());
            if (!current(binding, event)
                    || !binding.route().equals(route)
                    || binding.channel().pipeline().get(OutboundAdmissionHandler.class) == null)
                throw new IllegalStateException("STALE_BINDING");
            issued =
                    receipts.issue(
                            event, route, end - System.nanoTime(), () -> issuedRouteCurrent(route, event));
        } catch (RuntimeException invalid) {
            return rejected(invalid);
        }
        var dispatch = new CompletableFuture<Void>();
        var socket = new CompletableFuture<Void>();
        CompletableFuture.allOf(dispatch, socket).thenRun(issued::sendFinished);
        try {
            cpu.execute(
                    () -> {
                        try {
                            if (end - System.nanoTime() <= 0) throw new TimeoutException();
                            var payload = JSON.readTree(event.getMetadata().toByteArray());
                            if (payload == null || !payload.isObject()) throw new IllegalArgumentException();
                            String encoded =
                                    JSON.writeValueAsString(
                                            Map.of(
                                                    "v",
                                                    1,
                                                    "type",
                                                    event.getType(),
                                                    "eventId",
                                                    event.getEventId(),
                                                    "callId",
                                                    event.getCallId(),
                                                    "callVersion",
                                                    Long.toString(event.getCallVersion()),
                                                    "sessionIncarnation",
                                                    route.incarnation().value().toString(),
                                                    "connectionGeneration",
                                                    Long.toString(route.connectionGeneration()),
                                                    "payload",
                                                    payload));
                            var frame = new OutboundAdmissionHandler.ControlFrame(encoded);
                            Runnable write =
                                    () -> {
                                        final boolean allowed;
                                        try {
                                            allowed = end - System.nanoTime() > 0 && issuedRouteCurrent(route, event);
                                        } catch (RuntimeException unavailable) {
                                            ReferenceCountUtil.release(frame);
                                            socket.complete(null);
                                            issued.failed(unavailable);
                                            return;
                                        }
                                        if (!allowed) {
                                            ReferenceCountUtil.release(frame);
                                            socket.complete(null);
                                            issued.failed(new IllegalStateException("STALE_BINDING"));
                                            return;
                                        }
                                        try {
                                            frame
                                                    .physicalCompletion()
                                                    .thenRun(
                                                            () -> {
                                                                issued.writeCleaned();
                                                                socket.complete(null);
                                                            });
                                            binding
                                                    .channel()
                                                    .writeAndFlush(frame)
                                                    .addListener(
                                                            done -> {
                                                                if (done.isSuccess()) issued.writeSucceeded();
                                                                else
                                                                    issued.failed(new IllegalStateException("RESYNC_REQUIRED"));
                                                            });
                                        } catch (Throwable unknown) {
                                            // Once dispatch may have started, only the frame's physical receipt retires
                                            // it.
                                            issued.failed(unknown);
                                        }
                                    };
                            try {
                                binding.channel().eventLoop().execute(write);
                            } catch (RejectedExecutionException notStarted) {
                                ReferenceCountUtil.release(frame);
                                socket.complete(null);
                                issued.failed(notStarted);
                            }
                        } catch (Exception invalid) {
                            socket.complete(null);
                            issued.failed(invalid);
                        } finally {
                            dispatch.complete(null);
                        }
                    });
        } catch (RejectedExecutionException notStarted) {
            issued.failed(notStarted);
            dispatch.complete(null);
            socket.complete(null);
        }
        return issued.operation();
    }

    public CompletionStage<Void> drain() {
        return receipts.drain();
    }

    private boolean issuedRouteCurrent(SessionRepository.Route route, ControlEvent event) {
        var binding = registry.binding(route.connectionId());
        return current(binding, event) && route.equals(binding.route());
    }

    private boolean current(ConnectionRegistry.Binding binding, ControlEvent event) {
        if (binding == null
                || !trustedClock.getAsBoolean()
                || !clock.instant().isBefore(binding.principal().expiresAt())
                || !services.currentBoot()
                || services.cachedSecurity(binding.principal(), clock.instant())
                != AuthorizationStatus.ALLOWED) return false;
        var route = binding.route();
        var destination = event.getDestination();
        return registry.current(route.connectionId(), route)
                && route.user().value().equals(destination.getUserId())
                && route.key().issuer().equals(destination.getIssuer())
                && route.key().jti().equals(destination.getJti())
                && route.incarnation().value().equals(uuid(destination.getIncarnation()))
                && route.connectionGeneration() == destination.getConnectionGeneration()
                && route.connectionId().equals(uuid(destination.getConnectionId()));
    }
}
