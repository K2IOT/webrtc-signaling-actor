package io.webrtc.signaling.rpc;

import io.grpc.Context;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import io.webrtc.signaling.protocol.internal.CompatibilityObservationIngressGrpc;
import io.webrtc.signaling.protocol.internal.CompatibilityObservationReply;
import io.webrtc.signaling.protocol.internal.CompatibilityObservationRequest;

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;
import java.util.function.Supplier;

/**
 * Read-only, cached native facts. Authentication is inherited from the server mTLS interceptor.
 */
final class CompatibilityObservationService
        extends CompatibilityObservationIngressGrpc.CompatibilityObservationIngressImplBase {
    private final String cell, gateway, boot;
    private final RpcAdmission admission;
    private final Supplier<CellRpcServer.Peer> peer;
    private final Supplier<CompatibilityObservationReply> source;

    CompatibilityObservationService(
            String cell,
            String gateway,
            String boot,
            RpcAdmission admission,
            Supplier<CellRpcServer.Peer> peer,
            Supplier<CompatibilityObservationReply> source) {
        this.cell = cell;
        this.gateway = gateway;
        this.boot = boot;
        this.admission = admission;
        this.peer = peer;
        this.source = source;
    }

    @Override
    public void observeCompatibility(
            CompatibilityObservationRequest request,
            StreamObserver<CompatibilityObservationReply> reply) {
        RpcAdmission.Ticket ticket = null;
        try {
            var caller = peer.get();
            var deadline = Context.current().getDeadline();
            // Identity extraction already checks environment and the trusted actor/gateway role catalog.
            if (caller == null || !(caller.role().equals("actor") || caller.role().equals("gateway")))
                throw Status.PERMISSION_DENIED.asRuntimeException();
            if (deadline == null
                    || deadline.isExpired()
                    || request.getSerializedSize() > 1024
                    || !cell.equals(request.getDestinationCell())
                    || !gateway.equals(request.getGatewayId())
                    || !boot.equals(request.getBootId())) throw Status.INVALID_ARGUMENT.asRuntimeException();
            UUID.fromString(request.getNonce());
            ticket = admission.acquire(RpcAdmission.Lane.CONTROL, request.getSerializedSize());
            var facts = Objects.requireNonNull(source.get());
            long now = Instant.now().toEpochMilli();
            if (!cell.equals(facts.getSourceCell())
                    || !gateway.equals(facts.getGatewayId())
                    || !boot.equals(facts.getBootId())
                    || facts.getActorsCount() > 7
                    || facts.getSerializedSize() > 16384
                    || facts.getObservedAtMs() > now
                    || facts.getObservedAtMs() < now - 5000
                    || facts.getValidUntilMs() <= now
                    || facts.getValidUntilMs() > facts.getObservedAtMs() + 5000)
                throw Status.UNAVAILABLE.asRuntimeException();
            reply.onNext(facts.toBuilder().setNonce(request.getNonce()).build());
            reply.onCompleted();
        } catch (RuntimeException unavailable) {
            reply.onError(
                    Status.fromThrowable(unavailable).getCode() == Status.Code.UNKNOWN
                            ? Status.UNAVAILABLE.asRuntimeException()
                            : unavailable);
        } finally {
            // The producer only reads immutable cached facts; its synchronous return is physical
            // completion.
            if (ticket != null) ticket.close();
        }
    }
}
