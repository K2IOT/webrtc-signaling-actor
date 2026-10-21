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
import java.util.function.Predicate;
import java.util.function.Supplier;

/**
 * Read-only, cached native facts. Authentication is inherited from the server mTLS interceptor.
 */
final class CompatibilityObservationService
        extends CompatibilityObservationIngressGrpc.CompatibilityObservationIngressImplBase {
    private final String cell, gateway, boot;
    private final RpcAdmission admission;
    private final Supplier<CellRpcServer.Peer> peer;
    private final Predicate<CellRpcServer.Peer> admitted;
    private final Supplier<CompatibilityObservationReply> source;

    CompatibilityObservationService(
            String cell,
            String gateway,
            String boot,
            RpcAdmission admission,
            Supplier<CellRpcServer.Peer> peer,
            Predicate<CellRpcServer.Peer> admitted,
            Supplier<CompatibilityObservationReply> source) {
        this.cell = cell;
        this.gateway = gateway;
        this.boot = boot;
        this.admission = admission;
        this.peer = peer;
        this.admitted = Objects.requireNonNull(admitted);
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
            // Identity extraction checks environment and the role catalog; the hosting server decides
            // which of those roles may observe it.
            if (caller == null || !admitted.test(caller))
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
