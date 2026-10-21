package io.webrtc.signaling.rpc;

import io.grpc.MethodDescriptor;
import io.grpc.ServiceDescriptor;
import io.webrtc.signaling.protocol.internal.CellIngressGrpc;
import io.webrtc.signaling.protocol.internal.CompatibilityObservationIngressGrpc;
import io.webrtc.signaling.protocol.internal.ControlRouteIngressGrpc;
import io.webrtc.signaling.protocol.internal.SessionIngressGrpc;

import java.util.List;
import java.util.Map;

/**
 * Which authenticated workload role may use which surface of the actor cell-RPC server. Roles come
 * only from {@link RpcTlsIdentity}. Every served method has a {@link Surface}, and every role's
 * decision is an exhaustive switch, so a new operation does not compile without one.
 *
 * <p>The {@code control} role exists only so the control plane can observe actor compatibility for
 * its readiness gate: it is admitted for {@link Surface#COMPATIBILITY_OBSERVATION} and refused for
 * everything else, already at the transport, before any handler runs.
 */
final class CellRpcAuthorization {
    static final String ACTOR = "actor";
    static final String GATEWAY = "gateway";
    static final String CONTROL = "control";
    /** Every service the actor cell-RPC server serves. */
    static final List<ServiceDescriptor> SERVICES =
            List.of(
                    CellIngressGrpc.getServiceDescriptor(),
                    SessionIngressGrpc.getServiceDescriptor(),
                    ControlRouteIngressGrpc.getServiceDescriptor(),
                    CompatibilityObservationIngressGrpc.getServiceDescriptor());
    private static final Map<String, Surface> METHODS =
            Map.of(
                    name(CellIngressGrpc.getReserveUserMethod()), Surface.RESERVE,
                    name(CellIngressGrpc.getClaimAcceptMethod()), Surface.CLAIM,
                    name(CellIngressGrpc.getReleaseIfCallVersionMethod()), Surface.RELEASE,
                    name(CellIngressGrpc.getExecuteCallCommandMethod()), Surface.EXECUTE,
                    name(CellIngressGrpc.getDeliverControlEventMethod()), Surface.DELIVER,
                    name(CellIngressGrpc.getRelayNegotiationMethod()), Surface.RELAY,
                    name(CellIngressGrpc.getSyncCallMethod()), Surface.SYNC,
                    name(SessionIngressGrpc.getMutateSessionMethod()), Surface.SESSION,
                    name(ControlRouteIngressGrpc.getResolveControlRouteMethod()), Surface.CONTROL_ROUTE,
                    name(CompatibilityObservationIngressGrpc.getObserveCompatibilityMethod()),
                    Surface.COMPATIBILITY_OBSERVATION);

    private CellRpcAuthorization() {
    }

    private static String name(MethodDescriptor<?, ?> method) {
        return method.getFullMethodName();
    }

    /** The surface of a served method, or {@code null} for a method this server does not serve. */
    static Surface surface(String fullMethodName) {
        return fullMethodName == null ? null : METHODS.get(fullMethodName);
    }

    static Surface surface(CellRpcServer.Operation operation) {
        return switch (operation) {
            case RESERVE -> Surface.RESERVE;
            case CLAIM -> Surface.CLAIM;
            case RELEASE -> Surface.RELEASE;
            case EXECUTE -> Surface.EXECUTE;
            case DELIVER -> Surface.DELIVER;
            case RELAY -> Surface.RELAY;
            case SYNC -> Surface.SYNC;
        };
    }

    /**
     * The role decision for one surface. Actor and gateway decisions are the ones the server already
     * made; handlers behind {@code SESSION} and {@code CONTROL_ROUTE} still check the role themselves.
     */
    static boolean admits(CellRpcServer.Peer peer, Surface surface) {
        if (peer == null || surface == null) return false;
        return switch (peer.role()) {
            case ACTOR -> true;
            case GATEWAY -> switch (surface) {
                case EXECUTE, RELAY, SYNC, SESSION, CONTROL_ROUTE, COMPATIBILITY_OBSERVATION -> true;
                case RESERVE, CLAIM, RELEASE, DELIVER -> false;
            };
            case CONTROL -> switch (surface) {
                case COMPATIBILITY_OBSERVATION -> true;
                case RESERVE, CLAIM, RELEASE, EXECUTE, DELIVER, RELAY, SYNC, SESSION, CONTROL_ROUTE -> false;
            };
            default -> false;
        };
    }

    /**
     * The transport gate, applied to every call before its handler. A control peer reaches only the
     * methods its role admits (so an unknown or future method is refused); actor and gateway calls
     * proceed to their per-surface decisions unchanged.
     */
    static boolean admitsCall(CellRpcServer.Peer peer, String fullMethodName) {
        if (peer == null) return false;
        return switch (peer.role()) {
            case ACTOR, GATEWAY -> true;
            case CONTROL -> admits(peer, surface(fullMethodName));
            default -> false;
        };
    }

    enum Surface {
        RESERVE,
        CLAIM,
        RELEASE,
        EXECUTE,
        DELIVER,
        RELAY,
        SYNC,
        SESSION,
        CONTROL_ROUTE,
        COMPATIBILITY_OBSERVATION
    }
}
