package io.webrtc.signaling.rpc;

import io.grpc.MethodDescriptor;
import io.webrtc.signaling.protocol.internal.CellIngressGrpc;
import io.webrtc.signaling.protocol.internal.CompatibilityObservationIngressGrpc;
import io.webrtc.signaling.protocol.internal.ControlRouteIngressGrpc;
import io.webrtc.signaling.protocol.internal.GatewayControlIngressGrpc;
import io.webrtc.signaling.protocol.internal.GatewayRelayIngressGrpc;
import io.webrtc.signaling.protocol.internal.SessionIngressGrpc;
import org.junit.jupiter.api.Test;

import java.util.EnumSet;
import java.util.List;

import static io.webrtc.signaling.rpc.CellRpcAuthorization.Surface;
import static org.assertj.core.api.Assertions.assertThat;

class CellRpcAuthorizationTest {
    private static final CellRpcServer.Peer ACTOR = new CellRpcServer.Peer("c001", "actor");
    private static final CellRpcServer.Peer GATEWAY = new CellRpcServer.Peer("c001", "gateway", "gw-1");
    private static final CellRpcServer.Peer CONTROL = new CellRpcServer.Peer("c001", "control");
    private static final CellRpcServer.Peer NAMED_CONTROL =
            new CellRpcServer.Peer("c002", "control", "control-1");
    private static final String OBSERVE =
            CompatibilityObservationIngressGrpc.getObserveCompatibilityMethod().getFullMethodName();

    private static List<String> servedMethods() {
        return CellRpcAuthorization.SERVICES.stream()
                .flatMap(service -> service.getMethods().stream())
                .map(MethodDescriptor::getFullMethodName)
                .toList();
    }

    @Test
    void everyServedMethodHasAnExplicitSurface() {
        // A new rpc in a served service fails here until it is given a surface and a role decision.
        assertThat(servedMethods()).hasSize(Surface.values().length);
        for (var method : servedMethods()) assertThat(CellRpcAuthorization.surface(method)).isNotNull();
        assertThat(servedMethods().stream().map(CellRpcAuthorization::surface).distinct())
                .containsExactlyInAnyOrder(Surface.values());
    }

    @Test
    void servedServicesAreExactlyTheCellIngressSurfaces() {
        assertThat(CellRpcAuthorization.SERVICES)
                .containsExactly(
                        CellIngressGrpc.getServiceDescriptor(),
                        SessionIngressGrpc.getServiceDescriptor(),
                        ControlRouteIngressGrpc.getServiceDescriptor(),
                        CompatibilityObservationIngressGrpc.getServiceDescriptor());
    }

    @Test
    void everyOperationMapsToItsOwnSurface() {
        for (var operation : CellRpcServer.Operation.values())
            assertThat(CellRpcAuthorization.surface(operation).name()).isEqualTo(operation.name());
    }

    @Test
    void controlIsAdmittedOnlyForCompatibilityObservation() {
        for (var surface : Surface.values()) {
            boolean expected = surface == Surface.COMPATIBILITY_OBSERVATION;
            assertThat(CellRpcAuthorization.admits(CONTROL, surface)).as(surface.name()).isEqualTo(expected);
            assertThat(CellRpcAuthorization.admits(NAMED_CONTROL, surface))
                    .as(surface.name())
                    .isEqualTo(expected);
        }
    }

    @Test
    void controlIsRefusedForEveryBusinessOperation() {
        for (var operation : CellRpcServer.Operation.values())
            assertThat(CellRpcAuthorization.admits(CONTROL, CellRpcAuthorization.surface(operation)))
                    .as(operation.name())
                    .isFalse();
    }

    @Test
    void controlTransportReachesOnlyTheObservationMethod() {
        assertThat(CellRpcAuthorization.admitsCall(CONTROL, OBSERVE)).isTrue();
        for (var method : servedMethods())
            if (!method.equals(OBSERVE))
                assertThat(CellRpcAuthorization.admitsCall(CONTROL, method)).as(method).isFalse();
        // Methods this server does not serve (including the gateway-side ones) are refused too.
        for (var method :
                List.of(
                        GatewayRelayIngressGrpc.getDeliverRelayMethod().getFullMethodName(),
                        GatewayControlIngressGrpc.getDeliverControlMethod().getFullMethodName(),
                        "signaling.internal.CellIngress/FutureOperation"))
            assertThat(CellRpcAuthorization.admitsCall(CONTROL, method)).as(method).isFalse();
    }

    @Test
    void actorAndGatewayKeepTheirExistingDecisions() {
        for (var surface : Surface.values())
            assertThat(CellRpcAuthorization.admits(ACTOR, surface)).as(surface.name()).isTrue();
        var gateway =
                EnumSet.of(
                        Surface.EXECUTE,
                        Surface.RELAY,
                        Surface.SYNC,
                        Surface.SESSION,
                        Surface.CONTROL_ROUTE,
                        Surface.COMPATIBILITY_OBSERVATION);
        for (var surface : Surface.values())
            assertThat(CellRpcAuthorization.admits(GATEWAY, surface))
                    .as(surface.name())
                    .isEqualTo(gateway.contains(surface));
        // The transport gate leaves actor and gateway calls to the per-surface handlers.
        for (var method : servedMethods()) {
            assertThat(CellRpcAuthorization.admitsCall(ACTOR, method)).isTrue();
            assertThat(CellRpcAuthorization.admitsCall(GATEWAY, method)).isTrue();
        }
    }

    @Test
    void unauthenticatedOrUnknownRolesAreRefusedEverywhere() {
        var outsider = new CellRpcServer.Peer("c001", "outsider");
        for (var surface : Surface.values()) {
            assertThat(CellRpcAuthorization.admits(null, surface)).isFalse();
            assertThat(CellRpcAuthorization.admits(outsider, surface)).isFalse();
        }
        assertThat(CellRpcAuthorization.admitsCall(null, OBSERVE)).isFalse();
        assertThat(CellRpcAuthorization.admitsCall(outsider, OBSERVE)).isFalse();
    }

    @Test
    void gatewayRelayServerAdmitsOnlyNamedActors() {
        assertThat(GatewayRelayRpcServer.admits(new CellRpcServer.Peer("c001", "actor", "a-1"))).isTrue();
        assertThat(GatewayRelayRpcServer.admits(ACTOR)).isFalse();
        assertThat(GatewayRelayRpcServer.admits(GATEWAY)).isFalse();
        assertThat(GatewayRelayRpcServer.admits(CONTROL)).isFalse();
        assertThat(GatewayRelayRpcServer.admits(NAMED_CONTROL)).isFalse();
        assertThat(GatewayRelayRpcServer.admits(null)).isFalse();
    }
}
