package io.webrtc.signaling.rpc;

import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.NettyChannelBuilder;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslProvider;
import io.webrtc.signaling.protocol.internal.CellIngressGrpc;
import io.webrtc.signaling.protocol.internal.CompatibilityObservationIngressGrpc;
import io.webrtc.signaling.protocol.internal.CompatibilityObservationReply;
import io.webrtc.signaling.protocol.internal.CompatibilityObservationRequest;
import io.webrtc.signaling.protocol.internal.ControlEvent;
import io.webrtc.signaling.protocol.internal.ControlRouteIngressGrpc;
import io.webrtc.signaling.protocol.internal.ControlRouteRequest;
import io.webrtc.signaling.protocol.internal.GatewayControlIngressGrpc;
import io.webrtc.signaling.protocol.internal.GatewayControlRequest;
import io.webrtc.signaling.protocol.internal.GatewayRelayIngressGrpc;
import io.webrtc.signaling.protocol.internal.GatewayRelayRequest;
import io.webrtc.signaling.protocol.internal.InternalCommand;
import io.webrtc.signaling.protocol.internal.SessionCommand;
import io.webrtc.signaling.protocol.internal.SessionIngressGrpc;
import org.assertj.core.api.ThrowableAssert.ThrowingCallable;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.file.Path;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowableOfType;

/**
 * TEST_ONLY certificates over real TLS 1.3: the control workload identity may observe actor
 * compatibility and nothing else, and it can never stand in for an actor or gateway identity.
 */
class ControlRpcIdentityIT {
    private static final String ENV = "test";
    private static final String SPIFFE = "spiffe://signaling/" + ENV + "/cell/";
    @TempDir
    static Path dir;
    private static RpcTestPki pki;
    private static RpcTestPki.Leaf actor, gateway, control, otherCellControl, controlAndActor;

    @BeforeAll
    static void issue() throws Exception {
        pki = RpcTestPki.create(dir);
        actor = pki.leaf("actor", "URI:" + SPIFFE + "c001/actor,DNS:localhost");
        gateway = pki.leaf("gateway", "URI:" + SPIFFE + "c001/gateway/gw-1,DNS:localhost");
        control = pki.leaf("control", "URI:" + SPIFFE + "c001/control,DNS:localhost");
        otherCellControl = pki.leaf("control-c002", "URI:" + SPIFFE + "c002/control,DNS:localhost");
        controlAndActor =
                pki.leaf(
                        "control-actor",
                        "URI:" + SPIFFE + "c001/control,URI:" + SPIFFE + "c001/actor,DNS:localhost");
    }

    private static File f(Path path) {
        return path.toFile();
    }

    private static RpcAdmission admission() {
        return new RpcAdmission(8, 1048576, 8, 1048576);
    }

    private static CompatibilityObservationReply facts(String gatewayId, String boot) {
        long now = Instant.now().toEpochMilli();
        return CompatibilityObservationReply.newBuilder()
                .setSourceCell("c001")
                .setGatewayId(gatewayId)
                .setBootId(boot)
                .setObservedAtMs(now)
                .setValidUntilMs(now + 4000)
                .build();
    }

    private static ManagedChannel channel(int port, SslContext tls) {
        return NettyChannelBuilder.forAddress("localhost", port)
                .overrideAuthority("localhost")
                .sslContext(tls)
                .disableRetry()
                .build();
    }

    private static void unauthorized(ThrowingCallable call) {
        var refused = catchThrowableOfType(StatusRuntimeException.class, call);
        assertThat(refused).isNotNull();
        assertThat(refused.getStatus().getCode()).isEqualTo(Status.Code.PERMISSION_DENIED);
        assertThat(refused.getStatus().getDescription()).isEqualTo("UNAUTHORIZED");
    }

    @Test
    void controlLeafIsParsedAsItsOwnRoleAndNeverAsActorOrGateway() throws Exception {
        assertThatThrownBy(
                () ->
                        RpcTlsContexts.server(
                                ENV, "c001", f(pki.ca()), f(control.certificate()), f(control.key())))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(
                () ->
                        RpcTlsContexts.gatewayServer(
                                ENV,
                                "c001",
                                "gw-1",
                                f(pki.ca()),
                                f(control.certificate()),
                                f(control.key())))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(
                () ->
                        RpcTlsContexts.gatewayClients(
                                ENV, f(pki.ca()), f(control.certificate()), f(control.key())))
                .isInstanceOf(IllegalArgumentException.class);
        assertThat(
                RpcTlsContexts.controlClients(
                        ENV, "c001", f(pki.ca()), f(control.certificate()), f(control.key())))
                .isNotNull();
    }

    @Test
    void controlClientsRequireExactlyThisCellsControlLeaf() {
        for (var leaf : new RpcTestPki.Leaf[]{actor, gateway, controlAndActor})
            assertThatThrownBy(
                    () ->
                            RpcTlsContexts.controlClients(
                                    ENV, "c001", f(pki.ca()), f(leaf.certificate()), f(leaf.key())))
                    .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(
                () ->
                        RpcTlsContexts.controlClients(
                                ENV,
                                "c001",
                                f(pki.ca()),
                                f(otherCellControl.certificate()),
                                f(otherCellControl.key())))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void actorServerAnswersControlOnlyForCompatibilityObservation() throws Exception {
        var backendCalls = new AtomicInteger();
        var deliveries = new AtomicInteger();
        var server =
                new CellRpcServer(
                        "c001",
                        ENV,
                        0,
                        RpcTlsContexts.server(
                                ENV, "c001", f(pki.ca()), f(actor.certificate()), f(actor.key())),
                        admission(),
                        (operation, command, peer, budget) -> {
                            backendCalls.incrementAndGet();
                            return new CompletableFuture<>();
                        },
                        event -> {
                            deliveries.incrementAndGet();
                            return new CompletableFuture<>();
                        })
                        .compatibilityFacts(() -> facts("", ""))
                        .start();
        var controlTls =
                RpcTlsContexts.controlClients(
                                ENV, "c001", f(pki.ca()), f(control.certificate()), f(control.key()))
                        .context("c001");
        var actorTls =
                RpcTlsContexts.clients(ENV, f(pki.ca()), f(actor.certificate()), f(actor.key()))
                        .context("c001");
        var controlChannel = channel(server.port(), controlTls);
        var actorChannel = channel(server.port(), actorTls);
        try {
            var nonce = UUID.randomUUID().toString();
            var request =
                    CompatibilityObservationRequest.newBuilder()
                            .setDestinationCell("c001")
                            .setNonce(nonce)
                            .build();
            var observed =
                    CompatibilityObservationIngressGrpc.newBlockingStub(controlChannel)
                            .withDeadlineAfter(3, TimeUnit.SECONDS)
                            .observeCompatibility(request);
            assertThat(observed.getNonce()).isEqualTo(nonce);
            assertThat(observed.getSourceCell()).isEqualTo("c001");
            // The existing actor caller is unchanged.
            assertThat(
                    CompatibilityObservationIngressGrpc.newBlockingStub(actorChannel)
                            .withDeadlineAfter(3, TimeUnit.SECONDS)
                            .observeCompatibility(request)
                            .getNonce())
                    .isEqualTo(nonce);

            var cell = CellIngressGrpc.newBlockingStub(controlChannel).withDeadlineAfter(3, TimeUnit.SECONDS);
            var command =
                    InternalCommand.newBuilder()
                            .setOperationId(UUID.randomUUID().toString())
                            .setDestinationCell("c001")
                            .build();
            unauthorized(() -> cell.reserveUser(command));
            unauthorized(() -> cell.claimAccept(command));
            unauthorized(() -> cell.releaseIfCallVersion(command));
            unauthorized(() -> cell.executeCallCommand(command));
            unauthorized(() -> cell.relayNegotiation(command));
            unauthorized(() -> cell.syncCall(command));
            unauthorized(
                    () ->
                            cell.deliverControlEvent(
                                    ControlEvent.newBuilder().setEventId(UUID.randomUUID().toString()).build()));
            unauthorized(
                    () ->
                            SessionIngressGrpc.newBlockingStub(controlChannel)
                                    .withDeadlineAfter(3, TimeUnit.SECONDS)
                                    .mutateSession(
                                            SessionCommand.newBuilder()
                                                    .setOperationId(UUID.randomUUID().toString())
                                                    .build()));
            unauthorized(
                    () ->
                            ControlRouteIngressGrpc.newBlockingStub(controlChannel)
                                    .withDeadlineAfter(3, TimeUnit.SECONDS)
                                    .resolveControlRoute(
                                            ControlRouteRequest.newBuilder()
                                                    .setEventId(UUID.randomUUID().toString())
                                                    .build()));
            assertThat(backendCalls).hasValue(0);
            assertThat(deliveries).hasValue(0);
        } finally {
            controlChannel.shutdownNow().awaitTermination(3, TimeUnit.SECONDS);
            actorChannel.shutdownNow().awaitTermination(3, TimeUnit.SECONDS);
            server.drain().toCompletableFuture().get(10, TimeUnit.SECONDS);
        }
    }

    @Test
    void gatewayRelayServerRefusesTheControlRoleEntirely() throws Exception {
        var boot = UUID.randomUUID();
        var sends = new AtomicInteger();
        var server =
                new GatewayRelayRpcServer(
                        "c001",
                        "gw-1",
                        boot,
                        ENV,
                        0,
                        RpcTlsContexts.gatewayServer(
                                ENV,
                                "c001",
                                "gw-1",
                                f(pki.ca()),
                                f(gateway.certificate()),
                                f(gateway.key())),
                        admission(),
                        (delivery, peer, budget) -> {
                            sends.incrementAndGet();
                            return new RpcOperation<>(new CompletableFuture<>(), new CompletableFuture<>());
                        })
                        .compatibilityFacts(() -> facts("gw-1", boot.toString()))
                        .start();
        // A client that presents the control leaf and pins nothing: refusal must come from the server.
        var tls =
                GrpcSslContexts.forClient()
                        .sslProvider(SslProvider.JDK)
                        .trustManager(f(pki.ca()))
                        .keyManager(f(control.certificate()), f(control.key()))
                        .protocols("TLSv1.3")
                        .build();
        var channel = channel(server.port(), tls);
        try {
            unauthorized(
                    () ->
                            CompatibilityObservationIngressGrpc.newBlockingStub(channel)
                                    .withDeadlineAfter(3, TimeUnit.SECONDS)
                                    .observeCompatibility(
                                            CompatibilityObservationRequest.newBuilder()
                                                    .setDestinationCell("c001")
                                                    .setGatewayId("gw-1")
                                                    .setBootId(boot.toString())
                                                    .setNonce(UUID.randomUUID().toString())
                                                    .build()));
            unauthorized(
                    () ->
                            GatewayRelayIngressGrpc.newBlockingStub(channel)
                                    .withDeadlineAfter(3, TimeUnit.SECONDS)
                                    .deliverRelay(
                                            GatewayRelayRequest.newBuilder()
                                                    .setOperationId(UUID.randomUUID().toString())
                                                    .build()));
            unauthorized(
                    () ->
                            GatewayControlIngressGrpc.newBlockingStub(channel)
                                    .withDeadlineAfter(3, TimeUnit.SECONDS)
                                    .deliverControl(GatewayControlRequest.getDefaultInstance()));
            assertThat(sends).hasValue(0);
        } finally {
            channel.shutdownNow().awaitTermination(3, TimeUnit.SECONDS);
            server.drain().toCompletableFuture().get(10, TimeUnit.SECONDS);
        }
    }
}
