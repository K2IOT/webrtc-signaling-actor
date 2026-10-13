package io.webrtc.signaling.app.runtime;

import io.netty.handler.ssl.SslContext;
import io.webrtc.signaling.actors.relay.RelayBufferBudget;
import io.webrtc.signaling.protocol.internal.ControlEvent;
import io.webrtc.signaling.protocol.internal.InternalReply;
import io.webrtc.signaling.rpc.*;

import java.util.concurrent.CompletionStage;
import java.util.function.BiPredicate;
import java.util.function.Function;

/**
 * Explicit internal listener, outbound transport and workload-authority enrollment.
 */
public record NativeActorIngressEnrollment(
        String environment,
        int port,
        SslContext tls,
        RpcAdmission admission,
        NativeSagaEffects.Network network,
        int relayCapacity,
        RelayBufferBudget relayMemory,
        GatewayRelayRpcClient gateways,
        BiPredicate<CellRpcServer.Peer, NativeSessionHandler.GatewayIdentity> gatewayWorkloads,
        Function<ControlEvent, CompletionStage<InternalReply>> delivery) {
    /**
     * Native route/control RPCs are installed by the composition; no callback is a receipt authority.
     */
    public NativeActorIngressEnrollment(
            String environment,
            int port,
            SslContext tls,
            RpcAdmission admission,
            NativeSagaEffects.Network network,
            int relayCapacity,
            RelayBufferBudget relayMemory,
            GatewayRelayRpcClient gateways,
            BiPredicate<CellRpcServer.Peer, NativeSessionHandler.GatewayIdentity> gatewayWorkloads) {
        this(
                environment,
                port,
                tls,
                admission,
                network,
                relayCapacity,
                relayMemory,
                gateways,
                gatewayWorkloads,
                null);
    }
}
