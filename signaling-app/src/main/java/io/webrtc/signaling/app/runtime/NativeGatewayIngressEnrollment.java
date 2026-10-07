package io.webrtc.signaling.app.runtime;

import io.netty.handler.ssl.SslContext;
import io.webrtc.signaling.gateway.EdgeAdmission;
import io.webrtc.signaling.gateway.GatewayServer;
import io.webrtc.signaling.gateway.GatewaySecuritySweep;
import io.webrtc.signaling.rpc.RpcAdmission;
import java.net.InetSocketAddress;
import java.util.Objects;

/** Explicit WSS and internal mTLS ingress; neither listener gets a plaintext fallback. */
public record NativeGatewayIngressEnrollment(InetSocketAddress wssAddress, SslContext wssTls,
        GatewayServer.UpgradePolicy upgradePolicy, int eventLoops, int maxUnauthenticated, int maxConnections,
        EdgeAdmission.Limits edgeLimits, String rpcEnvironment, int rpcPort, SslContext rpcTls,
        RpcAdmission rpcAdmission, RpcAdmission writeAdmission, GatewaySecuritySweep.Settings securitySweep) {
    public NativeGatewayIngressEnrollment {
        Objects.requireNonNull(wssAddress); Objects.requireNonNull(wssTls); Objects.requireNonNull(upgradePolicy);
        Objects.requireNonNull(edgeLimits); Objects.requireNonNull(rpcTls);
        Objects.requireNonNull(rpcAdmission); Objects.requireNonNull(writeAdmission); Objects.requireNonNull(securitySweep);
        if(wssAddress.isUnresolved() || eventLoops<1 || eventLoops>64 || maxUnauthenticated<1
                || maxUnauthenticated>1000 || maxConnections<maxUnauthenticated || maxConnections>200000
                || rpcEnvironment==null || !rpcEnvironment.matches("[a-z0-9-]{1,32}") || rpcPort<0 || rpcPort>65535)
            throw new IllegalArgumentException("Invalid native gateway ingress bounds");
    }
}
