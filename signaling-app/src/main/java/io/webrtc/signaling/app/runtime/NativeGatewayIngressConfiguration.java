package io.webrtc.signaling.app.runtime;

import io.webrtc.signaling.auth.ClockSafetyMonitor;
import io.webrtc.signaling.gateway.ConnectionRegistry;
import io.webrtc.signaling.gateway.GatewayServer;
import io.webrtc.signaling.gateway.NativeControlReceiptTracker;
import io.webrtc.signaling.gateway.NativeGatewayServices;
import io.webrtc.signaling.rpc.GatewayRelayRpcServer;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Profile;

/**
 * Listener beans share the original native owner and are retired by gateway lifecycle.
 */
@AutoConfiguration(after = NativeGatewayBusinessConfiguration.class)
@Profile("gateway")
@ConditionalOnBean({
        NativeGatewayBusinessEnrollment.class,
        NativeGatewayIngressEnrollment.class,
        NativeGatewayServices.class,
        ClockSafetyMonitor.class
})
public class NativeGatewayIngressConfiguration {
    @Bean(destroyMethod = "")
    NativeGatewayIngress nativeGatewayIngress(
            NativeGatewayBusinessEnrollment business,
            NativeGatewayIngressEnrollment inputs,
            NativeGatewayServices services,
            ClockSafetyMonitor clock,
            NativeControlReceiptTracker receipts)
            throws Exception {
        return new NativeGatewayIngress(business, inputs, services, clock, receipts);
    }

    @Bean(destroyMethod = "")
    GatewayServer nativeGatewayWss(NativeGatewayIngress ingress) {
        return ingress.wss();
    }

    @Bean(destroyMethod = "")
    GatewayRelayRpcServer nativeGatewayRelayServer(NativeGatewayIngress ingress) {
        return ingress.relay();
    }

    @Bean
    ConnectionRegistry nativeGatewayConnections(NativeGatewayIngress ingress) {
        return ingress.connections();
    }
}
