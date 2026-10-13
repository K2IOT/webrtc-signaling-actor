package io.webrtc.signaling.app.runtime;

import io.webrtc.signaling.auth.BoundedTokenVerifier;
import io.webrtc.signaling.auth.ClockSafetyMonitor;
import io.webrtc.signaling.gateway.GatewayBootController;
import io.webrtc.signaling.gateway.NativeRelaySessionProofCache;
import io.webrtc.signaling.rpc.CellRpcClient;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Profile;
import org.springframework.context.support.DefaultLifecycleProcessor;

@AutoConfiguration(after = NativeGatewayIngressConfiguration.class)
@Profile("gateway")
@ConditionalOnBean({
        NativeGatewayLifecycleEnrollment.class,
        NativeGatewayIngress.class,
        GatewayBootController.class,
        NativeRelaySessionProofCache.class,
        CellRpcClient.class,
        ClockSafetyMonitor.class,
        BoundedTokenVerifier.class,
        NativeGatewaySafety.class
})
public class NativeGatewayLifecycleConfiguration {
    @Bean(name = "lifecycleProcessor")
    @ConditionalOnMissingBean(name = "lifecycleProcessor")
    static DefaultLifecycleProcessor nativeGatewayLifecycleProcessor() {
        var lifecycle = new DefaultLifecycleProcessor();
        lifecycle.setTimeoutPerShutdownPhase(310000);
        return lifecycle;
    }

    @Bean(destroyMethod = "")
    NativeGatewaySpringLifecycle nativeGatewaySpringLifecycle(
            NativeGatewayIngress ingress,
            GatewayBootController boot,
            NativeRelaySessionProofCache cache,
            CellRpcClient client,
            NativeGatewayLifecycleEnrollment inputs,
            BoundedTokenVerifier tokens,
            NativeGatewaySafety safety,
            ObjectProvider<NativeWorkerScheduler> workers,
            ObjectProvider<NativeClockSource> clocks,
            ObjectProvider<NativeCachedRevocationSource> sources,
            NativeTelemetry telemetry,
            NativeCompatibility compatibility)
            throws Exception {
        var lifecycle =
                new NativeGatewaySpringLifecycle(
                        ingress,
                        boot,
                        cache,
                        client,
                        inputs,
                        tokens,
                        safety,
                        workers.orderedStream().toList(),
                        clocks.orderedStream().toList(),
                        sources.orderedStream().toList(),
                        telemetry);
        lifecycle.compatibility(
                () -> compatibility.gatewayReady(boot.identity().gatewayId(), boot.identity().bootId()));
        return lifecycle;
    }

    @Bean(destroyMethod = "")
    PrivateHealthServer nativeGatewayHealth(NativeGatewaySpringLifecycle lifecycle) {
        return lifecycle.health();
    }
}
