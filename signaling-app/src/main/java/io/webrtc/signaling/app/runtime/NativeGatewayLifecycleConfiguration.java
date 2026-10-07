package io.webrtc.signaling.app.runtime;

import io.webrtc.signaling.auth.ClockSafetyMonitor;
import io.webrtc.signaling.gateway.*;
import io.webrtc.signaling.rpc.CellRpcClient;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.*;
import org.springframework.context.annotation.*;
import org.springframework.context.support.DefaultLifecycleProcessor;

@AutoConfiguration(after=NativeGatewayIngressConfiguration.class)
@Profile("gateway")
@ConditionalOnBean({NativeGatewayLifecycleEnrollment.class,NativeGatewayIngress.class,GatewayBootController.class,
    NativeRelaySessionProofCache.class,CellRpcClient.class,ClockSafetyMonitor.class})
public class NativeGatewayLifecycleConfiguration {
    @Bean(name="lifecycleProcessor") @ConditionalOnMissingBean(name="lifecycleProcessor")
    static DefaultLifecycleProcessor nativeGatewayLifecycleProcessor(){var lifecycle=new DefaultLifecycleProcessor();lifecycle.setTimeoutPerShutdownPhase(310000);return lifecycle;}
    @Bean(destroyMethod="") NativeGatewaySpringLifecycle nativeGatewaySpringLifecycle(NativeGatewayIngress ingress,
            GatewayBootController boot,NativeRelaySessionProofCache cache,CellRpcClient client,ClockSafetyMonitor clock,
            NativeGatewayLifecycleEnrollment inputs)throws Exception {
        return new NativeGatewaySpringLifecycle(ingress,boot,cache,client,clock,inputs);
    }
    @Bean(destroyMethod="") PrivateHealthServer nativeGatewayHealth(NativeGatewaySpringLifecycle lifecycle){return lifecycle.health();}
}
