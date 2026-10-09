package io.webrtc.signaling.app.runtime;

import io.webrtc.signaling.auth.ClockSafetyMonitor;
import io.webrtc.signaling.gateway.*;
import io.webrtc.signaling.rpc.GatewayRelayRpcServer;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Profile;

/** Listener beans share the original native owner and are retired by gateway lifecycle. */
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
      ClockSafetyMonitor clock)
      throws Exception {
    return new NativeGatewayIngress(business, inputs, services, clock);
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
