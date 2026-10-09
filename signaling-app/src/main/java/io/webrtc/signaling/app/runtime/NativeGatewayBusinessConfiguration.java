package io.webrtc.signaling.app.runtime;

import io.webrtc.signaling.auth.AuthorizationStatus;
import io.webrtc.signaling.auth.BoundedTokenVerifier;
import io.webrtc.signaling.auth.ClockSafetyMonitor;
import io.webrtc.signaling.gateway.*;
import io.webrtc.signaling.rpc.CellRpcClient;
import java.time.Clock;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Profile;

/** Native gateway composition with no implicit identity, directory or trusted-clock defaults. */
@AutoConfiguration
@Profile("gateway")
@ConditionalOnBean({
  NativeGatewayBusinessEnrollment.class,
  CellRpcClient.class,
  ClockSafetyMonitor.class
})
public class NativeGatewayBusinessConfiguration {
  @Bean
  NativeGatewaySafety nativeGatewaySafety(
      NativeGatewayBusinessEnrollment enrollment, ClockSafetyMonitor clock) {
    return new NativeGatewaySafety(clock, enrollment.securityFresh());
  }

  @Bean(destroyMethod = "")
  @ConditionalOnMissingBean(BoundedTokenVerifier.class)
  BoundedTokenVerifier nativeGatewayTokens(NativeGatewayBusinessEnrollment enrollment) {
    return enrollment.tokens();
  }

  // A gateway lifecycle retires these owners after original transport receipts settle.
  @Bean(destroyMethod = "")
  GatewayBootController nativeGatewayBoot(
      NativeGatewayBusinessEnrollment enrollment,
      CellRpcClient client,
      BoundedTokenVerifier tokens) {
    if (tokens != enrollment.tokens())
      throw new IllegalArgumentException("Native gateway verifier ownership differs");
    var boot =
        new GatewayBootController(
            enrollment.identity(), GatewayBootController.network(client), System::nanoTime);
    boot.start();
    return boot;
  }

  @Bean(destroyMethod = "")
  NativeRelaySessionProofCache nativeGatewayRelayProofCache(
      NativeGatewayBusinessEnrollment enrollment,
      GatewayBootController boot,
      NativeGatewaySafety safety) {
    return new NativeRelaySessionProofCache(
        enrollment.relayCacheCapacity(),
        enrollment.relayPendingLimit(),
        Clock.systemUTC(),
        System::nanoTime,
        () -> boot.current() && safety.valid(),
        enrollment.relayProofs());
  }

  @Bean
  NativeGatewayCommands nativeGatewayCommands(
      NativeGatewayBusinessEnrollment enrollment,
      CellRpcClient client,
      NativeRelaySessionProofCache cache) {
    return new NativeGatewayCommands(
            enrollment.identity(),
            enrollment.homes(),
            NativeGatewayCommands.network(client),
            Clock.systemUTC(),
            enrollment.routingEpoch())
        .relayProofCache(cache);
  }

  @Bean
  NativeGatewayServices nativeGatewayServices(
      NativeGatewayBusinessEnrollment enrollment,
      GatewayBootController boot,
      NativeGatewaySafety safety,
      CellRpcClient client,
      NativeGatewayCommands commands) {
    return new NativeGatewayServices(
        boot,
        enrollment.tokens(),
        (principal, now) -> {
          try {
            if (!safety.valid()) return AuthorizationStatus.FRESHNESS_UNKNOWN;
            var status = enrollment.cachedSecurity().apply(principal, now);
            return safety.valid() && status != null
                ? status
                : AuthorizationStatus.FRESHNESS_UNKNOWN;
          } catch (RuntimeException unavailable) {
            return AuthorizationStatus.FRESHNESS_UNKNOWN;
          }
        },
        user -> {
          var home = enrollment.homes().apply(user);
          return new NativeGatewayServices.Home(home.cell(), home.directoryEpoch());
        },
        client,
        commands);
  }
}
