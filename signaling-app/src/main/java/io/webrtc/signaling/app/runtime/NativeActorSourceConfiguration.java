package io.webrtc.signaling.app.runtime;

import io.webrtc.signaling.app.config.SignalingProperties;
import io.webrtc.signaling.auth.ClockSafetyMonitor;
import io.webrtc.signaling.storage.PrimaryCellFacts;
import io.webrtc.signaling.storage.SqlTransactions;
import io.webrtc.signaling.storage.worker.RevocationReconciler;
import io.webrtc.signaling.storage.worker.RevocationSourceVerifier;
import java.time.Clock;
import java.time.Duration;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Profile;

/** Native owners are created only after explicit source enrollment and admitted SQL exist. */
@AutoConfiguration(before = NativeActorPolicyConfiguration.class)
@Profile("actor")
@ConditionalOnBean({NativeActorSourceEnrollment.class, SqlTransactions.class})
public class NativeActorSourceConfiguration {
  @Bean
  ClockSafetyMonitor nativeClockMonitor(
      NativeActorSourceEnrollment enrollment, SignalingProperties properties) {
    var identity = enrollment.identity();
    if (!identity.issuer().equals(properties.identity().issuer())
        || !identity.audience().equals(properties.identity().audience())
        || !identity.clockSkew().equals(properties.identity().clockSkew()))
      throw new IllegalArgumentException(
          "Native source identity differs from effective configuration");
    return new ClockSafetyMonitor(
        enrollment.cell(),
        enrollment.storageEpoch(),
        enrollment.podUid(),
        enrollment.processBoot(),
        enrollment.clock().keys(),
        Clock.systemUTC(),
        System::nanoTime);
  }

  @Bean
  RevocationSourceVerifier nativeRevocationVerifier(NativeActorSourceEnrollment enrollment) {
    return new RevocationSourceVerifier(
        enrollment.cell(), enrollment.identity().issuer(), enrollment.revocations().keys());
  }

  @Bean
  RevocationReconciler nativeRevocationReconciler(
      NativeActorSourceEnrollment enrollment,
      SqlTransactions sql,
      RevocationSourceVerifier verifier) {
    Duration freshness = enrollment.identity().hardSafetyBound();
    if (freshness.compareTo(Duration.ofSeconds(5)) > 0) freshness = Duration.ofSeconds(5);
    return new RevocationReconciler(
        sql, enrollment.cell(), enrollment.storageEpoch(), freshness, verifier);
  }

  // Framework bean destruction must not stop safety polling before native lease handoff/release.
  // The runtime's source-drain barrier owns physical retirement of these resources.
  @Bean(destroyMethod = "")
  NativeClockSource nativeClockSource(
      NativeActorSourceEnrollment enrollment, ClockSafetyMonitor monitor) {
    var endpoint = enrollment.clock();
    return new NativeClockSource(
        endpoint.uri(), endpoint.tls(), monitor, enrollment.podUid(), enrollment.processBoot());
  }

  @Bean(destroyMethod = "")
  NativeRevocationSource nativeRevocationSource(
      NativeActorSourceEnrollment enrollment,
      RevocationSourceVerifier verifier,
      RevocationReconciler reconciler) {
    var endpoint = enrollment.revocations();
    return new NativeRevocationSource(
        endpoint.uri(),
        endpoint.tls(),
        verifier,
        reconciler,
        enrollment.podUid(),
        enrollment.processBoot());
  }

  @Bean(destroyMethod = "")
  NativeCellHealthSource nativeCellHealthSource(
      NativeActorSourceEnrollment enrollment, SqlTransactions sql) {
    return new NativeCellHealthSource(
        new PrimaryCellFacts(sql, enrollment.cell(), enrollment.storageEpoch()));
  }
}
