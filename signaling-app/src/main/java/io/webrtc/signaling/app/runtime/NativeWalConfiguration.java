package io.webrtc.signaling.app.runtime;

import io.micrometer.core.instrument.Gauge;
import io.webrtc.signaling.storage.PostgresWalFacts;
import io.webrtc.signaling.storage.SqlTransactions;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Profile;

/** Optional explicit enrollment. Missing enrollment keeps WAL admission closed. */
@AutoConfiguration(after = NativeActorSourceConfiguration.class, before = NativeActorSchedulingConfiguration.class)
@Profile("actor")
public class NativeWalConfiguration {
  @Bean(destroyMethod = "")
  @ConditionalOnBean(NativeWalEnrollment.class)
  NativeWalSource nativeWalSource(NativeWalEnrollment policy, NativeActorSourceEnrollment identity,
      SqlTransactions sql, NativeTelemetry telemetry, org.springframework.core.env.Environment environment) {
    if (!identity.cell().equals(policy.cell()) || identity.storageEpoch() != policy.storageEpoch()
        || (policy.localSingleWriter() && !environment.matchesProfiles("local-test-only")))
      throw new IllegalArgumentException("WAL enrollment differs from native identity/profile");
    var source = new NativeWalSource(new PostgresWalFacts(sql, policy.cell(), policy.storageEpoch(),
        policy.systemIdentifier(), policy.timeline()), policy);
    Gauge.builder("signaling.wal.source.age.seconds", source, s -> seconds(s.freshnessAgeNanos())).register(telemetry.metrics().registry());
    Gauge.builder("signaling.wal.flush.lag.bytes", source, s -> known(s.maximumFlushLagBytes())).register(telemetry.metrics().registry());
    Gauge.builder("signaling.wal.replication.flush.seconds", source, s -> seconds(s.maximumObservedFlushDurationNanos())).register(telemetry.metrics().registry());
    return source;
  }
  private static double known(long value) { return value < 0 ? Double.NaN : value; }
  private static double seconds(long value) { return value < 0 ? Double.NaN : value / 1e9; }
  @Bean
  static BeanPostProcessor nativeWalBinding(org.springframework.beans.factory.BeanFactory factory) {
    return new BeanPostProcessor() {
      @Override public Object postProcessAfterInitialization(Object bean, String name) {
        if (bean instanceof NativeActorSafetySources sources) {
          var source = factory.getBeanProvider(NativeWalSource.class).getIfAvailable();
          if (source != null) sources.wal(source);
        }
        return bean;
      }
    };
  }
}
