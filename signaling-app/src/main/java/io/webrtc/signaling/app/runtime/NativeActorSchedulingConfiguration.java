package io.webrtc.signaling.app.runtime;

import io.webrtc.signaling.actors.cluster.ClusterReadiness;
import io.webrtc.signaling.auth.ClockSafetyMonitor;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Profile;

/** Fixed safety work and private cached probes; native lifecycle owns physical teardown. */
@AutoConfiguration(
    after = {NativeActorSourceConfiguration.class, NativeActorCompositionConfiguration.class},
    before = NativeActorLifecycleConfiguration.class)
@Profile("actor")
@ConditionalOnBean({
  NativeActorSchedulingEnrollment.class,
  ClusterReadiness.class,
  ClockSafetyMonitor.class,
  NativeClockSource.class,
  NativeCellHealthSource.class,
  NativeRevocationSource.class
})
public class NativeActorSchedulingConfiguration {
  @Bean(destroyMethod = "")
  NativeActorSafetySources nativeActorSafetySources(
      NativeActorSchedulingEnrollment enrollment,
      ClusterReadiness readiness,
      ClockSafetyMonitor clock,
      NativeClockSource clockSource,
      NativeCellHealthSource primary,
      NativeRevocationSource revocations) {
    return new NativeActorSafetySources(
        enrollment.system(),
        readiness,
        enrollment.azRoles(),
        enrollment.fingerprint(),
        clock,
        clockSource,
        primary,
        revocations);
  }

  @Bean(destroyMethod = "")
  NativeWorkerScheduler nativeActorWorkers(
      NativeActorSchedulingEnrollment enrollment, NativeActorSafetySources sources) {
    var jobs = new ArrayList<>(sources.jobs());
    jobs.addAll(enrollment.maintenance());
    // Lifecycle registers cleanup before starting this scheduler.
    return new NativeWorkerScheduler(jobs, enrollment.events());
  }

  @Bean(destroyMethod = "")
  PrivateHealthServer nativeActorHealth(
      NativeActorSchedulingEnrollment enrollment,
      ClusterReadiness readiness,
      NativeActorSafetySources sources)
      throws Exception {
    var health =
        new PrivateHealthServer(
            enrollment.healthAddress(),
            enrollment.live(),
            readiness::businessReady,
            enrollment.metrics());
    try {
      health.start().toCompletableFuture().get(2, TimeUnit.SECONDS);
      return health;
    } catch (Exception failed) {
      health.stop();
      throw failed;
    }
  }
}
