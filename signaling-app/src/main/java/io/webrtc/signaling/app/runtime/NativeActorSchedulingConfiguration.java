package io.webrtc.signaling.app.runtime;

import io.webrtc.signaling.actors.cluster.ClusterReadiness;
import io.webrtc.signaling.auth.ClockSafetyMonitor;
import io.webrtc.signaling.rpc.CellRpcClient;
import io.webrtc.signaling.rpc.NativeActorComposition;
import java.util.ArrayList;
import java.util.List;
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
      NativeRevocationSource revocations,
      NativeCompatibility compatibility) {
    return new NativeActorSafetySources(
            enrollment.system(),
            readiness,
            enrollment.azRoles(),
            enrollment.fingerprint(),
            clock,
            clockSource,
            primary,
            revocations)
        .compatibility(compatibility::actorReady);
  }

  @Bean(destroyMethod = "")
  NativeActorMaintenance nativeActorMaintenance(
      NativeActorComposition actors,
      NativeActorMaintenanceEnrollment enrollment,
      CellRpcClient homeRpc,
      NativeActorIngressEnrollment ingress,
      NativeTelemetry telemetry) {
    var maintenance = new NativeActorMaintenance(actors, enrollment, homeRpc, ingress.gateways());
    telemetry.lagSource(
        io.webrtc.signaling.observability.SignalingMetrics.Lag.OLDEST_UNRECONCILED,
        maintenance.recovery()::observedProgressAgeNanos);
    telemetry.lagSource(
        io.webrtc.signaling.observability.SignalingMetrics.Lag.RECOVERY_SWEEP,
        maintenance.recovery()::observedProgressAgeNanos);
    telemetry.lagSource(
        io.webrtc.signaling.observability.SignalingMetrics.Lag.ROOT_RENEWAL,
        () ->
            io.webrtc.signaling.actors.lease.PostgresShardLeaseProvider.rootRenewalAgeNanos(
                org.apache.pekko.actor.typed.javadsl.Adapter.toClassic(actors.system())));
    return maintenance;
  }

  @Bean(destroyMethod = "")
  NativeWorkerScheduler nativeActorWorkers(
      NativeActorSchedulingEnrollment enrollment,
      NativeActorSafetySources sources,
      NativeActorMaintenance maintenance,
      NativeTelemetry telemetry,
      NativeCompatibility compatibility,
      io.webrtc.signaling.storage.SqlTransactions sql) {
    var jobs = new ArrayList<>(sources.jobs());
    jobs.addAll(maintenance.jobs());
    jobs.add(compatibility.schemaJob(sql));
    int availableAdditionalJobs = 16 - jobs.size();
    if (enrollment.maintenance().size() > availableAdditionalJobs)
      throw new IllegalArgumentException(
          "Native mandatory jobs leave " + availableAdditionalJobs + " additional worker slots");
    jobs.addAll(enrollment.maintenance());
    // Lifecycle registers cleanup before starting this scheduler.
    var scheduler =
        new NativeWorkerScheduler(jobs, enrollment.events(), List.of(maintenance::drain))
            .observe(telemetry::workerStarted);
    maintenance.bindWake(scheduler);
    return scheduler;
  }

  @Bean(destroyMethod = "")
  PrivateHealthServer nativeActorHealth(
      NativeActorSchedulingEnrollment enrollment,
      ClusterReadiness readiness,
      NativeActorSafetySources sources,
      NativeTelemetry telemetry,
      NativeCompatibility compatibility)
      throws Exception {
    var health =
        new PrivateHealthServer(
            enrollment.healthAddress(),
            enrollment.live(),
            () -> compatibility.admissionOpen() && readiness.businessReady(),
            telemetry::scrape);
    try {
      health.start().toCompletableFuture().get(2, TimeUnit.SECONDS);
      return health;
    } catch (Exception failed) {
      health.stop();
      throw failed;
    }
  }
}
