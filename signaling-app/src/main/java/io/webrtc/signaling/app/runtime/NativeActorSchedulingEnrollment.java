package io.webrtc.signaling.app.runtime;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.apache.pekko.actor.typed.ActorSystem;

/** Native topology, bounded additional jobs and probes; concrete maintenance is mandatory. */
public record NativeActorSchedulingEnrollment(
    ActorSystem<?> system,
    Set<String> azRoles,
    String fingerprint,
    List<NativeWorkerScheduler.Job> maintenance,
    Consumer<NativeWorkerScheduler.Event> events,
    InetSocketAddress healthAddress,
    BooleanSupplier live,
    Supplier<String> metrics) {
  public NativeActorSchedulingEnrollment {
    Objects.requireNonNull(system);
    azRoles = Set.copyOf(azRoles);
    if (maintenance == null
        || maintenance.size() > 8
        || maintenance.stream()
            .anyMatch(
                job -> job == null || job.priority() != NativeWorkerScheduler.Priority.MAINTENANCE))
      throw new IllegalArgumentException(
          "At most 8 additional jobs alongside mandatory native maintenance");
    maintenance = List.copyOf(maintenance);
    Objects.requireNonNull(events);
    Objects.requireNonNull(healthAddress);
    Objects.requireNonNull(live);
    Objects.requireNonNull(metrics);
  }
}
