package io.webrtc.signaling.app.runtime;

import org.apache.pekko.actor.typed.ActorSystem;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Set;
import java.util.Objects;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.Supplier;

/** Explicit native process topology, protected maintenance jobs and private probe inputs. */
public record NativeActorSchedulingEnrollment(ActorSystem<?> system,Set<String> azRoles,String fingerprint,
        List<NativeWorkerScheduler.Job> maintenance,Consumer<NativeWorkerScheduler.Event> events,
        InetSocketAddress healthAddress,BooleanSupplier live,Supplier<String> metrics) {
    public NativeActorSchedulingEnrollment {
        Objects.requireNonNull(system);azRoles=Set.copyOf(azRoles);
        if(maintenance==null||maintenance.size()>12||maintenance.stream().anyMatch(job->job==null||job.priority()!=NativeWorkerScheduler.Priority.MAINTENANCE))
            throw new IllegalArgumentException("Maintenance requires at most12 protected maintenance jobs");
        maintenance=List.copyOf(maintenance);
        Objects.requireNonNull(events);Objects.requireNonNull(healthAddress);Objects.requireNonNull(live);Objects.requireNonNull(metrics);
    }
}
