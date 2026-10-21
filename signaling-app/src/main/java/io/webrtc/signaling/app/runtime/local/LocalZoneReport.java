package io.webrtc.signaling.app.runtime.local;

import io.webrtc.signaling.app.runtime.NativeWorkerScheduler;
import io.webrtc.signaling.rpc.RpcOperation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.time.Duration;
import java.util.Collection;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

/**
 * States why a LOCAL_TEST_ONLY actor stays not-ready when its cell lacks a zone: compatibility
 * admission needs Up members in at least two enrolled zones, so a cell whose actors all carry one
 * zone forms but never becomes ready. Each enrolled zone without an Up member is logged by name
 * (the {@code missingZone} attribute, which the privacy-safe layout admits) whenever the set
 * changes. It only reads local membership; it grants and withholds nothing.
 */
final class LocalZoneReport {
    static final String JOB = "local_zone_report";
    static final Duration PERIOD = Duration.ofSeconds(5);
    private static final Logger LOG = LoggerFactory.getLogger(LocalZoneReport.class);
    private final Set<String> enrolled;
    private SortedSet<String> reported;

    LocalZoneReport(Set<String> enrolled) {
        // One zone can never open the two-zone compatibility window (Ruling 27).
        if (enrolled.size() < 2) throw new IllegalArgumentException("At least two enrolled zones");
        this.enrolled = Set.copyOf(enrolled);
    }

    /** Enrolled zones that no Up member carries; {@code INVALID} peers count for no zone. */
    SortedSet<String> missing(Collection<String> upZones) {
        var missing = new TreeSet<>(enrolled);
        missing.removeAll(upZones);
        return missing;
    }

    synchronized void observe(Collection<String> upZones) {
        var missing = missing(upZones);
        if (missing.equals(reported)) return;
        reported = missing;
        if (missing.isEmpty()) {
            // Only a positive fact: at least two distinct enrolled zones have Up members.
            if (enrolled.stream().filter(upZones::contains).count() >= 2)
                LOG.info("Local actor zones complete");
            return;
        }
        for (var zone : missing)
            try (var ignored = MDC.putCloseable("missingZone", zone)) {
                LOG.warn("Local actor not ready: an enrolled zone has no Up member");
            }
    }

    /** A MAINTENANCE job over the zones of the current Up members; a failed read reports nothing. */
    NativeWorkerScheduler.Job job(Supplier<Collection<String>> upZones) {
        return new NativeWorkerScheduler.Job(
                JOB,
                NativeWorkerScheduler.Priority.MAINTENANCE,
                PERIOD,
                budget -> {
                    try {
                        observe(upZones.get());
                    } catch (RuntimeException unavailable) {
                        // Membership not readable yet (e.g. before local Up): nothing to state.
                        // The class name only; an exception message may carry addresses.
                        LOG.debug("Local zone report skipped: {}", unavailable.getClass().getSimpleName());
                    }
                    return new RpcOperation<>(
                            CompletableFuture.completedFuture(null), CompletableFuture.completedFuture(null));
                });
    }
}
