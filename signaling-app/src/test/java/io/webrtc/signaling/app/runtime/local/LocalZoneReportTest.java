package io.webrtc.signaling.app.runtime.local;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import io.webrtc.signaling.app.runtime.NativeWorkerScheduler;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

class LocalZoneReportTest {
    /**
     * Logback copies the MDC lazily; capture it at append time, as the production async appender
     * does, so the assertion never depends on which other appenders ran first.
     */
    private final ListAppender<ILoggingEvent> events =
            new ListAppender<>() {
                @Override
                protected void append(ILoggingEvent event) {
                    event.prepareForDeferredProcessing();
                    super.append(event);
                }
            };
    private final Logger logger = (Logger) LoggerFactory.getLogger(LocalZoneReport.class);

    @BeforeEach
    void attach() {
        events.start();
        logger.addAppender(events);
    }

    @AfterEach
    void detach() {
        logger.detachAppender(events);
    }

    @Test
    void missingZonesAreTheEnrolledZonesWithoutAnUpMember() {
        var report = new LocalZoneReport(Set.of("az-a", "az-b"));
        assertThat(report.missing(List.of("az-a", "az-a", "INVALID"))).containsExactly("az-b");
        assertThat(report.missing(List.of())).containsExactly("az-a", "az-b");
        assertThat(report.missing(List.of("az-b", "az-a"))).isEmpty();
    }

    @Test
    void eachMissingZoneIsLoggedByNameOnlyWhenTheSetChanges() {
        var report = new LocalZoneReport(Set.of("az-a", "az-b"));
        report.observe(List.of("az-a", "az-a", "az-a"));
        report.observe(List.of("az-a", "az-a"));
        assertThat(events.list).hasSize(1);
        var warning = events.list.getFirst();
        assertThat(warning.getLevel().toString()).isEqualTo("WARN");
        assertThat(warning.getMDCPropertyMap()).containsEntry("missingZone", "az-b");

        report.observe(List.of("az-a", "az-b"));
        assertThat(events.list).hasSize(2);
        assertThat(events.list.get(1).getLevel().toString()).isEqualTo("INFO");
        assertThat(events.list.get(1).getMDCPropertyMap()).doesNotContainKey("missingZone");
        assertThat(org.slf4j.MDC.get("missingZone")).isNull();
    }

    @Test
    void completeIsStatedOnlyWhenTwoDistinctEnrolledZonesAreUp() {
        var report = new LocalZoneReport(Set.of("az-a", "az-b"));
        report.observe(List.of("az-a", "az-b", "INVALID"));
        assertThat(events.list).hasSize(1);
        assertThat(events.list.getFirst().getLevel().toString()).isEqualTo("INFO");
        // A single enrolled zone can never satisfy the two-zone compatibility window.
        org.assertj.core.api.Assertions.assertThatThrownBy(() -> new LocalZoneReport(Set.of("az-a")))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void theReportIsAMaintenanceJobThatNeverFailsTheScheduler() throws Exception {
        var report = new LocalZoneReport(Set.of("az-a", "az-b"));
        NativeWorkerScheduler.Job job =
                report.job(
                        () -> {
                            throw new IllegalStateException("membership unavailable");
                        });
        assertThat(job.priority()).isEqualTo(NativeWorkerScheduler.Priority.MAINTENANCE);
        assertThat(job.name()).isEqualTo("local_zone_report");
        var operation = job.run().apply(Duration.ofSeconds(1));
        assertThat(operation.logical().toCompletableFuture().get()).isNull();
        assertThat(operation.physicalCompletion().toCompletableFuture()).isCompleted();
        assertThat(events.list).isEmpty();
    }
}
