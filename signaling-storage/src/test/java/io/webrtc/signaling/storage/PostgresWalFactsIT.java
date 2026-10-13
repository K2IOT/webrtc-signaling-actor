package io.webrtc.signaling.storage;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Actual PG17 writer identity/LSN path, local single-writer test only.
 */
class PostgresWalFactsIT {
    @Test
    void writerFactsCrossImmutableBoundaryAndMismatchFailsClosed() throws Exception {
        try (var f = new LocalInviteAtomicIT.Fixture()) {
            String id;
            long timeline;
            try (var c = f.connection();
                 var q = c.createStatement();
                 var r =
                         q.executeQuery(
                                 "SELECT system_identifier::text,('x'||substring(pg_walfile_name(pg_current_wal_flush_lsn()),1,8))::bit(32)::bigint FROM pg_control_system()")) {
                r.next();
                id = r.getString(1);
                timeline = r.getLong(2);
            }
            var facts =
                    NegotiationGrantIT.done(
                            new PostgresWalFacts(f.runtime.sql, "c001", 1, id, timeline)
                                    .poll(Duration.ofSeconds(2)));
            assertThat(facts.systemIdentifier()).isEqualTo(id);
            assertThat(facts.timeline()).isEqualTo(timeline);
            assertThat(facts.flush().signum()).isNotNegative();
            assertThat(facts.synchronousPolicy().required()).isZero();
            assertThat(facts.synchronousPolicy().mode()).isEqualTo("NONE");
            assertThatThrownBy(
                    () ->
                            NegotiationGrantIT.done(
                                    new PostgresWalFacts(f.runtime.sql, "c001", 1, "0", timeline)
                                            .poll(Duration.ofSeconds(2))))
                    .hasRootCauseInstanceOf(IllegalStateException.class);
        }
    }
}
