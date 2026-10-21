package io.webrtc.signaling.it;

import io.webrtc.signaling.app.runtime.NativeControlDatabaseEnrollment;
import io.webrtc.signaling.app.runtime.local.LocalDatabases;
import io.webrtc.signaling.storage.DbClass;
import io.webrtc.signaling.storage.PgFixture;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

class LocalDatabasesIT {
    static NativeControlDatabaseEnrollment.Database database() {
        return new NativeControlDatabaseEnrollment.Database(
                PgFixture.PG.getJdbcUrl(),
                PgFixture.PG.getUsername(),
                PgFixture.PG.getPassword(),
                Map.of(DbClass.RECOVERY, 2, DbClass.CRITICAL, 2),
                2,
                2);
    }

    @Test
    void opensPoolsAndRunsBoundedTransaction() throws Exception {
        var sql = LocalDatabases.open(database());
        try {
            var settings =
                    sql.submit(
                                    DbClass.CRITICAL,
                                    Duration.ofSeconds(2),
                                    connection -> {
                                        try (var statement = connection.createStatement();
                                             var rows =
                                                     statement.executeQuery(
                                                             "SELECT current_setting('transaction_timeout'),"
                                                                     + " current_setting('synchronous_commit'),"
                                                                     + " current_setting('server_version_num')::int")) {
                                            rows.next();
                                            return rows.getString(1) + "|" + rows.getString(2) + "|" + rows.getString(3);
                                        }
                                    })
                            .toCompletableFuture()
                            .get(10, TimeUnit.SECONDS);
            var values = settings.split("\\|");
            // SET LOCAL transaction_timeout needs PostgreSQL 17; the transaction ran under a deadline.
            assertThat(Integer.parseInt(values[2])).isGreaterThanOrEqualTo(170000);
            assertThat(values[0]).isNotEqualTo("0").matches("[0-9]+(ms|s)");
            assertThat(values[1]).isEqualTo("on");
            assertThat(sql.pools().closed()).isFalse();
        } finally {
            sql.boundary().drain().toCompletableFuture().get(5, TimeUnit.SECONDS);
            sql.pools().close();
        }
        assertThat(sql.pools().closed()).isTrue();
    }
}
