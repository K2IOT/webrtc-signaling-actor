package io.webrtc.signaling.storage;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Every transaction runs under its own bounded timeouts and synchronous commit, set LOCAL so they
 * never outlive it on the pooled connection.
 */
class SqlTransactionsSessionSettingsIT {
    private static final String[] SETTINGS = {
            "lock_timeout", "statement_timeout", "transaction_timeout",
            "idle_in_transaction_session_timeout", "synchronous_commit"};

    private static Map<String, String> settings(java.sql.Connection c) throws Exception {
        var values = new LinkedHashMap<String, String>();
        for (var name : SETTINGS)
            try (var q = c.prepareStatement("SELECT current_setting(?)")) {
                q.setString(1, name);
                try (var r = q.executeQuery()) {
                    r.next();
                    values.put(name, r.getString(1));
                }
            }
        return Map.copyOf(values);
    }

    /** The boundary admits only native DTOs: carry the settings out as one string. */
    private static String encoded(java.sql.Connection c) throws Exception {
        var out = new StringBuilder();
        settings(c).forEach((k, v) -> out.append(k).append('=').append(v).append(';'));
        return out.toString();
    }

    private static Map<String, String> decoded(String text) {
        var values = new LinkedHashMap<String, String>();
        for (var pair : text.split(";")) {
            if (pair.isEmpty()) continue;
            int at = pair.indexOf('=');
            values.put(pair.substring(0, at), pair.substring(at + 1));
        }
        return values;
    }

    @Test
    void eachTransactionRunsUnderItsBoundedLocalSettings() throws Exception {
        try (var runtime = new DbTestRuntime()) {
            var inside =
                    decoded(
                            runtime.sql
                                    .submit(DbClass.CRITICAL, Duration.ofSeconds(2), SqlTransactionsSessionSettingsIT::encoded)
                                    .toCompletableFuture()
                                    .get(5, TimeUnit.SECONDS));

            assertThat(inside)
                    .containsEntry("lock_timeout", "100ms")
                    .containsEntry("synchronous_commit", "on");
            // The remaining budget, capped at one second, bounds the statement and the idle time.
            for (var name : new String[]{"statement_timeout", "transaction_timeout", "idle_in_transaction_session_timeout"})
                assertThat(inside.get(name)).as(name).matches("(9[0-9]{2}ms|1s)");
        }
    }

    @Test
    void theSettingsDoNotOutliveTheTransaction() throws Exception {
        try (var runtime = new DbTestRuntime()) {
            runtime.sql
                    .submit(DbClass.CRITICAL, Duration.ofSeconds(2), SqlTransactionsSessionSettingsIT::encoded)
                    .toCompletableFuture()
                    .get(5, TimeUnit.SECONDS);
            // The same pooled connections, outside any SqlTransactions transaction.
            try (var c = runtime.pools.manager(DbClass.CRITICAL).getDataSource().getConnection()) {
                var outside = settings(c);
                assertThat(outside.get("lock_timeout")).isEqualTo("0");
                assertThat(outside.get("statement_timeout")).isEqualTo("0");
            }
        }
    }
}
