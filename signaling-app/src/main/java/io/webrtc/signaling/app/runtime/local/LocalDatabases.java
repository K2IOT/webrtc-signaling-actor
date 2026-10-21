package io.webrtc.signaling.app.runtime.local;

import io.webrtc.signaling.app.runtime.NativeControlDatabaseEnrollment;
import io.webrtc.signaling.app.runtime.NativeControlDatabaseResources;
import io.webrtc.signaling.storage.SqlTransactions;

/**
 * Opens the admission boundary and connection pools for a mounted database. It delegates to the
 * same construction the native control process uses, so quotas, resource profiles and pool
 * budgets are enforced identically; nothing is relaxed for local use.
 */
public final class LocalDatabases {
    private LocalDatabases() {
    }

    /**
     * The caller owns the result: drain {@code boundary()} and then close {@code pools()}. The
     * database's JDBC URL has already been required to use {@code sslmode=verify-full}.
     */
    public static SqlTransactions open(NativeControlDatabaseEnrollment.Database database) {
        return NativeControlDatabaseResources.open(database);
    }
}
