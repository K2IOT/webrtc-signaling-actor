package io.webrtc.signaling.app.runtime;

import io.webrtc.signaling.storage.DbAdmission;
import io.webrtc.signaling.storage.DbBoundary;
import io.webrtc.signaling.storage.DbPools;
import io.webrtc.signaling.storage.SqlTransactions;

/**
 * Published before SQL/directory aliases; Spring never independently destroys its pool leaves.
 */
public final class NativeControlDatabaseResources {
    private final SqlTransactions regional, local;

    NativeControlDatabaseResources(NativeControlDatabaseEnrollment inputs) {
        regional = open(inputs.regional());
        try {
            local = open(inputs.local());
        } catch (RuntimeException failed) {
            // No SQL owner has escaped construction, so this original boundary has no admitted work.
            try {
                regional.boundary().drain().toCompletableFuture().join();
                regional.pools().close();
            } catch (RuntimeException cleanup) {
                failed.addSuppressed(cleanup);
            }
            throw failed;
        }
    }

    /**
     * One owned boundary and pool set for a database enrollment. The caller owns draining the
     * boundary and closing the pools.
     */
    public static SqlTransactions open(NativeControlDatabaseEnrollment.Database inputs) {
        var admission = new DbAdmission(inputs.quotas(), inputs.resourceProfiles());
        var boundary = new DbBoundary(admission);
        try {
            return new SqlTransactions(
                    boundary,
                    new DbPools(
                            inputs.jdbcUrl(),
                            inputs.username(),
                            inputs.password(),
                            admission,
                            inputs.safetyMax(),
                            inputs.controlMax()));
        } catch (RuntimeException failed) {
            boundary.drain();
            throw failed;
        }
    }

    public SqlTransactions regional() {
        return regional;
    }

    public SqlTransactions local() {
        return local;
    }
}
