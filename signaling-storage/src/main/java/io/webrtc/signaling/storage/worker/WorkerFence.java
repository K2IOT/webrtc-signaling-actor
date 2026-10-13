package io.webrtc.signaling.storage.worker;

import io.webrtc.signaling.storage.AuthoritySql;

import java.sql.Connection;
import java.sql.SQLException;

final class WorkerFence {
    static void cell(Connection c, String cell, long epoch) throws SQLException {
        AuthoritySql.cellBarrier(c, false);
        AuthoritySql.validateCell(c, cell, epoch);
    }

    static void bucket(Connection c, int bucket) throws SQLException {
        AuthoritySql.bucketBarrier(c, bucket, false);
        try (var q = c.prepareStatement("SELECT status FROM bucket_authority WHERE bucket_id=?")) {
            q.setInt(1, bucket);
            try (var r = q.executeQuery()) {
                if (!r.next() || !"ACTIVE".equals(r.getString(1))) throw new AuthoritySql.FencedException();
            }
        }
    }

    /**
     * Security ingestion may advance epochs on an exactly fenced source without touching business
     * rows.
     */
    static boolean securityBucket(Connection c, int bucket) throws SQLException {
        AuthoritySql.bucketBarrier(c, bucket, false);
        try (var q =
                     c.prepareStatement(
                             "SELECT status,native_transfer_source_fenced(bucket_id) FROM bucket_authority WHERE"
                                     + " bucket_id=?")) {
            q.setInt(1, bucket);
            try (var r = q.executeQuery()) {
                if (!r.next()) throw new AuthoritySql.FencedException();
                if ("ACTIVE".equals(r.getString(1))) return true;
                if ("FROZEN".equals(r.getString(1)) && r.getBoolean(2)) return false;
                throw new AuthoritySql.FencedException();
            }
        }
    }

    /**
     * This does not authorize business writes. SQL triggers separately enforce expiry-only deletes.
     */
    static void cleanupBucket(Connection c, int bucket) throws SQLException {
        AuthoritySql.bucketBarrier(c, bucket, false);
        try (var q =
                     c.prepareStatement(
                             "SELECT status,native_transfer_published(bucket_id) FROM bucket_authority WHERE"
                                     + " bucket_id=?")) {
            q.setInt(1, bucket);
            try (var r = q.executeQuery()) {
                if (!r.next() || !("ACTIVE".equals(r.getString(1)) || r.getBoolean(2)))
                    throw new AuthoritySql.FencedException();
            }
        }
    }

    static int retireTransferCopies(Connection c, int bucket, int limit) throws SQLException {
        limit(limit, 128);
        try (var q =
                     c.prepareStatement(
                             "WITH due AS (SELECT i.transfer_id,i.dataset,i.row_key FROM bucket_transfer_item i JOIN"
                                     + " bucket_transfer t ON t.transfer_id=i.transfer_id JOIN bucket_authority a ON"
                                     + " a.bucket_id=t.bucket_id AND a.transfer_id=t.transfer_id WHERE t.bucket_id=? AND"
                                     + " t.state='PUBLISHED' AND native_transfer_published(t.bucket_id) ORDER BY"
                                     + " i.transfer_id,i.dataset,i.row_key LIMIT ? FOR UPDATE OF i SKIP LOCKED) DELETE"
                                     + " FROM bucket_transfer_item i USING due d WHERE"
                                     + " (i.transfer_id,i.dataset,i.row_key)=(d.transfer_id,d.dataset,d.row_key)")) {
            q.setInt(1, bucket);
            q.setInt(2, limit);
            return q.executeUpdate();
        }
    }

    static boolean copiesRetired(Connection c, int bucket) throws SQLException {
        try (var q =
                     c.prepareStatement(
                             "SELECT NOT EXISTS(SELECT 1 FROM bucket_transfer_item i JOIN bucket_transfer t ON"
                                     + " t.transfer_id=i.transfer_id WHERE t.bucket_id=?)")) {
            q.setInt(1, bucket);
            try (var r = q.executeQuery()) {
                return r.next() && r.getBoolean(1);
            }
        }
    }

    static void limit(int limit, int maximum) {
        if (limit < 1 || limit > maximum)
            throw new IllegalArgumentException("Invalid worker batch bound");
    }
}
