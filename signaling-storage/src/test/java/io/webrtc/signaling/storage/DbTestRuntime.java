package io.webrtc.signaling.storage;

import java.util.Map;

/** Real PostgreSQL fixtures, never packaged in the production artifact. */
public final class DbTestRuntime implements AutoCloseable {
  public final DbBoundary boundary;
  public final DbPools pools;
  public final SqlTransactions sql;

  public DbTestRuntime() throws Exception {
    this(PgFixture.PG.getJdbcUrl(), PgFixture.PG.getUsername(), PgFixture.PG.getPassword());
  }

  public DbTestRuntime(String url, String username, String password) throws Exception {
    try (var c = java.sql.DriverManager.getConnection(url, username, password);
        var s = c.createStatement()) {
      s.execute(
          "INSERT INTO cell_authority VALUES(1,'c001',1,'GROUPED',1,'ACTIVE') ON CONFLICT DO NOTHING");
      s.execute(
          "INSERT INTO bucket_authority(bucket_id,directory_epoch,status,recovery_epoch) SELECT n,1,'ACTIVE',1 FROM generate_series(0,16383) n ON CONFLICT DO NOTHING");
    }
    var quotas =
        new DbAdmission(
            Map.of(
                DbClass.CRITICAL,
                8,
                DbClass.RENEWAL,
                2,
                DbClass.TERMINATION,
                2,
                DbClass.RECOVERY,
                2,
                DbClass.OUTBOX,
                2,
                DbClass.MAINTENANCE,
                1));
    boundary = new DbBoundary(quotas);
    pools = new DbPools(url, username, password, quotas, 6, 11);
    sql = new SqlTransactions(boundary, pools);
  }

  public void close() {
    try {
      boundary.drain().toCompletableFuture().get(5, java.util.concurrent.TimeUnit.SECONDS);
    } catch (Exception unknown) {
      throw new IllegalStateException(
          "Native fixture cleanup unproven; pools remain open", unknown);
    }
    pools.close();
  }
}
