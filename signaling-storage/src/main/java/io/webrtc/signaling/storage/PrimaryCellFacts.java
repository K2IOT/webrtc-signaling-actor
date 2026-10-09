package io.webrtc.signaling.storage;

import java.time.Duration;
import java.time.Instant;
import java.util.Objects;

/**
 * Read-only primary/cell health facts from the actual safety pool. Never substitutes for mutation
 * barriers.
 */
public final class PrimaryCellFacts {
  public record Facts(
      String cell,
      long storageEpoch,
      String status,
      boolean primary,
      boolean compatible,
      Instant checkedAt,
      long sampledAtNanos) {
    public boolean usable(long nowNanos) {
      long age = nowNanos - sampledAtNanos;
      return primary
          && compatible
          && "ACTIVE".equals(status)
          && age >= 0
          && age < Duration.ofSeconds(1).toNanos();
    }
  }

  private final SqlTransactions sql;
  private final String cell;
  private final long storageEpoch;

  public PrimaryCellFacts(SqlTransactions sql, String cell, long storageEpoch) {
    this.sql = Objects.requireNonNull(sql);
    if (cell == null || !cell.matches("[a-z][a-z0-9-]{0,23}") || storageEpoch < 1)
      throw new IllegalArgumentException("Native process cell identity required");
    this.cell = cell;
    this.storageEpoch = storageEpoch;
  }

  public DbOperation<Facts> poll(Duration budget) {
    if (budget == null
        || budget.isNegative()
        || budget.isZero()
        || budget.compareTo(Duration.ofSeconds(2)) > 0)
      throw new IllegalArgumentException("Primary probe budget outside bound");
    long started = System.nanoTime();
    return sql.submitTracked(
        DbClass.RECOVERY,
        budget,
        c -> {
          try (var query = c.createStatement();
              var row =
                  query.executeQuery(
                      "SELECT cell_id,storage_epoch,status,ownership_mode,ownership_schema_version,NOT pg_is_in_recovery(),clock_timestamp() FROM cell_authority WHERE singleton_id=1")) {
            if (!row.next()) throw new AuthoritySql.FencedException();
            boolean compatible =
                cell.equals(row.getString(1))
                    && storageEpoch == row.getLong(2)
                    && "GROUPED".equals(row.getString(4))
                    && row.getLong(5) == 1;
            return new Facts(
                row.getString(1),
                row.getLong(2),
                row.getString(3),
                row.getBoolean(6),
                compatible,
                row.getTimestamp(7).toInstant(),
                started);
          }
        });
  }
}
