package io.webrtc.signaling.storage;

import java.sql.*;
import java.time.Duration;
import java.util.concurrent.CompletionStage;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;

/** TransactionTemplate returns only after COMMIT on the same admitted virtual thread. */
public final class SqlTransactions {
  @FunctionalInterface
  public interface Work<T> {
    T apply(Connection connection) throws Exception;
  }

  private final DbBoundary boundary;
  private final DbPools pools;

  public SqlTransactions(DbBoundary boundary, DbPools pools) {
    this.boundary = boundary;
    this.pools = pools;
  }

  /** Original process owners; lifecycle code must join the boundary before retiring the pools. */
  public DbBoundary boundary() {
    return boundary;
  }

  public DbPools pools() {
    return pools;
  }

  public <T> CompletionStage<T> submit(DbClass clazz, Duration budget, Work<T> work) {
    return submitTracked(clazz, budget, work).logical();
  }

  public <T> DbOperation<T> submitTracked(DbClass clazz, Duration budget, Work<T> work) {
    long start = System.nanoTime();
    return boundary.submitTracked(
        clazz,
        budget,
        () -> {
          var template = new TransactionTemplate(pools.manager(clazz));
          template.setIsolationLevel(TransactionDefinition.ISOLATION_READ_COMMITTED);
          template.setTimeout((int) Math.max(1, Math.min(2, budget.toSeconds())));
          return template.execute(
              status -> {
                try {
                  var c = pools.currentConnection(clazz);
                  long remaining = budget.toNanos() - (System.nanoTime() - start);
                  if (remaining <= 0) throw new DbOverloadedException();
                  long ms = Math.max(1, Math.min(1000, remaining / 1_000_000));
                  try (var s = c.createStatement()) {
                    s.execute("SET LOCAL lock_timeout='" + Math.min(100, ms) + "ms'");
                    s.execute("SET LOCAL statement_timeout='" + ms + "ms'");
                    s.execute("SET LOCAL transaction_timeout='" + Math.min(2000, ms) + "ms'");
                    s.execute("SET LOCAL idle_in_transaction_session_timeout='" + ms + "ms'");
                    s.execute("SET LOCAL synchronous_commit=on");
                  }
                  T result = work.apply(c);
                  DbBoundary.validate(result, 0);
                  return result;
                } catch (RuntimeException e) {
                  throw e;
                } catch (Exception e) {
                  throw new SqlWorkException(e);
                }
              });
        });
  }

  public static final class SqlWorkException extends RuntimeException {
    public SqlWorkException(Exception cause) {
      super("Database operation failed", cause);
    }
  }
}
