package io.webrtc.signaling.storage;

import io.webrtc.signaling.observability.*;
import io.webrtc.signaling.observability.SignalingMetrics.*;
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

  private volatile OperationObservation observer = OperationObservation.NONE;
  private final DbBoundary boundary;
  private final DbPools pools;
  private final OutboxHints outboxHints = new OutboxHints(1024);

  public OutboxHints outboxHints() {
    return outboxHints;
  }

  public SqlTransactions(DbBoundary boundary, DbPools pools) {
    this.boundary = boundary;
    this.pools = pools;
  }

  public void observe(OperationObservation observer) {
    this.observer = java.util.Objects.requireNonNull(observer);
    boundary.observe(observer);
    pools.observe(observer);
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

  /** Caller validates its finite immutable input cap; this never inspects an opaque closure. */
  public DbCost boundedCharge(DbClass clazz, long inputByteBound, long resultByteBound) {
    var maximum = boundary.maximumCharge(clazz);
    if (maximum == null
        || inputByteBound < 1
        || resultByteBound < 1
        || inputByteBound > maximum.inputBytes()
        || resultByteBound > maximum.resultBytes()) throw new DbOverloadedException();
    long retained =
        Math.addExact(
            Math.multiplyExact(inputByteBound, 2), Math.multiplyExact(resultByteBound, 2));
    if (retained > maximum.retainedBytes()) throw new DbOverloadedException();
    return new DbCost(inputByteBound, retained, resultByteBound, maximum.serviceNanos());
  }

  public <T> DbOperation<T> submitTracked(DbClass clazz, Duration budget, Work<T> work) {
    return submitTracked(clazz, budget, boundary.maximumCharge(clazz), work);
  }

  public <T> DbOperation<T> submitTracked(
      DbClass clazz, Duration budget, DbCost cost, Work<T> work) {
    long start = System.nanoTime();
    return boundary.submitTransactionTracked(
        clazz,
        budget,
        cost,
        () -> {
          var observation =
              OperationObservation.safeStart(
                  observer,
                  clazz.safety() ? Operation.TRANSACTION_SAFETY : Operation.TRANSACTION_CONTROL);
          var template = new TransactionTemplate(pools.manager(clazz));
          template.setIsolationLevel(TransactionDefinition.ISOLATION_READ_COMMITTED);
          template.setTimeout((int) Math.max(1, Math.min(2, budget.toSeconds())));
          try (var scope = observation.activate()) {
            DbBoundary.Validated<T> value =
                template.execute(
                    status -> {
                      try {
                        org.springframework.transaction.support.TransactionSynchronizationManager
                            .registerSynchronization(
                                new org.springframework.transaction.support
                                    .TransactionSynchronization() {
                                  private long committing;

                                  public void beforeCompletion() {
                                    committing = System.nanoTime();
                                  }

                                  public void afterCompletion(int completion) {
                                    if (completion == STATUS_COMMITTED && committing != 0)
                                      observation.commit(
                                          Math.max(0, System.nanoTime() - committing));
                                  }
                                });
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
                        return DbBoundary.beforeCommit(result, cost.resultBytes());
                      } catch (RuntimeException e) {
                        throw e;
                      } catch (Exception e) {
                        throw new SqlWorkException(e);
                      }
                    });
            observation.logical(Outcome.COMMITTED);
            return value;
          } catch (RuntimeException failure) {
            observation.logical(Outcome.INTERNAL_ERROR);
            throw failure;
          } finally {
            observation.physical(true);
          }
        });
  }

  public static final class SqlWorkException extends RuntimeException {
    public SqlWorkException(Exception cause) {
      super("Database operation failed", cause);
    }
  }
}
