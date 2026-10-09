package io.webrtc.signaling.actors.call;

import io.webrtc.signaling.protocol.Identity.CallId;
import io.webrtc.signaling.storage.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;

/**
 * Indexed bounded hints. Neither the scan nor a successful wake manufactures participant authority.
 */
public final class CallRecoveryService implements AutoCloseable {
  public record Report(int examined, int hydrated, int unresolved, boolean cycleComplete) {}

  private record Hint(long hashVersion, int group, CallId call) {}

  private final SqlTransactions sql;
  private final String cell;
  private final long epoch;
  private final BiFunction<CallId, Duration, CompletionStage<Boolean>> wake;
  private final AtomicBoolean busy = new AtomicBoolean();
  private final ScheduledExecutorService worker =
      Executors.newSingleThreadScheduledExecutor(
          Thread.ofPlatform().name("call-recovery").daemon().factory());
  private volatile Hint cursor;
  private volatile boolean closed;

  public CallRecoveryService(
      SqlTransactions sql,
      String cell,
      long epoch,
      BiFunction<CallId, Duration, CompletionStage<Boolean>> wake) {
    this.sql = Objects.requireNonNull(sql);
    this.cell = Objects.requireNonNull(cell);
    this.epoch = epoch;
    this.wake = Objects.requireNonNull(wake);
  }

  public void start() {
    worker.scheduleWithFixedDelay(
        () -> {
          if (!closed) sweep();
        },
        0,
        1,
        TimeUnit.SECONDS);
  }

  public CompletionStage<Report> sweep() {
    if (closed || !busy.compareAndSet(false, true))
      return CompletableFuture.<Report>failedFuture(new DbOverloadedException())
          .minimalCompletionStage();
    long started = System.nanoTime();
    Hint after = cursor;
    DbOperation<List<Hint>> scan;
    try {
      scan =
          sql.submitTracked(
              DbClass.RECOVERY,
              Duration.ofSeconds(2),
              c -> {
                AuthoritySql.cellBarrier(c, false);
                AuthoritySql.validateCell(c, cell, epoch);
                var hints = new ArrayList<Hint>();
                String suffix =
                    after == null
                        ? ""
                        : " AND (ownership_hash_version,ownership_group_id,call_id)>(?,?,?)";
                try (var q =
                    c.prepareStatement(
                        "SELECT ownership_hash_version,ownership_group_id,call_id FROM call_state WHERE terminal_at IS NULL"
                            + suffix
                            + " ORDER BY ownership_hash_version,ownership_group_id,call_id LIMIT 512")) {
                  if (after != null) {
                    q.setLong(1, after.hashVersion());
                    q.setInt(2, after.group());
                    q.setString(3, after.call().value());
                  }
                  try (var r = q.executeQuery()) {
                    while (r.next())
                      hints.add(new Hint(r.getLong(1), r.getInt(2), new CallId(r.getString(3))));
                  }
                }
                return List.copyOf(hints);
              });
    } catch (RuntimeException error) {
      busy.set(false);
      return CompletableFuture.<Report>failedFuture(error).minimalCompletionStage();
    }
    var logical =
        scan.logical()
            .thenCombine(scan.physicalCompletion(), (hints, cleanup) -> hints)
            .thenComposeAsync(
                hints -> {
                  boolean complete = hints.size() < 512;
                  cursor = complete ? null : hints.getLast();
                  return dispatch(hints, 0, 0, started)
                      .thenApply(
                          hydrated ->
                              new Report(
                                  hints.size(), hydrated, hints.size() - hydrated, complete));
                },
                worker);
    // Failure may arrive before physical cleanup; do not release the scan credit at logical
    // timeout.
    logical.whenComplete(
        (r, e) -> scan.physicalCompletion().whenComplete((cleanup, error) -> busy.set(false)));
    return logical;
  }

  private CompletionStage<Integer> dispatch(
      List<Hint> hints, int offset, int hydrated, long started) {
    if (closed || offset >= hints.size()) return CompletableFuture.completedFuture(hydrated);
    Duration remaining = Duration.ofSeconds(5).minusNanos(Math.max(0, System.nanoTime() - started));
    if (remaining.isNegative() || remaining.isZero())
      return CompletableFuture.completedFuture(hydrated);
    int end = Math.min(offset + 64, hints.size());
    var operations = new ArrayList<CompletableFuture<Boolean>>();
    Duration budget =
        remaining.compareTo(Duration.ofSeconds(2)) > 0 ? Duration.ofSeconds(2) : remaining;
    for (int i = offset; i < end; i++) {
      try {
        operations.add(
            wake.apply(hints.get(i).call(), budget)
                .toCompletableFuture()
                .orTimeout(budget.toMillis(), TimeUnit.MILLISECONDS)
                .exceptionally(e -> false));
      } catch (RuntimeException unavailable) {
        operations.add(CompletableFuture.completedFuture(false));
      }
    }
    return CompletableFuture.allOf(operations.toArray(CompletableFuture[]::new))
        .thenComposeAsync(
            done ->
                dispatch(
                    hints,
                    end,
                    hydrated + (int) operations.stream().filter(CompletableFuture::join).count(),
                    started),
            worker);
  }

  @Override
  public void close() {
    closed = true;
    worker.shutdown();
  }
}
