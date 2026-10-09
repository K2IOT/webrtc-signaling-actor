package io.webrtc.signaling.app.runtime;

import io.webrtc.signaling.auth.BoundedTokenVerifier;
import io.webrtc.signaling.storage.*;
import java.util.*;
import java.util.concurrent.*;
import org.springframework.context.SmartLifecycle;

/** Retires original control resources after Boot's HTTPS drain and before its HTTP loops. */
public final class NativeControlProcess implements SmartLifecycle {
  private final NativeControlReadiness readiness;
  private final BoundedTokenVerifier tokens;
  private final List<DbBoundary> boundaries;
  private final List<DbPools> pools;
  private final List<NativeWorkerScheduler> workers;
  private final List<NativeClockSource> clocks;
  private final List<NativeRevocationSource> revocations;
  private final List<NativeCellHealthSource> primaries;
  private final List<NativeCachedRevocationSource> cachedSources;
  private PrivateHealthServer health;
  private volatile boolean running;
  private CompletionStage<Void> drained;
  private long drainStarted;

  NativeControlProcess(
      NativeControlBusinessEnrollment business,
      BoundedTokenVerifier tokens,
      List<SqlTransactions> sql,
      List<NativeWorkerScheduler> workers,
      List<NativeClockSource> clocks,
      List<NativeRevocationSource> revocations,
      List<NativeCellHealthSource> primaries,
      List<NativeCachedRevocationSource> cachedSources) {
    if (tokens != business.tokens())
      throw new IllegalArgumentException("Native control verifier ownership differs");
    if (!sql.containsAll(business.directory().transactionOwners()))
      throw new IllegalArgumentException("Control directory SQL owners are not enrolled");
    if (clocks.stream().anyMatch(source -> !source.monitors(business.clock())))
      throw new IllegalArgumentException("Control clock ownership differs");
    readiness = new NativeControlReadiness(business);
    this.tokens = tokens;
    boundaries = distinct(sql.stream().map(SqlTransactions::boundary).toList());
    pools = distinct(sql.stream().map(SqlTransactions::pools).toList());
    this.workers = distinct(workers);
    this.clocks = distinct(clocks);
    this.revocations = distinct(revocations);
    this.primaries = distinct(primaries);
    this.cachedSources = distinct(cachedSources);
  }

  private static <T> List<T> distinct(List<T> values) {
    Set<T> identities = Collections.newSetFromMap(new IdentityHashMap<>());
    var result = new ArrayList<T>();
    for (var value : values) if (identities.add(Objects.requireNonNull(value))) result.add(value);
    return List.copyOf(result);
  }

  synchronized void installHealth(PrivateHealthServer health) {
    if (this.health != null || drained != null)
      throw new IllegalStateException("Control probe ownership already sealed");
    this.health = Objects.requireNonNull(health);
  }

  NativeControlReadiness readiness() {
    return readiness;
  }

  @Override
  public synchronized void start() {
    if (drained != null || running)
      throw new IllegalStateException("Control process already started or drained");
    for (var worker : workers) worker.start();
    running = true;
  }

  @Override
  public boolean isRunning() {
    return running;
  }

  // Boot graceful shutdown is MAX_VALUE-1024 and web-server stop is MAX_VALUE-2048.
  @Override
  public int getPhase() {
    return Integer.MAX_VALUE - 4096;
  }

  public synchronized CompletionStage<Void> drain() {
    if (drained != null) return drained;
    drainStarted = System.nanoTime();
    readiness.stop();
    running = false;
    var physical = new ArrayList<CompletableFuture<?>>();
    for (var worker : workers) physical.add(worker.drain().toCompletableFuture());
    for (var clock : clocks) physical.add(clock.drain().toCompletableFuture());
    for (var source : revocations) physical.add(source.drain().toCompletableFuture());
    for (var source : primaries) physical.add(source.drain().toCompletableFuture());
    for (var source : cachedSources) physical.add(source.drain().toCompletableFuture());
    physical.add(tokens.drain().toCompletableFuture());
    for (var boundary : boundaries) physical.add(boundary.drain().toCompletableFuture());
    drained =
        CompletableFuture.allOf(physical.toArray(CompletableFuture[]::new))
            .thenCompose(
                v -> health == null ? CompletableFuture.completedFuture(null) : health.stop())
            .thenRun(() -> pools.forEach(DbPools::close))
            .minimalCompletionStage();
    return drained;
  }

  /**
   * Failed construction cannot infer ownership from a pool alone. Use only published native SQL
   * bindings.
   */
  public static CompletionStage<Void> drainUninstalled(List<Object> owners) {
    var sql = new ArrayList<>(of(owners, SqlTransactions.class));
    of(owners, NativeControlDatabaseResources.class)
        .forEach(
            resources -> {
              sql.add(resources.regional());
              sql.add(resources.local());
            });
    var boundaries = new ArrayList<>(of(owners, DbBoundary.class));
    sql.forEach(owner -> boundaries.add(owner.boundary()));
    var tokens = new ArrayList<>(of(owners, BoundedTokenVerifier.class));
    of(owners, NativeControlBusinessEnrollment.class)
        .forEach(business -> tokens.add(business.tokens()));
    var pools = distinct(sql.stream().map(SqlTransactions::pools).toList());
    var physical = new ArrayList<CompletableFuture<?>>();
    of(owners, NativeControlReadiness.class).forEach(NativeControlReadiness::stop);
    of(owners, NativeWorkerScheduler.class)
        .forEach(worker -> physical.add(worker.drain().toCompletableFuture()));
    of(owners, NativeClockSource.class)
        .forEach(clock -> physical.add(clock.drain().toCompletableFuture()));
    of(owners, NativeRevocationSource.class)
        .forEach(source -> physical.add(source.drain().toCompletableFuture()));
    of(owners, NativeCellHealthSource.class)
        .forEach(source -> physical.add(source.drain().toCompletableFuture()));
    of(owners, NativeCachedRevocationSource.class)
        .forEach(source -> physical.add(source.drain().toCompletableFuture()));
    distinct(tokens).forEach(token -> physical.add(token.drain().toCompletableFuture()));
    distinct(boundaries).forEach(boundary -> physical.add(boundary.drain().toCompletableFuture()));
    return CompletableFuture.allOf(physical.toArray(CompletableFuture[]::new))
        .thenCompose(
            v ->
                CompletableFuture.allOf(
                    of(owners, PrivateHealthServer.class).stream()
                        .map(health -> health.stop().toCompletableFuture())
                        .toArray(CompletableFuture[]::new)))
        .thenRun(
            () -> {
              pools.forEach(DbPools::close);
              if (of(owners, DbPools.class).stream().anyMatch(pool -> !pools.contains(pool)))
                throw new IllegalStateException(
                    "Control SQL ownership unproven; unbound pools retained");
            })
        .minimalCompletionStage();
  }

  private static <T> List<T> of(List<Object> owners, Class<T> type) {
    return owners.stream().filter(type::isInstance).map(type::cast).toList();
  }

  /** Repeated stop/failure observers share the original stage and Spring's original 30s budget. */
  public void awaitDrain() throws Exception {
    var original = drain();
    long remaining = TimeUnit.SECONDS.toNanos(30) - (System.nanoTime() - drainStarted);
    original.toCompletableFuture().get(Math.max(0, remaining), TimeUnit.NANOSECONDS);
  }

  @Override
  public void stop(Runnable stopped) {
    drain().thenRun(stopped);
  }

  @Override
  public void stop() {
    try {
      awaitDrain();
    } catch (InterruptedException interrupted) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("Native control cleanup interrupted", interrupted);
    } catch (Exception unknown) {
      throw new IllegalStateException("Native control cleanup unproven", unknown);
    }
  }
}
