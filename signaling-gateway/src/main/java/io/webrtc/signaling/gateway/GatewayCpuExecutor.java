package io.webrtc.signaling.gateway;

import java.util.List;
import java.util.concurrent.*;

/** Independent bounded service for critical decode and its synchronous downstream handoff. */
final class GatewayCpuExecutor extends AbstractExecutorService {
  private static final ThreadLocal<Boolean> CRITICAL = new ThreadLocal<>();
  private final ExecutorService normal = pool("gateway-json-", 768);
  private final ExecutorService critical = pool("gateway-critical-json-", 256);

  private static ExecutorService pool(String name, int queued) {
    return new ThreadPoolExecutor(
        2,
        2,
        0,
        TimeUnit.SECONDS,
        new ArrayBlockingQueue<>(queued),
        Thread.ofPlatform().daemon().name(name, 0).factory(),
        new ThreadPoolExecutor.AbortPolicy());
  }

  static void handoff(boolean critical, Runnable work) {
    Boolean previous = CRITICAL.get();
    try {
      CRITICAL.set(critical);
      work.run();
    } finally {
      if (previous == null) CRITICAL.remove();
      else CRITICAL.set(previous);
    }
  }

  void execute(boolean priority, Runnable task) {
    (priority ? critical : normal).execute(() -> handoff(priority, task));
  }

  @Override
  public void execute(Runnable task) {
    execute(Boolean.TRUE.equals(CRITICAL.get()), task);
  }

  @Override
  public void shutdown() {
    normal.shutdown();
    critical.shutdown();
  }

  @Override
  public List<Runnable> shutdownNow() {
    var tasks = new java.util.ArrayList<>(normal.shutdownNow());
    tasks.addAll(critical.shutdownNow());
    return tasks;
  }

  @Override
  public boolean isShutdown() {
    return normal.isShutdown() && critical.isShutdown();
  }

  @Override
  public boolean isTerminated() {
    return normal.isTerminated() && critical.isTerminated();
  }

  @Override
  public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
    long start = System.nanoTime(), budget = unit.toNanos(timeout);
    return normal.awaitTermination(timeout, unit)
        && critical.awaitTermination(
            Math.max(0, budget - (System.nanoTime() - start)), TimeUnit.NANOSECONDS);
  }
}
