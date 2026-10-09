package io.webrtc.signaling.app.runtime;

import io.webrtc.signaling.app.PekkoShutdownLifecycle;
import io.webrtc.signaling.app.ShutdownCoordinator;
import io.webrtc.signaling.auth.BoundedTokenVerifier;
import io.webrtc.signaling.rpc.*;
import io.webrtc.signaling.storage.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Function;
import org.apache.pekko.actor.CoordinatedShutdown;
import org.apache.pekko.actor.typed.javadsl.Adapter;
import org.apache.pekko.cluster.Cluster;

/** Failed refresh still owns the original resources; it uses Pekko's original handoff graph. */
public final class NativeActorStartupDrain implements ShutdownCoordinator.Hooks {
  private final List<Object> owners;
  private final NativeActorComposition actors;
  private boolean released, closed;
  private CompletionStage<Void> closure;

  private NativeActorStartupDrain(List<Object> owners, NativeActorComposition actors) {
    this.owners = List.copyOf(owners);
    this.actors = actors;
  }

  public static CompletionStage<Void> drain(List<Object> owners) {
    var compositions = of(owners, NativeActorComposition.class);
    if (compositions.size() > 1)
      return CompletableFuture.failedFuture(
          new IllegalStateException("Ambiguous native actor ownership"));
    var actors = compositions.isEmpty() ? null : compositions.getFirst();
    var installed = of(owners, NativeActorRuntimeHooks.class);
    if (!installed.isEmpty()) {
      if (actors == null || installed.size() != 1)
        return CompletableFuture.failedFuture(
            new IllegalStateException("Native shutdown ownership unproven"));
      return CoordinatedShutdown.get(actors.system())
          .runAll(CoordinatedShutdown.unknownReason())
          .thenApply(
              done -> {
                if (!installed.getFirst().databaseClosed())
                  throw new IllegalStateException("Native database closure unproven");
                return (Void) null;
              });
    }
    var partial = new NativeActorStartupDrain(owners, actors);
    if (actors == null) {
      partial.readinessOff();
      partial.shedIngress();
      partial.released = true;
      return partial
          .settleAdmitted()
          .thenCompose(v -> partial.drainType(NativeActorProcess.class, NativeActorProcess::drain))
          .thenCompose(v -> partial.closeDatabase());
    }
    PekkoShutdownLifecycle.register(actors.system(), partial);
    return CoordinatedShutdown.get(actors.system())
        .runAll(CoordinatedShutdown.unknownReason())
        .thenApply(
            done -> {
              if (!partial.closed)
                throw new IllegalStateException("Native startup cleanup unproven");
              return (Void) null;
            });
  }

  @Override
  public void readinessOff() {
    if (actors != null) actors.readiness().beginDrain();
  }

  @Override
  public void shedIngress() {
    if (actors != null) actors.shedNewAcquisition();
    of(owners, NativeWorkerScheduler.class).forEach(NativeWorkerScheduler::shedNormal);
  }

  @Override
  public CompletionStage<Integer> reconnectBatch(int maximum) {
    return CompletableFuture.completedFuture(0);
  }

  @Override
  public CompletionStage<Void> settleAdmitted() {
    if (!of(owners, DbPools.class).isEmpty() && of(owners, DbBoundary.class).isEmpty())
      return CompletableFuture.failedFuture(
          new IllegalStateException("Native database physical ownership unproven"));
    var receipts = new ArrayList<CompletionStage<?>>();
    if (actors != null) receipts.add(actors.ingress().settleAdmitted());
    of(owners, NativeWorkerScheduler.class).forEach(o -> receipts.add(o.settleAdmitted()));
    of(owners, CellRpcServer.class).forEach(o -> receipts.add(o.settleAdmitted()));
    of(owners, NativeActorRpcIngress.class).forEach(o -> receipts.add(o.server().settleAdmitted()));
    of(owners, CellRpcClient.class).forEach(o -> receipts.add(o.settleAdmitted()));
    of(owners, DbBoundary.class).forEach(o -> receipts.add(o.settleAdmitted()));
    return all(receipts);
  }

  @Override
  public CompletionStage<Void> handoffAndRelease() {
    if (actors == null || !Cluster.get(Adapter.toClassic(actors.system())).isTerminated())
      return CompletableFuture.failedFuture(
          new IllegalStateException("Framework handoff/leave unproven"));
    return drainType(NativeWorkerScheduler.class, NativeWorkerScheduler::drain)
        .thenCompose(v -> actors.ingress().drain())
        .thenCompose(v -> actors.drainRoots())
        .thenRun(() -> released = true);
  }

  @Override
  public CompletionStage<Void> leaveCluster() {
    return CompletableFuture.failedFuture(
        new IllegalStateException("Pekko exclusively owns cluster leave"));
  }

  @Override
  public synchronized CompletionStage<Void> closeDatabase() {
    if (closure != null) return closure;
    if (!released)
      return CompletableFuture.failedFuture(
          new IllegalStateException("Native root cleanup unproven"));
    closure =
        drainType(NativeWorkerScheduler.class, NativeWorkerScheduler::drain)
            .thenCompose(v -> drainType(CellRpcServer.class, CellRpcServer::drain))
            .thenCompose(v -> drainType(NativeActorRpcIngress.class, NativeActorRpcIngress::drain))
            .thenCompose(v -> drainType(CellRpcClient.class, CellRpcClient::drain))
            .thenCompose(
                v -> drainType(NativeActorSafetySources.class, NativeActorSafetySources::drain))
            .thenCompose(v -> drainType(NativeClockSource.class, NativeClockSource::drain))
            .thenCompose(
                v -> drainType(NativeCellHealthSource.class, NativeCellHealthSource::drain))
            .thenCompose(
                v -> drainType(NativeRevocationSource.class, NativeRevocationSource::drain))
            .thenCompose(
                v ->
                    actors == null
                        ? CompletableFuture.completedFuture(null)
                        : actors.drainVerification())
            .thenCompose(v -> drainType(BoundedTokenVerifier.class, BoundedTokenVerifier::drain))
            .thenCompose(v -> drainType(DbBoundary.class, DbBoundary::drain))
            .thenRun(() -> of(owners, DbPools.class).forEach(DbPools::close))
            .thenCompose(v -> drainType(PrivateHealthServer.class, PrivateHealthServer::stop))
            .thenRun(() -> closed = true)
            .toCompletableFuture()
            .minimalCompletionStage();
    return closure;
  }

  private <T> CompletionStage<Void> drainType(
      Class<T> type, Function<T, CompletionStage<Void>> drain) {
    try {
      return all(of(owners, type).stream().map(drain).toList());
    } catch (RuntimeException unproven) {
      return CompletableFuture.failedFuture(unproven);
    }
  }

  private static <T> List<T> of(List<Object> owners, Class<T> type) {
    return owners.stream().filter(type::isInstance).map(type::cast).toList();
  }

  private static CompletionStage<Void> all(List<? extends CompletionStage<?>> receipts) {
    return CompletableFuture.allOf(
            receipts.stream()
                .map(CompletionStage::toCompletableFuture)
                .toArray(CompletableFuture[]::new))
        .minimalCompletionStage();
  }
}
