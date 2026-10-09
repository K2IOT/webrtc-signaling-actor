package io.webrtc.signaling.app.runtime;

import io.webrtc.signaling.actors.cluster.ShardingBootstrap;
import io.webrtc.signaling.app.PekkoShutdownLifecycle;
import java.time.Duration;
import java.util.concurrent.*;
import org.apache.pekko.actor.CoordinatedShutdown;
import org.apache.pekko.actor.typed.ActorSystem;
import org.apache.pekko.actor.typed.javadsl.Adapter;
import org.apache.pekko.actor.typed.javadsl.Behaviors;
import org.apache.pekko.cluster.Cluster;
import org.apache.pekko.http.javadsl.ConnectionContext;
import org.apache.pekko.http.javadsl.model.Uri;
import org.apache.pekko.management.javadsl.PekkoManagement;

/**
 * Published before asynchronous management binding; one original process owns startup and
 * retirement.
 */
public final class NativeActorProcess {
  private final ActorSystem<Void> system;
  private final NativeActorProcessEnrollment enrollment;
  private final CompletableFuture<Void> up = new CompletableFuture<>();
  private CompletionStage<Uri> binding, started;
  private CompletionStage<Void> managementStopped, drained;
  private long startupAt;

  public NativeActorProcess(NativeActorProcessEnrollment enrollment) {
    this.enrollment = enrollment;
    system =
        ActorSystem.create(
            Behaviors.empty(),
            enrollment.systemName(),
            PekkoShutdownLifecycle.config(enrollment.config()));
    system
        .getWhenTerminated()
        .whenComplete(
            (done, error) ->
                up.completeExceptionally(
                    new IllegalStateException("Native actor process terminated before Up")));
    CoordinatedShutdown.get(system)
        .addTask(
            "service-unbind",
            "signaling-managed-management-stop",
            () -> stopManagement().thenApply(v -> org.apache.pekko.Done.getInstance()));
    CoordinatedShutdown.get(system)
        .addTask(
            "before-actor-system-terminate",
            "signaling-managed-root-drain",
            () ->
                io.webrtc.signaling.actors.lease.PostgresShardLeaseProvider.drainIfInstalled(
                        Adapter.toClassic(system))
                    .thenApply(v -> org.apache.pekko.Done.getInstance()));
  }

  public ActorSystem<?> system() {
    return system;
  }

  /** The 30s budget starts once, covers original binding/bootstrap and local membership. */
  public synchronized CompletionStage<Void> start() {
    if (drained != null || CoordinatedShutdown.get(system).getShutdownReason().isPresent())
      return CompletableFuture.failedFuture(
          new IllegalStateException("Native actor process draining"));
    if (started != null) return up.minimalCompletionStage();
    startupAt = System.nanoTime();
    var server =
        ConnectionContext.httpsServer(
            () -> {
              var engine = enrollment.managementServerTls().createSSLEngine();
              engine.setUseClientMode(false);
              engine.setEnabledProtocols(new String[] {"TLSv1.3"});
              engine.setNeedClientAuth(true);
              return engine;
            });
    var client =
        ConnectionContext.httpsClient(
            (host, port) -> {
              var engine = enrollment.managementClientTls().createSSLEngine(host, port);
              engine.setUseClientMode(true);
              engine.setEnabledProtocols(new String[] {"TLSv1.3"});
              var parameters = engine.getSSLParameters();
              parameters.setEndpointIdentificationAlgorithm("HTTPS");
              engine.setSSLParameters(parameters);
              return engine;
            });
    var memberUp = new CompletableFuture<Void>();
    Cluster.get(Adapter.toClassic(system)).registerOnMemberUp(() -> memberUp.complete(null));
    try {
      binding = ShardingBootstrap.bindManagement(system, server, client);
      started =
          binding.thenApply(
              uri -> {
                org.apache.pekko.management.cluster.bootstrap.ClusterBootstrap.get(system).start();
                return uri;
              });
    } catch (RuntimeException failure) {
      binding = CompletableFuture.failedFuture(failure);
      started = binding;
    }
    started
        .thenCompose(bound -> memberUp)
        .whenComplete(
            (v, failure) -> {
              if (failure != null) up.completeExceptionally(failure);
              else up.complete(null);
            });
    system
        .scheduler()
        .scheduleOnce(
            Duration.ofSeconds(30),
            () ->
                up.completeExceptionally(
                    new TimeoutException("Native actor membership Up not established within 30s")),
            system.executionContext());
    return up.minimalCompletionStage();
  }

  public ActorSystem<?> awaitLocalUp() {
    var gate = start();
    long remaining =
        Math.max(0, Duration.ofSeconds(30).toNanos() - (System.nanoTime() - startupAt));
    try {
      gate.toCompletableFuture().get(remaining, TimeUnit.NANOSECONDS);
      return system;
    } catch (InterruptedException interrupted) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("Native actor formation interrupted", interrupted);
    } catch (Exception failure) {
      throw new IllegalStateException("Native actor formation failed", failure);
    }
  }

  private synchronized CompletionStage<Void> stopManagement() {
    if (managementStopped == null) {
      var original = binding == null ? CompletableFuture.<Uri>completedFuture(null) : binding;
      // A late binding must complete before its native stop; failure means no binding was
      // installed.
      managementStopped =
          original
              .handle((uri, failure) -> uri)
              .thenCompose(
                  uri ->
                      uri == null
                          ? CompletableFuture.completedFuture(null)
                          : PekkoManagement.get(system).stop().thenApply(done -> (Void) null))
              .toCompletableFuture()
              .minimalCompletionStage();
    }
    return managementStopped;
  }

  public synchronized CompletionStage<Void> drain() {
    if (drained == null)
      drained =
          CoordinatedShutdown.get(system)
              .runAll(CoordinatedShutdown.unknownReason())
              .thenCompose(done -> stopManagement())
              .thenCompose(v -> system.getWhenTerminated().thenApply(done -> (Void) null))
              .toCompletableFuture()
              .minimalCompletionStage();
    return drained;
  }
}
