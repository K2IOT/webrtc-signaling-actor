package io.webrtc.signaling.app;

import io.webrtc.signaling.app.runtime.*;
import io.webrtc.signaling.auth.BoundedTokenVerifier;
import io.webrtc.signaling.gateway.*;
import io.webrtc.signaling.rpc.*;
import io.webrtc.signaling.storage.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ConfigurableApplicationContext;

/** Startup failure retires only already-created native owners, in physical order. */
final class NativeStartupCleanup implements BeanPostProcessor {
  static final String OWNER_BEAN = "nativeStartupCleanupOwner";

  record Owner(NativeStartupCleanup cleanup) {}

  private final List<Object> created = new ArrayList<>();
  private CompletionStage<Void> actorDrain;
  private long actorStarted;
  private CompletionStage<Void> controlDrain;
  private long controlStarted;

  @Override
  public synchronized Object postProcessAfterInitialization(Object bean, String name) {
    if (bean instanceof NativeGatewayIngress
        || bean instanceof NativeGatewaySpringLifecycle
        || bean instanceof NativeActorProcess
        || bean instanceof NativeActorComposition
        || bean instanceof NativeActorRuntimeHooks
        || bean instanceof NativeActorSpringLifecycle
        || bean instanceof NativeControlProcess
        || bean instanceof NativeControlReadiness
        || bean instanceof NativeControlDatabaseResources
        || bean instanceof NativeControlBusinessEnrollment
        || bean instanceof SqlTransactions
        || bean instanceof NativeActorRpcIngress
        || bean instanceof NativeWorkerScheduler
        || bean instanceof PrivateHealthServer
        || bean instanceof NativeActorSafetySources
        || bean instanceof NativeClockSource
        || bean instanceof NativeRevocationSource
        || bean instanceof NativeCellHealthSource
        || bean instanceof NativeCachedRevocationSource
        || bean instanceof CellRpcServer
        || bean instanceof DbBoundary
        || bean instanceof DbPools
        || bean instanceof NativeRelaySessionProofCache
        || bean instanceof CellRpcClient
        || bean instanceof GatewayBootController
        || bean instanceof BoundedTokenVerifier)
      if (created.stream().noneMatch(owner -> owner == bean)) created.add(bean);
    return bean;
  }

  synchronized void capture(ApplicationContext context) {
    if (context instanceof ConfigurableApplicationContext configurable) {
      var beans = configurable.getBeanFactory();
      for (var name : beans.getSingletonNames())
        postProcessAfterInitialization(beans.getSingleton(name), name);
    }
  }

  synchronized void failed(ApplicationContext context, Throwable failed) {
    retire(context, failed, List.copyOf(created));
  }

  static void cleanup(ApplicationContext context, Throwable failed) {
    if (!(context instanceof ConfigurableApplicationContext configurable)) return;
    var beans = configurable.getBeanFactory();
    var observed = beans.getSingleton(OWNER_BEAN);
    var cleanup = observed instanceof Owner owner ? owner.cleanup() : new NativeStartupCleanup();
    cleanup.capture(context);
    cleanup.failed(context, failed);
  }

  private void retire(ApplicationContext context, Throwable failed, List<Object> owners) {
    if (context == null) return;
    var profiles = List.of(context.getEnvironment().getActiveProfiles());
    if (profiles.contains("actor")) {
      try {
        if (actorDrain == null) {
          actorStarted = System.nanoTime();
          actorDrain = NativeActorStartupDrain.drain(owners);
        }
        long remaining =
            Math.max(0, TimeUnit.SECONDS.toNanos(65) - (System.nanoTime() - actorStarted));
        actorDrain.toCompletableFuture().get(remaining, TimeUnit.NANOSECONDS);
      } catch (InterruptedException interrupted) {
        Thread.currentThread().interrupt();
        failed.addSuppressed(new IllegalStateException("Native startup cleanup interrupted"));
      } catch (Exception unknown) {
        failed.addSuppressed(new IllegalStateException("Native startup cleanup unproven"));
      }
      return;
    }
    if (profiles.contains("control")) {
      try {
        for (var owner : owners)
          if (owner instanceof NativeControlReadiness readiness) readiness.stop();
        var process =
            owners.stream()
                .filter(NativeControlProcess.class::isInstance)
                .map(NativeControlProcess.class::cast)
                .findFirst();
        if (process.isPresent()) process.get().awaitDrain();
        else {
          if (controlDrain == null) {
            controlStarted = System.nanoTime();
            controlDrain = NativeControlProcess.drainUninstalled(owners);
          }
          controlDrain
              .toCompletableFuture()
              .get(
                  Math.max(0, TimeUnit.SECONDS.toNanos(30) - (System.nanoTime() - controlStarted)),
                  TimeUnit.NANOSECONDS);
        }
      } catch (InterruptedException interrupted) {
        Thread.currentThread().interrupt();
        failed.addSuppressed(new IllegalStateException("Native startup cleanup interrupted"));
      } catch (Exception unknown) {
        failed.addSuppressed(new IllegalStateException("Native startup cleanup unproven"));
      }
      return;
    }
    if (!profiles.contains("gateway")) return;
    var lifecycle =
        owners.stream()
            .filter(NativeGatewaySpringLifecycle.class::isInstance)
            .map(NativeGatewaySpringLifecycle.class::cast)
            .findFirst();
    long started = System.nanoTime();
    try {
      if (lifecycle.isPresent())
        lifecycle.get().drain().toCompletableFuture().get(310, TimeUnit.SECONDS);
      else {
        for (var owner : owners)
          if (owner instanceof NativeGatewayIngress ingress) await(ingress.drain(), started);
        for (var owner : owners)
          if (owner instanceof NativeRelaySessionProofCache cache) await(cache.drain(), started);
        for (var owner : owners)
          if (owner instanceof BoundedTokenVerifier tokens) await(tokens.drain(), started);
        for (var owner : owners)
          if (owner instanceof CellRpcClient client) await(client.drain(), started);
        for (var owner : owners) if (owner instanceof GatewayBootController boot) boot.close();
        for (var owner : owners)
          if (owner instanceof NativeWorkerScheduler worker) await(worker.drain(), started);
        for (var owner : owners)
          if (owner instanceof NativeClockSource clock) await(clock.drain(), started);
        for (var owner : owners)
          if (owner instanceof NativeCachedRevocationSource source) await(source.drain(), started);
        for (var owner : owners)
          if (owner instanceof PrivateHealthServer health) await(health.stop(), started);
      }
    } catch (InterruptedException interrupted) {
      Thread.currentThread().interrupt();
      failed.addSuppressed(new IllegalStateException("Native startup cleanup interrupted"));
    } catch (Exception unknown) {
      failed.addSuppressed(new IllegalStateException("Native startup cleanup unproven"));
    }
  }

  private static void await(CompletionStage<Void> completion, long started) throws Exception {
    long remaining = TimeUnit.SECONDS.toNanos(300) - (System.nanoTime() - started);
    if (remaining <= 0) throw new TimeoutException();
    completion.toCompletableFuture().get(remaining, TimeUnit.NANOSECONDS);
  }
}
