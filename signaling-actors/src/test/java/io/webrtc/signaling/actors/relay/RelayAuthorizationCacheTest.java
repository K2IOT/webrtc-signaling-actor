package io.webrtc.signaling.actors.relay;

import static org.assertj.core.api.Assertions.*;

import io.webrtc.signaling.protocol.Identity.*;
import io.webrtc.signaling.storage.AuthoritySql;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import org.junit.jupiter.api.Test;

class RelayAuthorizationCacheTest {
  final AtomicLong now = new AtomicLong();
  final AtomicBoolean trusted = new AtomicBoolean(true);
  final CallId call = new CallId("c001.e1.00000000-0000-0000-0000-000000000001");
  final AuthenticatedSession sender =
      new AuthenticatedSession(
          new UserId("sender"),
          new SessionKey("TEST_ONLY", "s"),
          new SessionIncarnation(UUID.randomUUID()),
          1,
          UUID.randomUUID());
  final AuthenticatedSession recipient =
      new AuthenticatedSession(
          new UserId("receiver"),
          new SessionKey("TEST_ONLY", "r"),
          new SessionIncarnation(UUID.randomUUID()),
          1,
          UUID.randomUUID());
  final AtomicReference<AuthoritySql.GroupToken> root =
      new AtomicReference<>(
          new AuthoritySql.GroupToken("c001", 1, 1, 685, 1, "TEST_ONLY_OWNER", UUID.randomUUID()));

  RelayAuthorizationCache cache() {
    return new RelayAuthorizationCache(4, 1, now::get, trusted::get, c -> Optional.of(root.get()));
  }

  RelayAuthorizationCache.Snapshot snapshot(
      long tokenUntil, long reservationUntil, long groupUntil) {
    return new RelayAuthorizationCache.Snapshot(
        call,
        UUID.randomUUID(),
        2,
        1,
        1,
        "CONNECTING",
        sender,
        recipient,
        root.get(),
        now.get(),
        tokenUntil,
        reservationUntil,
        groupUntil,
        Duration.ofSeconds(30).toNanos());
  }

  @Test
  void ageIsAtMostFiveSecondsAndNeverOutlivesAnyNativeAuthorityDeadline() {
    for (long expiry :
        new long[] {Duration.ofSeconds(2).toNanos(), Duration.ofSeconds(8).toNanos()}) {
      now.set(0);
      var cache = cache();
      cache.put(
          snapshot(expiry, Duration.ofSeconds(20).toNanos(), Duration.ofSeconds(20).toNanos()));
      assertThat(cache.get(call, sender, 1, 1)).isPresent();
      now.set(Math.min(expiry, Duration.ofSeconds(5).toNanos()));
      assertThat(cache.get(call, sender, 1, 1)).isEmpty();
    }
    now.set(0);
    var cache = cache();
    cache.put(
        snapshot(
            Duration.ofSeconds(20).toNanos(),
            Duration.ofSeconds(1).toNanos(),
            Duration.ofSeconds(20).toNanos()));
    now.set(Duration.ofSeconds(1).toNanos());
    assertThat(cache.get(call, sender, 1, 1)).isEmpty();
  }

  @Test
  void takeoverGenerationRoundAndClockUncertaintyInvalidateRelay() {
    var cache = cache();
    cache.put(snapshot(30000000000L, 30000000000L, 30000000000L));
    assertThat(cache.get(call, sender, 2, 1)).isEmpty();
    var newer =
        new AuthenticatedSession(
            sender.userId(), sender.key(), sender.incarnation(), 2, UUID.randomUUID());
    assertThat(cache.get(call, newer, 1, 1)).isEmpty();
    trusted.set(false);
    assertThat(cache.get(call, sender, 1, 1)).isEmpty();
    trusted.set(true);
    root.set(
        new AuthoritySql.GroupToken(
            "c001", 1, 1, 685, 2, "TEST_ONLY_NEW_OWNER", UUID.randomUUID()));
    assertThat(cache.get(call, sender, 1, 1)).isEmpty();
  }

  @Test
  void trafficRefreshIsSingleFlightAndBoundedWithNoPeriodicIdleRefresh() {
    var cache = cache();
    var primary = new CompletableFuture<RelayAuthorizationCache.Snapshot>();
    var loads = new AtomicInteger();
    var a =
        cache.refresh(
            call,
            sender,
            1,
            1,
            Duration.ofSeconds(1),
            () -> {
              loads.incrementAndGet();
              return new io.webrtc.signaling.actors.admission.ActorOperation<>(
                  primary, primary.thenApply(value -> null));
            });
    var b =
        cache.refresh(
            call,
            sender,
            1,
            1,
            Duration.ofSeconds(1),
            () -> {
              loads.incrementAndGet();
              throw new AssertionError();
            });
    assertThat(loads).hasValue(1);
    primary.complete(snapshot(30000000000L, 30000000000L, 30000000000L));
    assertThat(a.toCompletableFuture().join()).isEqualTo(b.toCompletableFuture().join());
    assertThat(cache.pending()).isZero();
    assertThat(cache.size()).isEqualTo(1);
  }

  @Test
  void unknownRefreshRetainsAdmissionUntilIndependentCleanup() {
    var cache = cache();
    var logical = new CompletableFuture<RelayAuthorizationCache.Snapshot>();
    var physical = new CompletableFuture<Void>();
    var operation =
        cache.refresh(
            call,
            sender,
            1,
            1,
            Duration.ofSeconds(1),
            () -> new io.webrtc.signaling.actors.admission.ActorOperation<>(logical, physical));
    logical.completeExceptionally(new io.webrtc.signaling.storage.DbOutcomeUnknownException());
    assertThat(operation.toCompletableFuture()).isCompletedExceptionally();
    assertThat(cache.pending()).isEqualTo(1);
    physical.complete(null);
    assertThat(cache.pending()).isZero();
  }

  @Test
  void exceptionalCleanupDoesNotProveRefreshCreditCanBeReused() {
    var cache = cache();
    var physical = new CompletableFuture<Void>();
    var operation =
        cache.refresh(
            call,
            sender,
            1,
            1,
            Duration.ofSeconds(1),
            () ->
                new io.webrtc.signaling.actors.admission.ActorOperation<>(
                    CompletableFuture.failedFuture(
                        new io.webrtc.signaling.storage.DbOutcomeUnknownException()),
                    physical));
    assertThat(operation.toCompletableFuture()).isCompletedExceptionally();
    physical.completeExceptionally(new IllegalStateException("TEST_ONLY_CLEANUP_UNKNOWN"));
    assertThat(cache.pending()).isEqualTo(1);
    assertThat(
            cache
                .refresh(
                    call,
                    sender,
                    2,
                    2,
                    Duration.ofSeconds(1),
                    () -> {
                      throw new AssertionError("Unknown physical work cannot admit replacement");
                    })
                .toCompletableFuture())
        .isCompletedExceptionally();
  }

  @Test
  void throwingFactoryDoesNotProveNativeWorkNeverStarted() {
    var cache = cache();
    var started = new AtomicInteger();
    var operation =
        cache.refresh(
            call,
            sender,
            1,
            1,
            Duration.ofSeconds(1),
            () -> {
              started.incrementAndGet();
              throw new IllegalStateException("TEST_ONLY_THROW_AFTER_START");
            });
    assertThat(operation.toCompletableFuture()).isCompletedExceptionally();
    assertThat(started).hasValue(1);
    assertThat(cache.pending()).isEqualTo(1);
    assertThat(
            cache
                .refresh(
                    call,
                    sender,
                    2,
                    2,
                    Duration.ofSeconds(1),
                    () -> {
                      throw new AssertionError("Physical start was not disproven");
                    })
                .toCompletableFuture())
        .isCompletedExceptionally();
  }

  @Test
  void trackedRefreshExposesTheOriginalReceiptAcrossSuccessfulLogicalAuthorization() {
    var cache = cache();
    var logical = new CompletableFuture<RelayAuthorizationCache.Snapshot>();
    var physical = new CompletableFuture<Void>();
    var work =
        cache.refreshTracked(
            call,
            sender,
            1,
            1,
            Duration.ofSeconds(1),
            () -> new io.webrtc.signaling.actors.admission.ActorOperation<>(logical, physical));
    logical.complete(snapshot(30000000000L, 30000000000L, 30000000000L));
    assertThat(work.logical().toCompletableFuture()).isDone();
    assertThat(work.physicalCompletion().toCompletableFuture()).isNotDone();
    physical.complete(null);
    assertThat(work.physicalCompletion().toCompletableFuture()).isDone();
    assertThat(cache.pending()).isZero();
  }

  @Test
  void trackedThrowingRefreshKeepsUnknownPhysicalOwnership() {
    var cache = cache();
    var work =
        cache.refreshTracked(
            call,
            sender,
            1,
            1,
            Duration.ofSeconds(1),
            () -> {
              throw new IllegalStateException("TEST_ONLY_UNKNOWN_FACTORY");
            });
    assertThat(work.logical().toCompletableFuture()).isCompletedExceptionally();
    assertThat(work.physicalCompletion().toCompletableFuture()).isNotDone();
    assertThat(cache.pending()).isEqualTo(1);
  }

  @Test
  void coalescedRefreshKeepsEachOriginalCallerDeadline() throws Exception {
    var cache = cache();
    var logical = new CompletableFuture<RelayAuthorizationCache.Snapshot>();
    var physical = new CompletableFuture<Void>();
    var first =
        cache.refreshTracked(
            call,
            sender,
            1,
            1,
            Duration.ofSeconds(1),
            () -> new io.webrtc.signaling.actors.admission.ActorOperation<>(logical, physical));
    var shorter =
        cache.refreshTracked(
            call,
            sender,
            1,
            1,
            Duration.ofMillis(40),
            () -> {
              throw new AssertionError("Single flight required");
            });
    try {
      shorter.logical().toCompletableFuture().get(250, TimeUnit.MILLISECONDS);
      fail("Expected shorter original deadline");
    } catch (ExecutionException e) {
      assertThat(e).hasCauseInstanceOf(TimeoutException.class);
    }
    assertThat(first.logical().toCompletableFuture()).isNotDone();
    logical.completeExceptionally(new TimeoutException("TEST_ONLY_ORIGINAL"));
    physical.complete(null);
  }

  @Test
  void knownInvalidationCannotRepopulateAuthorityFromAnOlderPendingRead() {
    var cache = cache();
    var logical = new CompletableFuture<RelayAuthorizationCache.Snapshot>();
    var physical = new CompletableFuture<Void>();
    var work =
        cache.refreshTracked(
            call,
            sender,
            1,
            1,
            Duration.ofSeconds(1),
            () -> new io.webrtc.signaling.actors.admission.ActorOperation<>(logical, physical));
    cache.invalidate(call);
    logical.complete(snapshot(30000000000L, 30000000000L, 30000000000L));
    assertThat(work.logical().toCompletableFuture()).isCompletedExceptionally();
    assertThat(cache.get(call, sender, 1, 1)).isEmpty();
    assertThat(work.physicalCompletion().toCompletableFuture()).isNotDone();
    physical.complete(null);
    assertThat(work.physicalCompletion().toCompletableFuture()).isDone();
  }

  @Test
  void observedClockLossClosesCoalescedAdmissionAndInvalidatesItsPendingProof() {
    var cache = cache();
    var logical = new CompletableFuture<RelayAuthorizationCache.Snapshot>();
    var physical = new CompletableFuture<Void>();
    var first =
        cache.refreshTracked(
            call,
            sender,
            1,
            1,
            Duration.ofSeconds(1),
            () -> new io.webrtc.signaling.actors.admission.ActorOperation<>(logical, physical));
    trusted.set(false);
    var denied =
        cache.refreshTracked(
            call,
            sender,
            1,
            1,
            Duration.ofSeconds(1),
            () -> {
              throw new AssertionError("No new native read");
            });
    assertThat(denied.logical().toCompletableFuture()).isCompletedExceptionally();
    trusted.set(true);
    logical.complete(snapshot(30000000000L, 30000000000L, 30000000000L));
    assertThat(first.logical().toCompletableFuture()).isCompletedExceptionally();
    assertThat(cache.get(call, sender, 1, 1)).isEmpty();
    physical.complete(null);
    assertThat(cache.pending()).isZero();
  }

  @Test
  void simultaneousNativeCompletionsCannotDeadlockConsumersReadingAnotherRelayCache()
      throws Exception {
    var a = cache();
    var b = cache();
    var sourceA = new CompletableFuture<RelayAuthorizationCache.Snapshot>();
    var sourceB = new CompletableFuture<RelayAuthorizationCache.Snapshot>();
    var barrier = new CountDownLatch(2);
    var returnedA = new CompletableFuture<Void>();
    var returnedB = new CompletableFuture<Void>();
    var workA =
        a.refreshTracked(
            call,
            sender,
            1,
            1,
            Duration.ofSeconds(2),
            () ->
                new io.webrtc.signaling.actors.admission.ActorOperation<>(
                    sourceA, CompletableFuture.completedFuture(null)));
    var workB =
        b.refreshTracked(
            call,
            sender,
            1,
            1,
            Duration.ofSeconds(2),
            () ->
                new io.webrtc.signaling.actors.admission.ActorOperation<>(
                    sourceB, CompletableFuture.completedFuture(null)));
    java.util.function.Function<RelayAuthorizationCache, Optional<RelayAuthorizationCache.Snapshot>>
        lookup =
            other -> {
              barrier.countDown();
              try {
                if (!barrier.await(1, TimeUnit.SECONDS))
                  throw new IllegalStateException("TEST_ONLY_BARRIER_TIMEOUT");
              } catch (InterruptedException interrupted) {
                throw new IllegalStateException(interrupted);
              }
              return other.get(call, sender, 1, 1);
            };
    var crossA = workA.logical().thenApply(v -> lookup.apply(b));
    var crossB = workB.logical().thenApply(v -> lookup.apply(a));
    Thread.ofPlatform()
        .daemon()
        .name("TEST_ONLY_NATIVE_REFRESH_A")
        .start(
            () -> {
              sourceA.complete(snapshot(30000000000L, 30000000000L, 30000000000L));
              returnedA.complete(null);
            });
    Thread.ofPlatform()
        .daemon()
        .name("TEST_ONLY_NATIVE_REFRESH_B")
        .start(
            () -> {
              sourceB.complete(snapshot(30000000000L, 30000000000L, 30000000000L));
              returnedB.complete(null);
            });
    CompletableFuture.allOf(returnedA, returnedB).get(1500, TimeUnit.MILLISECONDS);
    assertThat(crossA.toCompletableFuture().join()).isPresent();
    assertThat(crossB.toCompletableFuture().join()).isPresent();
    workA.physicalCompletion().toCompletableFuture().get(1, TimeUnit.SECONDS);
    workB.physicalCompletion().toCompletableFuture().get(1, TimeUnit.SECONDS);
  }

  @Test
  void expiredSuccessWithUnknownOriginalCleanupCannotAuthorizeAnotherFrame() {
    var cache = cache();
    var cleanup = new CompletableFuture<Void>();
    var original =
        cache.refreshTracked(
            call,
            sender,
            1,
            1,
            Duration.ofSeconds(1),
            () ->
                new io.webrtc.signaling.actors.admission.ActorOperation<>(
                    CompletableFuture.completedFuture(
                        snapshot(3000000000L, 30000000000L, 30000000000L)),
                    cleanup));
    assertThat(original.logical().toCompletableFuture()).isDone();
    assertThat(original.physicalCompletion().toCompletableFuture()).isNotDone();
    now.set(4000000000L);
    var late =
        cache.refreshTracked(
            call,
            sender,
            1,
            1,
            Duration.ofSeconds(1),
            () -> {
              throw new AssertionError("Original cleanup still owns the refresh slot");
            });
    assertThat(late.logical().toCompletableFuture()).isCompletedExceptionally();
    assertThat(late.physicalCompletion().toCompletableFuture()).isNotDone();
    cleanup.complete(null);
    assertThat(late.physicalCompletion().toCompletableFuture()).isDone();
    assertThat(cache.pending()).isZero();
  }
}
