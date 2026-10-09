package io.webrtc.signaling.actors.relay;

import static org.assertj.core.api.Assertions.*;

import io.webrtc.signaling.actors.admission.ActorOperation;
import io.webrtc.signaling.protocol.Identity.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import org.junit.jupiter.api.Test;

class RelayLifecycleTest {
  @Test
  void trafficFromBothParticipantsUsesIndependentAuthorizationSnapshots() {
    var fixture = new NegotiationRelayTest();
    var cache = fixture.cacheForTest();
    cache.put(fixture.snapshot(fixture.caller, 1, 1));
    cache.put(fixture.snapshot(fixture.callee, 1, 1));
    assertThat(cache.get(fixture.call, fixture.caller, 1, 1)).isPresent();
    assertThat(cache.get(fixture.call, fixture.callee, 1, 1)).isPresent();
  }

  @Test
  void logicalUnknownDoesNotPermitRetryOrReleaseRetainedBodyBeforePhysicalCleanup() {
    var fixture = new NegotiationRelayTest();
    var logical = new CompletableFuture<Void>();
    var physical = new CompletableFuture<Void>();
    var relay =
        new NegotiationRelay(
            4,
            fixture.now::get,
            fixture.memory,
            fixture.cacheForTest(),
            (c, s, r, i, b) ->
                new ActorOperation<>(
                    CompletableFuture.completedFuture(fixture.snapshot(s, r, i)),
                    CompletableFuture.completedFuture(null)),
            message -> {
              fixture.writes.incrementAndGet();
              return new ActorOperation<>(logical, physical);
            });
    relay.install(fixture.grant(1, 1));
    var offer = fixture.offer(UUID.randomUUID(), "private SDP", 1, 1);
    var first = relay.send(offer, Duration.ofSeconds(1));
    logical.completeExceptionally(new IllegalStateException("UNKNOWN"));
    relay.send(offer, Duration.ofSeconds(1));
    assertThat(fixture.writes).hasValue(1);
    relay.close();
    assertThat(fixture.memory.retainedBytes()).isPositive();
    physical.complete(null);
    assertThat(fixture.memory.retainedBytes()).isZero();
    assertThat(first.toCompletableFuture()).isCompletedExceptionally();
  }

  @Test
  void malformedUtf16CannotAliasDistinctCandidateHashes() {
    assertThatThrownBy(() -> new IceReceiveWindow.Item(1, "\ud800", "0", 0, "u", false))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void idleDescriptionIsPhysicallyEvictedAtTheOriginalNativeDeadline() throws Exception {
    var fixture = new NegotiationRelayTest();
    var relay = fixture.relay();
    var grant =
        new NegotiationRelay.Grant(
            fixture.call,
            fixture.activation,
            3,
            1,
            1,
            fixture.caller,
            fixture.callee,
            100000000L,
            fixture.group);
    relay.install(grant);
    relay
        .send(fixture.offer(UUID.randomUUID(), "private SDP", 1, 1), Duration.ofSeconds(1))
        .toCompletableFuture()
        .join();
    fixture.now.set(100000000L);
    CompletableFuture.runAsync(
            () -> {
              while (fixture.memory.retainedBytes() != 0) {
                try {
                  Thread.sleep(10);
                } catch (InterruptedException e) {
                  throw new RuntimeException(e);
                }
              }
            })
        .get(1, TimeUnit.SECONDS);
    relay.close();
  }

  @Test
  void aggregateMemoryAlsoBoundsTinyFramesByTicketCount() {
    var budget = new RelayBufferBudget(1048576, 2);
    try (var a = budget.acquire(1);
        var b = budget.acquire(1)) {
      assertThatThrownBy(() -> budget.acquire(1)).isInstanceOf(RelayBufferBudget.Overloaded.class);
    }
    assertThat(budget.retainedBytes()).isZero();
  }

  @Test
  void committedActivationAndVersionMustMatchTheNativeRelayProjection() {
    var fixture = new NegotiationRelayTest();
    var relay = fixture.relay();
    relay.install(
        new NegotiationRelay.Grant(
            fixture.call,
            UUID.randomUUID(),
            3,
            1,
            1,
            fixture.caller,
            fixture.callee,
            20000000000L,
            fixture.group));
    assertThatThrownBy(
            () ->
                relay
                    .send(fixture.offer(UUID.randomUUID(), "private", 1, 1), Duration.ofSeconds(1))
                    .toCompletableFuture()
                    .join())
        .hasCauseInstanceOf(IllegalStateException.class);
    assertThat(fixture.writes).hasValue(0);
    relay.close();
  }
}
