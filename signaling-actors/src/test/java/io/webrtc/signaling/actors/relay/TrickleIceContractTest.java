package io.webrtc.signaling.actors.relay;

import static org.assertj.core.api.Assertions.*;

import io.webrtc.signaling.protocol.Identity.*;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import org.junit.jupiter.api.Test;

class TrickleIceContractTest {
  final AtomicLong now = new AtomicLong();
  final List<Long> calls = new ArrayList<>();
  final IceReceiveWindow.Key key =
      new IceReceiveWindow.Key(
          new CallId("c001.e1.00000000-0000-0000-0000-000000000001"), 1, 1, UUID.randomUUID());

  IceReceiveWindow window() {
    return new IceReceiveWindow(
        key,
        256,
        65536,
        Duration.ofSeconds(10),
        now::get,
        item -> {
          calls.add(item.sequence());
          return CompletableFuture.completedFuture(null);
        });
  }

  static IceReceiveWindow.Item candidate(long sequence) {
    return new IceReceiveWindow.Item(
        sequence, "candidate:TEST_ONLY " + sequence, "0", 0, "TEST_ONLY_UFRAG", false);
  }

  static IceReceiveWindow.Item end(long sequence) {
    return new IceReceiveWindow.Item(sequence, null, null, null, "TEST_ONLY_UFRAG", true);
  }

  @Test
  void duplicateBatchesReachIceBoundaryExactlyOnce() {
    var w = window();
    w.remoteDescriptionReady("TEST_ONLY_UFRAG");
    assertThat(w.accept(key, List.of(candidate(1), candidate(2))).highestContiguous()).isEqualTo(2);
    w.accept(key, List.of(candidate(1), candidate(2)));
    assertThat(calls).containsExactly(1L, 2L);
  }

  @Test
  void aMissingMiddleBlocksLaterCandidatesAndReportsBoundedRanges() {
    var w = window();
    w.remoteDescriptionReady("TEST_ONLY_UFRAG");
    w.accept(key, List.of(candidate(1)));
    var gap = w.accept(key, List.of(candidate(3)));
    assertThat(gap.highestContiguous()).isEqualTo(1);
    assertThat(gap.missing()).containsExactly(new IceReceiveWindow.Range(2, 2));
    assertThat(calls).containsExactly(1L);
    w.accept(key, List.of(candidate(2)));
    assertThat(calls).containsExactly(1L, 2L, 3L);
  }

  @Test
  void anEarlyEndMarkerWaitsForDelayedCandidateAndIsIdempotent() {
    var w = window();
    w.remoteDescriptionReady("TEST_ONLY_UFRAG");
    w.accept(key, List.of(candidate(1)));
    w.accept(key, List.of(end(3)));
    assertThat(calls).containsExactly(1L);
    w.accept(key, List.of(candidate(2)));
    w.accept(key, List.of(end(3)));
    w.accept(key, List.of(candidate(4)));
    assertThat(calls).containsExactly(1L, 2L, 3L);
    assertThat(w.ended()).isTrue();
  }

  @Test
  void candidatesBeforeRemoteDescriptionRemainBoundedAndOrdered() {
    var w = window();
    w.accept(key, List.of(candidate(1), candidate(2), end(3)));
    assertThat(calls).isEmpty();
    w.remoteDescriptionReady("TEST_ONLY_UFRAG");
    assertThat(calls).containsExactly(1L, 2L, 3L);
  }

  @Test
  void reconnectDuringGapDoesNotResetTheOriginalTenSecondDeadline() {
    var w = window();
    w.remoteDescriptionReady("TEST_ONLY_UFRAG");
    w.accept(key, List.of(candidate(2)));
    now.addAndGet(Duration.ofSeconds(9).toNanos());
    w.reconnected();
    now.addAndGet(Duration.ofSeconds(2).toNanos());
    assertThat(w.tick().code()).isEqualTo("RESYNC_REQUIRED");
    w.accept(key, List.of(candidate(1)));
    assertThat(calls).isEmpty();
  }

  @Test
  void oldGenerationReplayCannotReachIceBoundary() {
    var w = window();
    w.remoteDescriptionReady("TEST_ONLY_UFRAG");
    var old = new IceReceiveWindow.Key(key.call(), 1, 2, key.senderIncarnation());
    assertThat(w.accept(old, List.of(candidate(1))).code()).isEqualTo("STALE_GENERATION");
    assertThat(calls).isEmpty();
  }

  @Test
  void gatewayRetryAfterUnknownIceApplicationNeverCallsBoundaryTwice() {
    var applied = new CompletableFuture<Void>();
    var w =
        new IceReceiveWindow(
            key,
            256,
            65536,
            Duration.ofSeconds(10),
            now::get,
            item -> {
              calls.add(item.sequence());
              return applied;
            });
    w.remoteDescriptionReady("TEST_ONLY_UFRAG");
    w.accept(key, List.of(candidate(1)));
    w.accept(key, List.of(candidate(1)));
    assertThat(calls).containsExactly(1L);
    assertThat(w.highestContiguous()).isZero();
    applied.complete(null);
    assertThat(w.highestContiguous()).isEqualTo(1);
  }

  @Test
  void mutatedDuplicatesAndOversizedBatchesRequireResync() {
    var w = window();
    w.remoteDescriptionReady("TEST_ONLY_UFRAG");
    w.accept(key, List.of(candidate(1)));
    assertThat(
            w.accept(
                    key,
                    List.of(
                        new IceReceiveWindow.Item(
                            1, "different", "0", 0, "TEST_ONLY_UFRAG", false)))
                .code())
        .isEqualTo("RESYNC_REQUIRED");
    var bounded = window();
    assertThat(
            bounded
                .accept(
                    key,
                    java.util.stream.LongStream.rangeClosed(1, 21)
                        .mapToObj(TrickleIceContractTest::candidate)
                        .toList())
                .code())
        .isEqualTo("RESYNC_REQUIRED");
    assertThat(bounded.retainedBytes()).isZero();
  }

  @Test
  void usernameFragmentMismatchCannotMixRestartCredentials() {
    var w = window();
    w.accept(
        key,
        List.of(new IceReceiveWindow.Item(1, "candidate:TEST_ONLY", "0", 0, "OLD_UFRAG", false)));
    w.remoteDescriptionReady("TEST_ONLY_UFRAG");
    assertThat(calls).isEmpty();
    assertThat(w.tick().code()).isEqualTo("RESYNC_REQUIRED");
  }

  @Test
  void crossCellReorderingAndDuplicatesPreserveExactlyOneOrderedIceCallPerSequence() {
    var w = window();
    w.remoteDescriptionReady("TEST_ONLY_UFRAG");
    for (long sequence : new long[] {4, 2, 4, 3, 2, 1}) w.accept(key, List.of(candidate(sequence)));
    w.accept(key, List.of(end(5)));
    assertThat(calls).containsExactly(1L, 2L, 3L, 4L, 5L);
    assertThat(w.retainedBytes()).isZero();
  }

  @Test
  void candidateHashEncodingCannotConfuseFieldBoundaries() {
    var w = window();
    w.remoteDescriptionReady("TEST_ONLY_UFRAG");
    w.accept(
        key,
        List.of(new IceReceiveWindow.Item(1, "candidate:x|y", "z", 0, "TEST_ONLY_UFRAG", false)));
    assertThat(
            w.accept(
                    key,
                    List.of(
                        new IceReceiveWindow.Item(
                            1, "candidate:x", "y|z", 0, "TEST_ONLY_UFRAG", false)))
                .code())
        .isEqualTo("RESYNC_REQUIRED");
    assertThat(calls).containsExactly(1L);
  }

  @Test
  void idleGapEvictsItsPayloadWithoutRequiringFurtherTraffic() throws Exception {
    var memory = new RelayBufferBudget(65536);
    var w =
        new IceReceiveWindow(
            key,
            256,
            65536,
            Duration.ofMillis(50),
            now::get,
            item -> CompletableFuture.completedFuture(null),
            memory);
    w.accept(key, List.of(candidate(2)));
    now.set(50000000L);
    CompletableFuture.runAsync(
            () -> {
              while (memory.retainedBytes() != 0) {
                try {
                  Thread.sleep(10);
                } catch (InterruptedException e) {
                  throw new RuntimeException(e);
                }
              }
            })
        .get(1, TimeUnit.SECONDS);
    assertThat(w.tick().code()).isEqualTo("RESYNC_REQUIRED");
    w.close();
  }
}
