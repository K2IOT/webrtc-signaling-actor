package io.webrtc.signaling.auth;

import static org.assertj.core.api.Assertions.*;

import java.security.*;
import java.security.interfaces.EdECPublicKey;
import java.security.spec.*;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/** Real JCA signatures; a delegated public key pauses only its original verification. */
class ClockSafetyMonitorConcurrencyTest {
  enum Action {
    CACHE_READ,
    INVALIDATE,
    NEWER_DISCONTINUITY
  }

  @ParameterizedTest
  @EnumSource(Action.class)
  void pendingSignatureCannotBlockCachedSafetyOrOverwriteNewerLoss(Action action) throws Exception {
    var pair = KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
    var key = new HeldKey((EdECPublicKey) pair.getPublic());
    var time = new ClockSafetyMonitorTest.Time();
    var monitor =
        new ClockSafetyMonitor(
            "c001",
            1,
            ClockSafetyMonitorTest.POD,
            ClockSafetyMonitorTest.BOOT,
            Map.of("TEST_ONLY_TIME", key),
            time,
            time.elapsed::get);
    var first = ClockSafetyMonitorTest.report(1, time.wall, 200000, 1000, true);
    assertThat(monitor.observe(first, ClockSafetyMonitorTest.sign(first, pair), 0)).isTrue();
    key.armed.set(true);
    try (var tasks = Executors.newVirtualThreadPerTaskExecutor()) {
      var next = ClockSafetyMonitorTest.report(2, time.wall, 200000, 1000, true);
      var signed = ClockSafetyMonitorTest.sign(next, pair);
      var original = tasks.submit(() -> monitor.observe(next, signed, 0));
      try {
        assertThat(key.entered.await(1, TimeUnit.SECONDS)).isTrue();
        switch (action) {
          case CACHE_READ ->
              assertThat(tasks.submit(monitor::valid))
                  .succeedsWithin(Duration.ofMillis(150))
                  .isEqualTo(true);
          case INVALIDATE ->
              assertThat(
                      tasks.submit(
                          () -> {
                            monitor.invalidate();
                            return true;
                          }))
                  .succeedsWithin(Duration.ofMillis(150))
                  .isEqualTo(true);
          case NEWER_DISCONTINUITY -> {
            var broken = ClockSafetyMonitorTest.report(3, time.wall, 200000, 1000, false);
            var signature = ClockSafetyMonitorTest.sign(broken, pair);
            assertThat(tasks.submit(() -> monitor.observe(broken, signature, 0)))
                .succeedsWithin(Duration.ofMillis(150))
                .isEqualTo(false);
          }
        }
        key.release.countDown();
        assertThat(original.get(1, TimeUnit.SECONDS)).isEqualTo(action == Action.CACHE_READ);
        assertThat(monitor.valid()).isEqualTo(action == Action.CACHE_READ);
        if (action != Action.CACHE_READ) {
          var recovered = ClockSafetyMonitorTest.report(4, time.wall, 200000, 1000, true);
          assertThat(monitor.observe(recovered, ClockSafetyMonitorTest.sign(recovered, pair), 0))
              .isTrue();
          assertThat(monitor.observe(next, signed, 0)).isFalse();
          assertThat(monitor.valid()).isTrue();
        }
      } finally {
        key.release.countDown();
      }
    }
  }

  static final class HeldKey implements EdECPublicKey {
    final EdECPublicKey delegate;
    final AtomicBoolean armed = new AtomicBoolean(), held = new AtomicBoolean();
    final CountDownLatch entered = new CountDownLatch(1), release = new CountDownLatch(1);

    HeldKey(EdECPublicKey delegate) {
      this.delegate = delegate;
    }

    public NamedParameterSpec getParams() {
      return delegate.getParams();
    }

    public EdECPoint getPoint() {
      if (armed.get() && held.compareAndSet(false, true)) {
        entered.countDown();
        try {
          release.await();
        } catch (InterruptedException interrupted) {
          Thread.currentThread().interrupt();
          throw new IllegalStateException("TEST_ONLY_INTERRUPTED");
        }
      }
      return delegate.getPoint();
    }

    public String getAlgorithm() {
      return delegate.getAlgorithm();
    }

    public String getFormat() {
      return delegate.getFormat();
    }

    public byte[] getEncoded() {
      return delegate.getEncoded();
    }
  }
}
