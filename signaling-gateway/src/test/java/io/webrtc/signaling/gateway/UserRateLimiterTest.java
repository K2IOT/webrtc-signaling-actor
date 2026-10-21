package io.webrtc.signaling.gateway;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

class UserRateLimiterTest {
  private final AtomicLong now = new AtomicLong();

  private UserRateLimiter limiter(double rate, double burst, int tracked) {
    return new UserRateLimiter(
        new GatewayRateLimits(20, 40, 30, 60, rate, burst, tracked), now::get);
  }

  @Test
  void burstIsAdmittedThenRefusedWithPositiveRetryAdvice() {
    var limiter = limiter(10, 3, 8);
    for (int i = 0; i < 3; i++) assertThat(limiter.tryAcquire("u")).isZero();
    assertThat(limiter.tryAcquire("u")).isBetween(1L, 100L);
  }

  @Test
  void tokensRefillAtTheConfiguredRateAndNeverExceedTheBurst() {
    var limiter = limiter(10, 2, 8);
    assertThat(limiter.tryAcquire("u")).isZero();
    assertThat(limiter.tryAcquire("u")).isZero();
    assertThat(limiter.tryAcquire("u")).isPositive();
    now.addAndGet(TimeUnit.MILLISECONDS.toNanos(100));
    assertThat(limiter.tryAcquire("u")).isZero();
    assertThat(limiter.tryAcquire("u")).isPositive();
    now.addAndGet(TimeUnit.SECONDS.toNanos(60));
    assertThat(limiter.tryAcquire("u")).isZero();
    assertThat(limiter.tryAcquire("u")).isZero();
    assertThat(limiter.tryAcquire("u")).isPositive();
  }

  @Test
  void usersAreIsolated() {
    var limiter = limiter(1, 1, 8);
    assertThat(limiter.tryAcquire("a")).isZero();
    assertThat(limiter.tryAcquire("a")).isPositive();
    assertThat(limiter.tryAcquire("b")).isZero();
  }

  @Test
  void trackedUsersAreBoundedByLeastRecentUse() {
    var limiter = limiter(1, 1, 2);
    limiter.tryAcquire("a");
    limiter.tryAcquire("b");
    limiter.tryAcquire("c");
    assertThat(limiter.tracked()).isEqualTo(2);
    // "a" was evicted, so it starts with a fresh burst instead of being refused.
    assertThat(limiter.tryAcquire("a")).isZero();
  }

  @Test
  void invalidLimitsFailFast() {
    assertThatThrownBy(() -> new GatewayRateLimits(0, 40, 30, 60, 40, 80, 8))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> new GatewayRateLimits(40, 40, 30, 60, 40, 80, 8))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> new GatewayRateLimits(20, 40, 30, 60, 40, 80, 0))
        .isInstanceOf(IllegalArgumentException.class);
    assertThat(GatewayRateLimits.V1.userRate()).isEqualTo(40);
  }
}
