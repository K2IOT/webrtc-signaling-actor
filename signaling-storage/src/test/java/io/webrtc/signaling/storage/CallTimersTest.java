package io.webrtc.signaling.storage;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import org.junit.jupiter.api.Test;

class CallTimersTest {
  private static CallTimers with(
      long ringing,
      long activation,
      long negotiation,
      long grace,
      long window,
      long debounce,
      int attempts) {
    return new CallTimers(
        Duration.ofSeconds(ringing),
        Duration.ofSeconds(activation),
        Duration.ofSeconds(negotiation),
        Duration.ofSeconds(grace),
        Duration.ofSeconds(window),
        Duration.ofSeconds(debounce),
        attempts);
  }

  @Test
  void v1MatchesTheSpecificationContract() {
    assertThat(CallTimers.V1.ringingSeconds()).isEqualTo(30);
    assertThat(CallTimers.V1.activationSeconds()).isEqualTo(5);
    assertThat(CallTimers.V1.negotiationSeconds()).isEqualTo(20);
    assertThat(CallTimers.V1.reconnectGraceSeconds()).isEqualTo(75);
    assertThat(CallTimers.V1.mediaRecoverySeconds()).isEqualTo(30);
    assertThat(CallTimers.V1.mediaDebounceSeconds()).isEqualTo(5);
    assertThat(CallTimers.V1.mediaRestartAttempts()).isEqualTo(1);
  }

  @Test
  void shorterWindowsAreAccepted() {
    var shorter = with(20, 3, 10, 60, 20, 2, 2);
    assertThat(shorter).isNotEqualTo(CallTimers.V1);
    assertThat(shorter.toString()).isNotEqualTo(CallTimers.V1.toString());
  }

  @Test
  void authorityWindowsCannotBeExtendedBeyondTheContract() {
    assertThatThrownBy(() -> with(31, 5, 20, 75, 30, 5, 1))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> with(30, 6, 20, 75, 30, 5, 1))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> with(30, 5, 21, 75, 30, 5, 1))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> with(30, 5, 20, 76, 30, 5, 1))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void mediaPolicyMustBeInternallyConsistent() {
    assertThatThrownBy(() -> with(30, 5, 20, 75, 30, 30, 1))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> with(30, 5, 20, 30, 31, 5, 1))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> with(30, 5, 20, 75, 30, 5, 0))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> with(30, 5, 20, 75, 30, 5, 4))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void zeroNegativeAndMissingDurationsAreRejected() {
    assertThatThrownBy(() -> with(0, 5, 20, 75, 30, 5, 1))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> with(30, -1, 20, 75, 30, 5, 1))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(
            () ->
                new CallTimers(
                    null,
                    Duration.ofSeconds(5),
                    Duration.ofSeconds(20),
                    Duration.ofSeconds(75),
                    Duration.ofSeconds(30),
                    Duration.ofSeconds(5),
                    1))
        .isInstanceOf(IllegalArgumentException.class);
  }
}
