package io.webrtc.signaling.storage;

import java.time.Duration;

/**
 * Call workflow timers (spec 8.1, 8.3, 11, 25.2, 49.4, 49.8). Values are validated against the v1
 * contract ceilings so a configuration can shorten, never extend, an authority window. The record's
 * string form feeds the configuration fingerprint, so mixed fleets with different timers differ.
 */
public record CallTimers(
    Duration ringing,
    Duration activation,
    Duration negotiation,
    Duration reconnectGrace,
    Duration mediaRecoveryWindow,
    Duration mediaDisconnectDebounce,
    int mediaRestartAttempts) {
  public static final CallTimers V1 =
      new CallTimers(
          Duration.ofSeconds(30),
          Duration.ofSeconds(5),
          Duration.ofSeconds(20),
          Duration.ofSeconds(75),
          Duration.ofSeconds(30),
          Duration.ofSeconds(5),
          1);

  public CallTimers {
    within(ringing, Duration.ofSeconds(30), "ringing");
    within(activation, Duration.ofSeconds(5), "activation");
    within(negotiation, Duration.ofSeconds(20), "negotiation");
    within(reconnectGrace, Duration.ofSeconds(75), "reconnect grace");
    within(mediaRecoveryWindow, reconnectGrace, "media recovery window");
    within(mediaDisconnectDebounce, mediaRecoveryWindow, "media debounce");
    if (mediaDisconnectDebounce.compareTo(mediaRecoveryWindow) >= 0)
      throw new IllegalArgumentException("media debounce must precede the recovery window");
    if (mediaRestartAttempts < 1 || mediaRestartAttempts > 3)
      throw new IllegalArgumentException("media restart attempts must be 1..3");
  }

  private static void within(Duration value, Duration ceiling, String name) {
    if (value == null || value.compareTo(Duration.ofSeconds(1)) < 0 || value.compareTo(ceiling) > 0)
      throw new IllegalArgumentException(name + " must be 1s.." + ceiling);
  }

  public long ringingSeconds() {
    return ringing.toSeconds();
  }

  public long activationSeconds() {
    return activation.toSeconds();
  }

  public long negotiationSeconds() {
    return negotiation.toSeconds();
  }

  public long reconnectGraceSeconds() {
    return reconnectGrace.toSeconds();
  }

  public long mediaRecoverySeconds() {
    return mediaRecoveryWindow.toSeconds();
  }

  public long mediaDebounceSeconds() {
    return mediaDisconnectDebounce.toSeconds();
  }
}
