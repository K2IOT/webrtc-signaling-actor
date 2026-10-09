package io.webrtc.signaling.protocol;

import io.webrtc.signaling.protocol.Identity.CallId;
import java.util.Objects;

/**
 * Bounded observations; never an ownership or peer-delivery proof. No SDP/IP/device identifiers.
 */
public record MediaTelemetry(
    Event event,
    CallId callId,
    long negotiationId,
    long iceGeneration,
    long senderSequence,
    Quality quality) {
  public enum Event {
    MEDIA_CONNECTED,
    MEDIA_DISCONNECTED,
    ICE_RESTARTING,
    MEDIA_FAILED,
    MEDIA_RECOVERED
  }

  public record Quality(long rttMillis, long jitterMicros, long packetsLost, long framesDropped) {
    public Quality {
      if (rttMillis < 0
          || rttMillis > 120000
          || jitterMicros < 0
          || jitterMicros > 120000000
          || packetsLost < 0
          || packetsLost > 1000000000L
          || framesDropped < 0
          || framesDropped > 1000000000L)
        throw new IllegalArgumentException("Invalid bounded QoE observation");
    }
  }

  public MediaTelemetry {
    Objects.requireNonNull(event);
    Objects.requireNonNull(callId);
    if (negotiationId < 1 || iceGeneration < 1 || senderSequence < 1)
      throw new IllegalArgumentException("Invalid media observation scope");
  }
}
