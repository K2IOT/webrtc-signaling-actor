package io.webrtc.signaling.actors.cluster;

import io.webrtc.signaling.actors.admission.*;
import java.util.function.*;

/**
 * Validate and charge the complete versioned message, including reply and physical receipt refs,
 * before tell.
 */
public final class NativeEnvelopeAdmission {
  private NativeEnvelopeAdmission() {}

  public static <M> void send(
      EntityAdmission.Ticket ticket,
      IntFunction<M> factory,
      Consumer<M> tell,
      ApplicationSerializer serializer) {
    M message;
    try {
      // The maximum five-digit encodedBytes value conservatively covers its own encoded field.
      int bytes = serializer.toBinary(factory.apply(98304)).length;
      ticket.charge(bytes);
      message = factory.apply(bytes);
      if (serializer.toBinary(message).length > bytes)
        throw new IllegalArgumentException("Unstable envelope factory");
    } catch (RuntimeException rejected) {
      throw new TrackedEntityAsk.NotEnqueued(rejected);
    }
    // A tell failure can have an unknown dispatch outcome. Never label it NOT_STARTED.
    tell.accept(message);
  }
}
