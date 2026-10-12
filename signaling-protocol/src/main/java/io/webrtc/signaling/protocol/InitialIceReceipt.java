package io.webrtc.signaling.protocol;

import com.fasterxml.jackson.databind.ObjectMapper;

/** Bounded feature classification after strict native protocol binding. */
public final class InitialIceReceipt {
  private static final ObjectMapper JSON = new ObjectMapper();

  private InitialIceReceipt() {}

  public static boolean requiresCapability(CallCommand command) {
    if (command.type() == SignalEnvelope.Type.INITIAL_ICE_RECEIPT) return true;
    if (command.type() != SignalEnvelope.Type.ICE_CANDIDATES
        && command.type() != SignalEnvelope.Type.END_OF_CANDIDATES) return false;
    try {
      if (command.payloadJson().length() > 8192)
        throw new IllegalArgumentException("ICE payload exceeds bound");
      return JSON.readTree(command.payloadJson()).has("receiptProtocol");
    } catch (java.io.IOException invalid) {
      throw new IllegalArgumentException("Invalid ICE payload");
    }
  }
}
