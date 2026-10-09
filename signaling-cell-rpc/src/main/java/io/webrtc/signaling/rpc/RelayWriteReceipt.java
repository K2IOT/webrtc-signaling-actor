package io.webrtc.signaling.rpc;

import io.webrtc.signaling.protocol.*;
import io.webrtc.signaling.protocol.Identity.*;
import java.util.*;

/** Volatile transport write result; never a COMMIT or an ICE-agent application acknowledgment. */
public record RelayWriteReceipt(
    CallId call,
    RequestId request,
    SignalEnvelope.Type type,
    long callVersion,
    long negotiationId,
    long iceGeneration) {
  public RelayWriteReceipt {
    Objects.requireNonNull(call);
    Objects.requireNonNull(request);
    Objects.requireNonNull(type);
    if (!Set.of(
                SignalEnvelope.Type.OFFER,
                SignalEnvelope.Type.ANSWER,
                SignalEnvelope.Type.ICE_CANDIDATES,
                SignalEnvelope.Type.END_OF_CANDIDATES)
            .contains(type)
        || callVersion < 1
        || negotiationId < 1
        || iceGeneration < 1) throw new IllegalArgumentException("Invalid volatile write receipt");
  }

  public boolean matches(CallCommand command) {
    return call.equals(command.callId())
        && request.equals(command.requestId())
        && type == command.type()
        && command.negotiationId() != null
        && negotiationId == command.negotiationId().value()
        && command.iceGeneration() != null
        && iceGeneration == command.iceGeneration().value();
  }
}
