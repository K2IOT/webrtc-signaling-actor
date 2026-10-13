package io.webrtc.signaling.rpc;

import io.webrtc.signaling.protocol.Identity.CallId;
import io.webrtc.signaling.storage.SessionRepository;

import java.util.Objects;
import java.util.UUID;

/**
 * Authenticated application receipt, bound to the route issued for this exact event.
 */
public record ControlApplicationReceipt(
        UUID eventId, CallId call, long callVersion, SessionRepository.Route route) {
    public ControlApplicationReceipt {
        Objects.requireNonNull(eventId);
        Objects.requireNonNull(call);
        Objects.requireNonNull(route);
        if (callVersion < 1) throw new IllegalArgumentException("Invalid control version");
    }
}
