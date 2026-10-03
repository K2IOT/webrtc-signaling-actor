package io.webrtc.signaling.auth;
import io.webrtc.signaling.protocol.Identity.UserId;
import java.time.Instant;
public record CallAuthorizationRequest(AuthPrincipal caller,UserId target,Instant now) {}
