package io.webrtc.signaling.auth;
import java.time.Instant;
public record AuthorizationDecision(boolean allowed,String policyVersion,Instant expiresAt){public static AuthorizationDecision denied(){return new AuthorizationDecision(false,"fail-closed",Instant.EPOCH);}}
