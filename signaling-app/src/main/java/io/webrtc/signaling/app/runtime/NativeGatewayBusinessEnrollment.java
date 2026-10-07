package io.webrtc.signaling.app.runtime;

import io.webrtc.signaling.auth.*;
import io.webrtc.signaling.protocol.Identity.UserId;
import io.webrtc.signaling.rpc.*;
import java.time.Instant;
import java.util.Objects;
import java.util.function.BiFunction;
import java.util.function.Function;

/** Explicit gateway identity, verified directory and cached security dependencies. */
public record NativeGatewayBusinessEnrollment(NativeSessionHandler.GatewayIdentity identity, long routingEpoch,
        BoundedTokenVerifier tokens, BiFunction<AuthPrincipal,Instant,AuthorizationStatus> cachedSecurity,
        Function<UserId,ProofBindings.TrustedHome> homes, RelaySessionAuthorizationProof relayProofs,
        int relayCacheCapacity, int relayPendingLimit) {
    public NativeGatewayBusinessEnrollment {
        Objects.requireNonNull(identity); Objects.requireNonNull(tokens); Objects.requireNonNull(cachedSecurity);
        Objects.requireNonNull(homes); Objects.requireNonNull(relayProofs);
        if(routingEpoch < 1 || relayCacheCapacity < 1 || relayCacheCapacity > 4096
                || relayPendingLimit < 1 || relayPendingLimit > 64 || relayPendingLimit > relayCacheCapacity)
            throw new IllegalArgumentException("Invalid native gateway enrollment bounds");
    }
}
