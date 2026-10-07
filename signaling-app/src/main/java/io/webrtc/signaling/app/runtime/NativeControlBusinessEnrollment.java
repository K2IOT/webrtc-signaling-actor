package io.webrtc.signaling.app.runtime;

import io.webrtc.signaling.auth.*;
import io.webrtc.signaling.control.PostgresDirectoryRepository;
import java.time.*;
import java.util.Objects;
import java.util.function.BiFunction;
import java.util.function.BooleanSupplier;

/** Cached security must include issuer/user/jti/signing-key status from the enrolled source. */
public record NativeControlBusinessEnrollment(PostgresDirectoryRepository directory,BoundedTokenVerifier tokens,
        BiFunction<AuthPrincipal,Instant,AuthorizationStatus> cachedSecurity,BooleanSupplier securityFresh,ClockSafetyMonitor clock,Duration directoryRefresh) {
    public NativeControlBusinessEnrollment {
        Objects.requireNonNull(directory);Objects.requireNonNull(tokens);Objects.requireNonNull(cachedSecurity);Objects.requireNonNull(clock);
        Objects.requireNonNull(securityFresh);
        if(directoryRefresh==null||directoryRefresh.isZero()||directoryRefresh.isNegative()||directoryRefresh.compareTo(Duration.ofSeconds(30))>0)
            throw new IllegalArgumentException("Control directory refresh outside bound");
    }
}
