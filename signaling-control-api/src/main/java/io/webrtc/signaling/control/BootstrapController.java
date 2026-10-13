package io.webrtc.signaling.control;

import io.webrtc.signaling.auth.*;
import io.webrtc.signaling.observability.OperationObservation;
import io.webrtc.signaling.observability.SignalingMetrics.Operation;
import io.webrtc.signaling.observability.SignalingMetrics.Outcome;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

import java.time.Clock;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.BiFunction;
import java.util.function.BooleanSupplier;

@RestController
public final class BootstrapController {
    private final DirectoryService directory;
    private final BoundedTokenVerifier verifier;
    private final BiFunction<AuthPrincipal, Instant, AuthorizationStatus> security;
    private final Clock clock;
    private final BooleanSupplier trustedClock;
    private OperationObservation observations = OperationObservation.NONE;
    public BootstrapController(
            DirectoryService directory,
            BoundedTokenVerifier verifier,
            RevocationState security,
            Clock clock) {
        this(directory, verifier, security::checkRevocation, clock, () -> true);
    }
    public BootstrapController(
            DirectoryService directory,
            BoundedTokenVerifier verifier,
            BiFunction<AuthPrincipal, Instant, AuthorizationStatus> security,
            Clock clock,
            BooleanSupplier trustedClock) {
        this.directory = java.util.Objects.requireNonNull(directory);
        this.verifier = java.util.Objects.requireNonNull(verifier);
        this.security = java.util.Objects.requireNonNull(security);
        this.clock = java.util.Objects.requireNonNull(clock);
        this.trustedClock = java.util.Objects.requireNonNull(trustedClock);
    }

    public BootstrapController observe(OperationObservation observations) {
        this.observations = java.util.Objects.requireNonNull(observations);
        return this;
    }

    @PostMapping("/v1/signaling/bootstrap")
    public Mono<BootstrapResponse> bootstrapHttp(
            @RequestHeader(value = "Authorization", required = false) String authorization) {
        if (authorization == null
                || !authorization.startsWith("Bearer ")
                || !trustedClock.getAsBoolean()) return Mono.error(new AuthException());
        return Mono.fromCompletionStage(
                verifier.verify(authorization.substring(7), clock.instant()).thenCompose(this::bootstrap));
    }

    private boolean allowed(AuthPrincipal principal) {
        try {
            var now = clock.instant();
            return trustedClock.getAsBoolean()
                    && principal.expiresAt().isAfter(now)
                    && security.apply(principal, now) == AuthorizationStatus.ALLOWED
                    && trustedClock.getAsBoolean();
        } catch (RuntimeException unavailable) {
            return false;
        }
    }

    public CompletionStage<BootstrapResponse> bootstrap(AuthPrincipal principal) {
        var receipt = OperationObservation.safeLogicalStart(observations, Operation.BOOTSTRAP);
        if (!allowed(principal)) {
            receipt.logical(Outcome.FORBIDDEN);
            return CompletableFuture.failedFuture(new AuthException());
        }
        var result =
                directory
                        .resolveHome(principal.userId(), clock.instant())
                        .thenApply(
                                route -> {
                                    if (!allowed(principal)) throw new AuthException();
                                    return new BootstrapResponse(
                                            route.wssUrl(), "webrtc-signaling.v1", route.epoch(), 0);
                                });
        result.whenComplete(
                (value, error) ->
                        receipt.logical(
                                error == null
                                        ? Outcome.READ
                                        : error instanceof AuthException ? Outcome.FORBIDDEN : Outcome.UNAVAILABLE));
        return result;
    }

    public record BootstrapResponse(
            String wssUrl,
            String protocol,
            @com.fasterxml.jackson.annotation.JsonFormat(
                    shape = com.fasterxml.jackson.annotation.JsonFormat.Shape.STRING)
            long directoryEpoch,
            int retryAfterMillis) {
    }
}
