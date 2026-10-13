package io.webrtc.signaling.gateway;

import io.webrtc.signaling.protocol.CallCommand;
import io.webrtc.signaling.protocol.Identity.AuthenticatedSession;
import io.webrtc.signaling.protocol.Identity.CallId;
import io.webrtc.signaling.protocol.Identity.CommandScope;
import io.webrtc.signaling.protocol.SignalEnvelope;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

/**
 * Native rebind/read completes before advice is emitted. A healthy PeerConnection survives WSS
 * changes.
 */
public final class ResumeHandler {
    private final NativeAccess nativeAccess;
    private final Clock clock;

    public ResumeHandler(NativeAccess nativeAccess, Clock clock) {
        this.nativeAccess = Objects.requireNonNull(nativeAccess);
        this.clock = Objects.requireNonNull(clock);
    }

    private static boolean samePrincipal(AuthenticatedSession a, AuthenticatedSession b) {
        return b != null && a.userId().equals(b.userId()) && a.key().equals(b.key());
    }

    public CompletionStage<Result> handle(
            CallCommand command, boolean mediaHealthy, Duration budget) {
        if (!Set.of(SignalEnvelope.Type.RESUME, SignalEnvelope.Type.SYNC_CALL).contains(command.type())
                || command.callId() == null
                || !command.scope().equals(CommandScope.call(command.callId()))
                || budget.isNegative()
                || budget.isZero()
                || budget.compareTo(Duration.ofSeconds(2)) > 0)
            return CompletableFuture.failedFuture(new IllegalArgumentException("Invalid resume request"));
        var result = new CompletableFuture<Result>();
        result.orTimeout(budget.toNanos(), TimeUnit.NANOSECONDS);
        try {
            nativeAccess
                    .rebindOrRead(command, budget)
                    .whenComplete(
                            (snapshot, failure) -> {
                                if (failure != null) {
                                    result.completeExceptionally(failure);
                                    return;
                                }
                                boolean terminal =
                                        snapshot != null && Set.of("TERMINAL", "FAILED").contains(snapshot.state());
                                boolean authorized =
                                        snapshot != null
                                                && (terminal
                                                ? (samePrincipal(command.sender(), snapshot.caller())
                                                || samePrincipal(command.sender(), snapshot.winner()))
                                                : (command.sender().equals(snapshot.caller())
                                                || command.sender().equals(snapshot.winner())));
                                if (snapshot == null || !snapshot.call().equals(command.callId()) || !authorized) {
                                    result.completeExceptionally(
                                            new IllegalStateException("Native participant rebind required"));
                                    return;
                                }
                                String code;
                                if (Set.of("TERMINAL", "FAILED").contains(snapshot.state())) code = "TERMINAL";
                                else if (snapshot.graceUntil() != null
                                        && !snapshot.graceUntil().isAfter(clock.instant())) code = "GRACE_EXPIRED";
                                else if (snapshot.state().equals("ESTABLISHED") && mediaHealthy)
                                    code = "CONTINUE_MEDIA";
                                else if (snapshot.negotiationId() > 0 && !snapshot.volatileRoundRetained())
                                    code = "RESYNC_REQUIRED";
                                else code = "SNAPSHOT";
                                result.complete(new Result(code, snapshot, false));
                            });
        } catch (Throwable failure) {
            result.completeExceptionally(failure);
        }
        return result;
    }

    /**
     * Must independently authorize the current full session before returning a committed snapshot.
     */
    @FunctionalInterface
    public interface NativeAccess {
        CompletionStage<Snapshot> rebindOrRead(CallCommand command, Duration budget);
    }

    public record Snapshot(
            CallId call,
            long version,
            String state,
            UUID activationId,
            AuthenticatedSession caller,
            AuthenticatedSession winner,
            long negotiationId,
            long iceGeneration,
            boolean volatileRoundRetained,
            Instant graceUntil) {
        public Snapshot {
            Objects.requireNonNull(call);
            Objects.requireNonNull(caller);
            if (version < 1
                    || !Set.of(
                            "PREPARING",
                            "RINGING",
                            "ACCEPTED",
                            "ACTIVATING",
                            "CONNECTING",
                            "ESTABLISHED",
                            "TERMINAL",
                            "FAILED")
                    .contains(state)
                    || negotiationId < 0
                    || iceGeneration < 0
                    || (negotiationId == 0) != (iceGeneration == 0))
                throw new IllegalArgumentException("Invalid native resume snapshot");
        }
    }

    public record Result(String code, Snapshot snapshot, boolean resetPeerConnection) {
    }
}
