package io.webrtc.signaling.gateway;

import com.google.protobuf.ByteString;
import io.webrtc.signaling.protocol.CallCommand;
import io.webrtc.signaling.protocol.Identity.*;
import io.webrtc.signaling.protocol.SignalEnvelope;
import io.webrtc.signaling.protocol.internal.InternalCommand;
import io.webrtc.signaling.protocol.internal.InternalReply;
import io.webrtc.signaling.protocol.internal.SessionCommand;
import io.webrtc.signaling.protocol.internal.SessionReply;
import io.webrtc.signaling.rpc.CellRpcServer;
import io.webrtc.signaling.rpc.NativeSessionHandler;
import io.webrtc.signaling.rpc.ProofBindings;
import io.webrtc.signaling.rpc.RpcBusinessHandler;
import io.webrtc.signaling.storage.SessionRepository;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A retried INVITE (same issuer, jti and requestId) must reach the same call entity and group, so
 * the retry finds the first attempt's lease and result instead of a fresh cold group.
 */
class NativeGatewayInviteCallIdTest {
    private static final UUID BOOT = UUID.randomUUID(), CONNECTION = UUID.randomUUID();

    private static String dispatchedCall(String jti, RequestId request) {
        var sender =
                new AuthenticatedSession(
                        new UserId("TEST_ONLY_USER"),
                        new SessionKey("TEST_ONLY_ISSUER", jti),
                        new SessionIncarnation(UUID.randomUUID()),
                        1,
                        CONNECTION);
        var route =
                new SessionRepository.Route(
                        sender.userId(),
                        sender.key(),
                        sender.incarnation(),
                        1,
                        "TEST_ONLY_GW",
                        BOOT,
                        CONNECTION,
                        Instant.now().plusSeconds(60),
                        "TEST_ONLY_KEY",
                        1);
        var gateway =
                new NativeSessionHandler.GatewayIdentity(route.gatewayId(), BOOT, "c001", 1, "TEST_ONLY");
        var dispatched = new AtomicReference<InternalCommand>();
        var network =
                new NativeGatewayCommands.Network() {
                    public CompletionStage<SessionReply> session(SessionCommand command, Duration remaining) {
                        return CompletableFuture.completedFuture(
                                SessionReply.newBuilder()
                                        .setOperationId(command.getOperationId())
                                        .setStatus("READ")
                                        .setResult(ByteString.copyFrom(RpcBusinessHandler.encode("TEST_ONLY_PROOF")))
                                        .build());
                    }

                    public CompletionStage<InternalReply> call(
                            CellRpcServer.Operation operation, InternalCommand command, Duration remaining) {
                        dispatched.set(command);
                        return CompletableFuture.completedFuture(
                                InternalReply.newBuilder()
                                        .setOperationId(command.getOperationId())
                                        .setCallId(command.getCallId())
                                        .setStatus("PENDING")
                                        .setErrorCode("UNSUPPORTED_OPERATION")
                                        .build());
                    }
                };
        var command =
                new CallCommand(
                        SignalEnvelope.Type.INVITE,
                        sender,
                        request,
                        null,
                        CommandScope.invite(),
                        new UserId("TEST_ONLY_CALLEE"),
                        null,
                        null,
                        "{}",
                        "b".repeat(64));
        new NativeGatewayCommands(
                gateway, user -> new ProofBindings.TrustedHome("c001", 1, 1), network, Clock.systemUTC())
                .execute(command, route, "TEST_ONLY_VERIFIED_TOKEN", Duration.ofSeconds(2))
                .toCompletableFuture()
                .join();
        return dispatched.get().getCallId();
    }

    @Test
    void aRetriedInviteKeepsItsCallIdAndADifferentRequestOrTokenDoesNot() {
        var request = new RequestId(UUID.randomUUID());
        String first = dispatchedCall("TEST_ONLY_JTI", request);
        assertThat(new CallId(first).coordinatorCell()).isEqualTo("c001");
        assertThat(dispatchedCall("TEST_ONLY_JTI", request)).isEqualTo(first);
        assertThat(dispatchedCall("TEST_ONLY_JTI", new RequestId(UUID.randomUUID()))).isNotEqualTo(first);
        assertThat(dispatchedCall("TEST_ONLY_OTHER_JTI", request)).isNotEqualTo(first);
    }
}
