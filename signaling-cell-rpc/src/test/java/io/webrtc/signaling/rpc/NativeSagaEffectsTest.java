package io.webrtc.signaling.rpc;

import io.webrtc.signaling.actors.call.CallActor;
import io.webrtc.signaling.actors.user.UserCommand;
import io.webrtc.signaling.protocol.CallCommand;
import io.webrtc.signaling.protocol.Identity.CallId;
import io.webrtc.signaling.protocol.Identity.UserId;
import io.webrtc.signaling.storage.AuthoritySql;
import io.webrtc.signaling.storage.CallCommandService;
import io.webrtc.signaling.storage.CallWorkflowService;
import io.webrtc.signaling.storage.CoordinatorGrantService;
import io.webrtc.signaling.storage.HomeParticipationService.AuthorizationIntent;
import io.webrtc.signaling.storage.HomeParticipationService.Grant;
import io.webrtc.signaling.storage.HomeParticipationService.Phase;
import io.webrtc.signaling.storage.HomeParticipationService.Request;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class NativeSagaEffectsTest {
    @Test
    void typedHomeEffectRequestsSealedGrantFromCurrentEntityAndCannotSendUnsignedWork()
            throws Exception {
        Instant now = Instant.now();
        var call = new CallId(CrossCellSagaIT.CALL);
        UUID op = UUID.randomUUID();
        var request =
                new Request(
                        new UserId("bob"),
                        call,
                        UUID.randomUUID(),
                        "a".repeat(64),
                        1,
                        Phase.RINGING,
                        new Grant("c001", 1, 1, 685, 2, 1, op, now, now.plusSeconds(5), "UNSIGNED"));
        var seen = new AtomicInteger();
        RpcBusinessHandler.ActorIngress actors =
                new RpcBusinessHandler.ActorIngress() {
                    public CompletionStage<CallActor.GrantReply> grant(
                            Request r,
                            AuthorizationIntent action,
                            String destination,
                            Instant deadline,
                            int bytes) {
                        seen.incrementAndGet();
                        assertThat(destination).isEqualTo("c002");
                        assertThat(action).isEqualTo(AuthorizationIntent.reserve());
                        return CompletableFuture.completedFuture(
                                new CallActor.GrantReply("UNKNOWN", null, null));
                    }

                    public CompletionStage<UserCommand.Result> user(
                            UserCommand.Operation r, Instant d, int b) {
                        throw new AssertionError();
                    }

                    public CompletionStage<CallCommandService.Outcome> call(
                            CallCommand c, String p, Instant d, int b) {
                        throw new AssertionError();
                    }

                    public CompletionStage<CallWorkflowService.Outcome> progress(
                            CallWorkflowService.Transition t, Instant d, int b) {
                        throw new AssertionError();
                    }
                };
        var effects =
                new NativeSagaEffects(
                        actors,
                        (operation, c, budget) -> {
                            throw new AssertionError("No sealed native grant, no network effect");
                        },
                        phase -> new NativeSagaEffects.HomeStep("c002", new UserCommand.Reserve(request), null),
                        Clock.systemUTC());
        assertThat(
                effects
                        .apply(CrossCellSaga.Phase.RESERVE_HOME, op, Duration.ofSeconds(2))
                        .toCompletableFuture()
                        .join())
                .isEqualTo("UNKNOWN");
        assertThat(seen).hasValue(1);
        assertThatThrownBy(
                () ->
                        effects
                                .apply(
                                        CrossCellSaga.Phase.RESERVE_HOME, UUID.randomUUID(), Duration.ofSeconds(2))
                                .toCompletableFuture()
                                .join())
                .hasCauseInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void homeRenewalAndActivationUseTheirExecuteRpcInsteadOfReserve() {
        Instant now = Instant.now();
        var call = new CallId(CrossCellSagaIT.CALL);
        UUID op = UUID.randomUUID();
        var request =
                new Request(
                        new UserId("bob"),
                        call,
                        UUID.randomUUID(),
                        "a".repeat(64),
                        1,
                        Phase.RINGING,
                        new Grant("c001", 1, 1, 685, 2, 1, op, now, now.plusSeconds(5), "TEST_ONLY_SEALED"));
        var token =
                new AuthoritySql.GroupToken("c001", 1, 1, 685, 2, "TEST_ONLY_OWNER", UUID.randomUUID());
        var issued =
                new CoordinatorGrantService.Issued(null, token, 1, now, now.plusSeconds(5), "a".repeat(64));
        RpcBusinessHandler.ActorIngress actors =
                new RpcBusinessHandler.ActorIngress() {
                    public CompletionStage<CallActor.GrantReply> grant(
                            Request r,
                            AuthorizationIntent action,
                            String destination,
                            Instant deadline,
                            int bytes) {
                        return CompletableFuture.completedFuture(
                                new CallActor.GrantReply("GRANTED", issued, r));
                    }

                    public CompletionStage<UserCommand.Result> user(
                            UserCommand.Operation r, Instant d, int b) {
                        throw new AssertionError();
                    }

                    public CompletionStage<CallCommandService.Outcome> call(
                            CallCommand c, String p, Instant d, int b) {
                        throw new AssertionError();
                    }

                    public CompletionStage<CallWorkflowService.Outcome> progress(
                            CallWorkflowService.Transition t, Instant d, int b) {
                        throw new AssertionError();
                    }
                };
        for (var operation :
                List.<UserCommand.Operation>of(
                        new UserCommand.Renew(request, UUID.randomUUID(), 1),
                        new UserCommand.Activate(
                                request, UUID.randomUUID(), 1, UUID.randomUUID(), 3, null, op))) {
            var effects =
                    new NativeSagaEffects(
                            actors,
                            (rpc, c, budget) -> {
                                assertThat(rpc).isEqualTo(CellRpcServer.Operation.EXECUTE);
                                var code =
                                        operation instanceof UserCommand.Renew
                                                ? UserCommand.Code.RENEWED
                                                : UserCommand.Code.CONFIRMED;
                                return CompletableFuture.completedFuture(
                                        io.webrtc.signaling.protocol.internal.InternalReply.newBuilder()
                                                .setOperationId(c.getOperationId())
                                                .setCallId(c.getCallId())
                                                .setAckCommitted(true)
                                                .setStatus("COMMITTED")
                                                .setResult(
                                                        com.google.protobuf.ByteString.copyFrom(
                                                                RpcBusinessHandler.encode(
                                                                        new UserCommand.Result(code, null, null, null))))
                                                .build());
                            },
                            phase -> new NativeSagaEffects.HomeStep("c002", operation, null),
                            Clock.systemUTC());
            assertThat(
                    effects
                            .apply(CrossCellSaga.Phase.CONFIRM_CALLEE, op, Duration.ofSeconds(2))
                            .toCompletableFuture()
                            .join())
                    .isIn("RENEWED", "CONFIRMED");
        }
    }
}
