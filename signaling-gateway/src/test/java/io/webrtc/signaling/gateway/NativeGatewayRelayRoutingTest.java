package io.webrtc.signaling.gateway;

import static org.assertj.core.api.Assertions.*;

import com.google.protobuf.ByteString;
import io.webrtc.signaling.protocol.*;
import io.webrtc.signaling.protocol.Identity.*;
import io.webrtc.signaling.protocol.internal.*;
import io.webrtc.signaling.rpc.*;
import io.webrtc.signaling.storage.*;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

/** Wire routing only: explicit TEST_ONLY proof/rejection, never a native delivery attestation. */
class NativeGatewayRelayRoutingTest {
  @Test
  void offerUsesIsolatedVolatileRelayLane() {
    check(SignalEnvelope.Type.OFFER);
  }

  @Test
  void answerUsesIsolatedVolatileRelayLane() {
    check(SignalEnvelope.Type.ANSWER);
  }

  @Test
  void iceBatchUsesIsolatedVolatileRelayLane() {
    check(SignalEnvelope.Type.ICE_CANDIDATES);
  }

  @Test
  void endMarkerUsesIsolatedVolatileRelayLane() {
    check(SignalEnvelope.Type.END_OF_CANDIDATES);
  }

  @Test
  void volatileOfferCannotBeProjectedAsDurableCommit() {
    check(SignalEnvelope.Type.OFFER, true);
  }

  private static void check(SignalEnvelope.Type type) {
    check(type, false);
  }

  private static void check(SignalEnvelope.Type type, boolean malformedCommit) {
    UUID boot = UUID.randomUUID(), connection = UUID.randomUUID();
    var sender =
        new AuthenticatedSession(
            new UserId("TEST_ONLY_USER"),
            new SessionKey("TEST_ONLY_ISSUER", "TEST_ONLY_JTI"),
            new SessionIncarnation(UUID.randomUUID()),
            1,
            connection);
    var route =
        new SessionRepository.Route(
            sender.userId(),
            sender.key(),
            sender.incarnation(),
            1,
            "TEST_ONLY_GW",
            boot,
            connection,
            Instant.now().plusSeconds(60),
            "TEST_ONLY_KEY",
            1);
    var gateway =
        new NativeSessionHandler.GatewayIdentity(route.gatewayId(), boot, "c001", 1, "TEST_ONLY");
    var lane = new AtomicReference<CellRpcServer.Operation>();
    var dispatched = new AtomicReference<InternalCommand>();
    var network =
        new NativeGatewayCommands.Network() {
          public CompletionStage<SessionReply> session(SessionCommand command, Duration remaining) {
            assertThat(remaining).isLessThanOrEqualTo(Duration.ofSeconds(1));
            assertThat(command.getRemainingBudgetMs()).isBetween(1L, 1000L);
            assertThat(command.getType()).isEqualTo("READ_RELAY_PROOF");
            assertThat(command.getDestinationCell()).isEqualTo("c001");
            try {
              var request =
                  new com.fasterxml.jackson.databind.ObjectMapper()
                      .findAndRegisterModules()
                      .readValue(
                          command.getPayload().toByteArray(), NativeSessionHandler.Request.class);
              assertThat(request.proofCommand().payloadJson()).isEqualTo("{}");
            } catch (java.io.IOException invalid) {
              throw new AssertionError(invalid);
            }
            return CompletableFuture.completedFuture(
                SessionReply.newBuilder()
                    .setOperationId(command.getOperationId())
                    .setStatus("READ")
                    .setResult(
                        ByteString.copyFrom(RpcBusinessHandler.encode("TEST_ONLY_WIRE_PROOF")))
                    .build());
          }

          public CompletionStage<InternalReply> call(
              CellRpcServer.Operation operation, InternalCommand command, Duration remaining) {
            lane.set(operation);
            dispatched.set(command);
            if (malformedCommit)
              return CompletableFuture.completedFuture(
                  InternalReply.newBuilder()
                      .setOperationId(command.getOperationId())
                      .setCallId(command.getCallId())
                      .setAckCommitted(true)
                      .setStatus("COMMITTED")
                      .setResult(
                          ByteString.copyFrom(
                              RpcBusinessHandler.encode(
                                  new CallCommandService.Outcome(
                                      "FINAL",
                                      "RELAYED",
                                      new CallId(command.getCallId()),
                                      1,
                                      "CONNECTING",
                                      List.of()))))
                      .build());
            return CompletableFuture.completedFuture(
                InternalReply.newBuilder()
                    .setOperationId(command.getOperationId())
                    .setCallId(command.getCallId())
                    .setStatus("PENDING")
                    .setErrorCode("UNSUPPORTED_OPERATION")
                    .build());
          }
        };
    var call = CallId.create("c002", 1);
    var request = new RequestId(UUID.randomUUID());
    var command =
        new CallCommand(
            type,
            sender,
            request,
            call,
            CommandScope.call(call),
            null,
            new NegotiationId(1),
            new IceGeneration(1),
            "{\"large\":\"" + "x".repeat(65536) + "\"}",
            "a".repeat(64));
    var commands =
        new NativeGatewayCommands(
            gateway,
            user -> new ProofBindings.TrustedHome("c001", 1, 1),
            network,
            Clock.systemUTC());
    String reply =
        commands
            .execute(command, route, "TEST_ONLY_VERIFIED_TOKEN", Duration.ofSeconds(2))
            .toCompletableFuture()
            .join();
    assertThat(lane.get()).isEqualTo(CellRpcServer.Operation.RELAY);
    assertThat(dispatched.get().getType()).isEqualTo(type.name());
    assertThat(dispatched.get().getDestinationCell()).isEqualTo("c002");
    assertThat(dispatched.get().getOperationId()).isEqualTo(request.value().toString());
    assertThat(dispatched.get().getCallId()).isEqualTo(call.value());
    assertThat(reply)
        .contains(
            malformedCommit ? "OUTCOME_UNKNOWN" : "UNSUPPORTED_OPERATION", "\"ackCommitted\":false")
        .doesNotContain("ACK_COMMITTED");
  }
}
