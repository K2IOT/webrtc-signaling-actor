package io.webrtc.signaling.rpc;

import static org.assertj.core.api.Assertions.*;

import io.webrtc.signaling.actors.call.CallActor;
import io.webrtc.signaling.actors.user.UserCommand;
import io.webrtc.signaling.protocol.*;
import io.webrtc.signaling.protocol.internal.*;
import io.webrtc.signaling.storage.*;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import org.junit.jupiter.api.Test;

class NativeProofPhysicalCompletionTest {
  @Test
  void producerRetainsBothGrantAndNetworkPhysicalWorkAfterLogicalProofResponse() {
    var fixture = new NativeProofIssuerTest();
    var request = fixture.request();
    var mutationCleanup = new CompletableFuture<Void>();
    var grantCleanup = new CompletableFuture<Void>();
    var networkCleanup = new CompletableFuture<Void>();
    var issued =
        new CoordinatorGrantService.Issued(
            fixture.snapshot(),
            fixture.group,
            1,
            fixture.now,
            fixture.now.plusSeconds(5),
            "a".repeat(64));
    var granted =
        CompletableFuture.completedFuture(new CallActor.GrantReply("GRANTED", issued, request));
    var actors =
        new RpcBusinessHandler.ActorIngress() {
          public RpcOperation<CallActor.GrantReply> grantTracked(
              HomeParticipationService.Request r,
              HomeParticipationService.AuthorizationIntent a,
              String h,
              Instant d,
              int b) {
            return new RpcOperation<>(granted, grantCleanup);
          }

          public CompletionStage<CallActor.GrantReply> grant(
              HomeParticipationService.Request r,
              HomeParticipationService.AuthorizationIntent a,
              String h,
              Instant d,
              int b) {
            return granted;
          }

          public CompletionStage<UserCommand.Result> user(
              UserCommand.Operation o, Instant d, int b) {
            throw new AssertionError();
          }

          public CompletionStage<CallCommandService.Outcome> call(
              CallCommand c, String p, Instant d, int b) {
            throw new AssertionError();
          }

          public CompletionStage<CallWorkflowService.Outcome> progress(
              CallWorkflowService.Transition t, Instant d, int b) {
            return CompletableFuture.completedFuture(
                new CallWorkflowService.Outcome("TEST_ONLY", null, List.of()));
          }

          public RpcOperation<CallWorkflowService.Outcome> progressTracked(
              CallWorkflowService.Transition t, Instant d, int b) {
            return new RpcOperation<>(progress(t, d, b), mutationCleanup);
          }
        };
    var network =
        new NativeSagaEffects.Network() {
          public CompletionStage<InternalReply> call(
              CellRpcServer.Operation o, InternalCommand c, Duration b) {
            return response(c);
          }

          public RpcOperation<InternalReply> callTracked(
              CellRpcServer.Operation o, InternalCommand c, Duration b) {
            return new RpcOperation<>(response(c), networkCleanup);
          }

          private CompletionStage<InternalReply> response(InternalCommand c) {
            return CompletableFuture.completedFuture(
                InternalReply.newBuilder()
                    .setOperationId(c.getOperationId())
                    .setCallId(c.getCallId())
                    .setStatus("READ")
                    .setResult(
                        com.google.protobuf.ByteString.copyFrom(
                            RpcBusinessHandler.encode(
                                new RpcBusinessHandler.HomeProofReply(
                                    "TEST_ONLY",
                                    fixture.now.plusSeconds(5),
                                    fixture.now.plusSeconds(30)))))
                    .build());
          }
        };
    var transition =
        new CallWorkflowService.Transition(
            request.call(),
            fixture.group,
            1,
            1,
            request.grant().operation(),
            CallWorkflowService.Step.ACCEPT,
            null,
            List.of(),
            null,
            fixture.now.plusSeconds(5),
            null,
            "TEST_ONLY",
            null);
    var client =
        new NativeHomeProofClient(actors, network, Clock.fixed(fixture.now, ZoneOffset.UTC));
    var operation =
        client.proveTracked(
            request, "c002", null, transition, "SESSION", null, Duration.ofSeconds(2));
    assertThat(operation.logical().toCompletableFuture().join().signed()).isEqualTo("TEST_ONLY");
    assertThat(operation.physicalCompletion().toCompletableFuture()).isNotDone();
    grantCleanup.complete(null);
    assertThat(operation.physicalCompletion().toCompletableFuture()).isNotDone();
    networkCleanup.complete(null);
    assertThat(operation.physicalCompletion().toCompletableFuture()).isCompleted();
    var spec = new NativeWorkflowExecutor.ProofSpec(request, "c002", null, null);
    var workflow =
        new NativeWorkflowExecutor(client, actors, Clock.fixed(fixture.now, ZoneOffset.UTC))
            .applyTracked(transition, spec, spec, Duration.ofSeconds(2));
    assertThat(workflow.logical().toCompletableFuture().join().code()).isEqualTo("TEST_ONLY");
    assertThat(workflow.physicalCompletion().toCompletableFuture()).isNotDone();
    mutationCleanup.complete(null);
    assertThat(workflow.physicalCompletion().toCompletableFuture()).isCompleted();
  }
}
