package io.webrtc.signaling.rpc;

import com.fasterxml.jackson.databind.*;
import com.google.protobuf.ByteString;
import io.webrtc.signaling.actors.user.UserCommand;
import io.webrtc.signaling.protocol.Identity.*;
import io.webrtc.signaling.protocol.internal.*;
import io.webrtc.signaling.storage.*;
import io.webrtc.signaling.storage.HomeParticipationService.*;
import java.nio.ByteBuffer;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;

/** Every read obtains a fresh sealed hosting-actor grant, then a fresh native home projection. */
public final class NativeHomeProofClient {
  private static final ObjectMapper JSON = new ObjectMapper().findAndRegisterModules();
  private final RpcBusinessHandler.ActorIngress actors;
  private final NativeSagaEffects.Network network;
  private final Clock clock;

  public NativeHomeProofClient(
      RpcBusinessHandler.ActorIngress actors, NativeSagaEffects.Network network, Clock clock) {
    this.actors = Objects.requireNonNull(actors);
    this.network = Objects.requireNonNull(network);
    this.clock = Objects.requireNonNull(clock);
  }

  public CompletionStage<RpcBusinessHandler.HomeProofReply> prove(
      Request template,
      String home,
      AuthenticatedSession sender,
      CallWorkflowService.Transition transition,
      String purpose,
      CallSnapshotRepository.Participant participant,
      Duration budget) {
    return proveTracked(template, home, sender, transition, purpose, participant, budget).logical();
  }

  public RpcOperation<RpcBusinessHandler.HomeProofReply> proveTracked(
      Request template,
      String home,
      AuthenticatedSession sender,
      CallWorkflowService.Transition transition,
      String purpose,
      CallSnapshotRepository.Participant participant,
      Duration budget) {
    return map(
        readTracked(
            template,
            home,
            sender,
            new RpcBusinessHandler.WorkflowProofRequest(transition, purpose, participant),
            null,
            null,
            budget),
        reply -> checkedProof(reply));
  }

  public CompletionStage<RpcBusinessHandler.HomeProofReply> proveCommand(
      Request template,
      String home,
      AuthenticatedSession sender,
      io.webrtc.signaling.protocol.CallCommand command,
      CallSnapshotRepository.Participant participant,
      Duration budget) {
    return proveCommandTracked(template, home, sender, command, participant, budget).logical();
  }

  public RpcOperation<RpcBusinessHandler.HomeProofReply> proveCommandTracked(
      Request template,
      String home,
      AuthenticatedSession sender,
      io.webrtc.signaling.protocol.CallCommand command,
      CallSnapshotRepository.Participant participant,
      Duration budget) {
    if (!template.grant().operation().equals(command.requestId().value())) {
      var failed =
          CompletableFuture.<RpcBusinessHandler.HomeProofReply>failedFuture(
              new IllegalArgumentException("Command proof nonce mismatch"));
      return new RpcOperation<>(failed, CompletableFuture.completedFuture(null));
    }
    return map(
        readTracked(template, home, sender, null, command, participant, budget),
        this::checkedProof);
  }

  public CompletionStage<HomeProofReadService.View> observe(
      Request template, String home, AuthenticatedSession sender, Duration budget) {
    return observeTracked(template, home, sender, budget).logical();
  }

  public RpcOperation<HomeProofReadService.View> observeTracked(
      Request template, String home, AuthenticatedSession sender, Duration budget) {
    return map(
        readTracked(template, home, sender, null, null, null, budget),
        reply -> {
          var result = decode(reply, UserCommand.Result.class);
          if (result.code() != UserCommand.Code.PROOF_READ || result.proofView() == null)
            throw new CompletionException(new AuthoritySql.FencedException());
          return result.proofView();
        });
  }

  private RpcBusinessHandler.HomeProofReply checkedProof(InternalReply reply) {
    var proof = decode(reply, RpcBusinessHandler.HomeProofReply.class);
    if (proof.signed() == null
        || proof.signed().length() > 4096
        || proof.expiresAt() == null
        || !proof.expiresAt().isAfter(clock.instant())
        || proof.expiresAt().isAfter(clock.instant().plusSeconds(5))
        || proof.participantUntil() == null)
      throw new CompletionException(new AuthoritySql.FencedException());
    return proof;
  }

  private static <A, B> RpcOperation<B> map(
      RpcOperation<A> operation, java.util.function.Function<A, B> map) {
    var logical = operation.logical().thenApply(map);
    return new RpcOperation<>(
        logical,
        operation.physicalCompletion().thenCombine(logical.handle((v, e) -> null), (a, b) -> null));
  }

  private RpcOperation<InternalReply> readTracked(
      Request template,
      String home,
      AuthenticatedSession sender,
      RpcBusinessHandler.WorkflowProofRequest desired,
      io.webrtc.signaling.protocol.CallCommand desiredCommand,
      CallSnapshotRepository.Participant participant,
      Duration budget) {
    if (budget == null || budget.isNegative() || budget.isZero())
      return new RpcOperation<>(
          CompletableFuture.failedFuture(new DbOutcomeUnknownException()),
          CompletableFuture.completedFuture(null));
    long end = System.nanoTime() + Math.min(budget.toNanos(), Duration.ofSeconds(2).toNanos());
    var action = new AuthorizationIntent("QUERY", null, 0, null, 0, null, null);
    var scope = new PhysicalScope();
    CompletionStage<InternalReply> logical;
    try {
      logical =
          scope
              .track(
                  actors.grantTracked(
                      template,
                      action,
                      home,
                      clock.instant().plus(remaining(end)),
                      RpcBusinessHandler.encode(template).length))
              .thenCompose(
                  granted -> {
                    if (!granted.code().equals("GRANTED")
                        || granted.signedRequest() == null
                        || granted.issued() == null)
                      throw new CompletionException(new AuthoritySql.FencedException());
                    var r = granted.signedRequest();
                    var g = r.grant();
                    var token = granted.issued().token();
                    if (!r.call().equals(template.call())
                        || !r.user().equals(template.user())
                        || !r.acquireOperation().equals(template.acquireOperation())
                        || !r.payloadHash().equals(template.payloadHash())
                        || r.directoryEpoch() != template.directoryEpoch()
                        || !g.operation().equals(template.grant().operation()))
                      throw new CompletionException(new AuthoritySql.FencedException());
                    var authority =
                        GroupAuthority.newBuilder()
                            .setCellId(g.cell())
                            .setStorageEpoch(g.storageEpoch())
                            .setOwnershipHashVersion(Math.toIntExact(g.hashVersion()))
                            .setGroupId(g.group())
                            .setGroupEpoch(g.groupEpoch())
                            .setLeaseSequence(g.sequence())
                            .setOwnerIncarnation(
                                ByteString.copyFrom(
                                    ByteBuffer.allocate(16)
                                        .putLong(token.incarnation().getMostSignificantBits())
                                        .putLong(token.incarnation().getLeastSignificantBits())
                                        .array()));
                    var wire =
                        InternalCommand.newBuilder()
                            .setSchemaMajor(1)
                            .setType("QueryParticipation")
                            .setOperationId(g.operation().toString())
                            .setCallId(r.call().value())
                            .setCommandScope(CommandScope.call(r.call()).value())
                            .setDestinationCell(home)
                            .setRemainingBudgetMs(Math.max(1, remaining(end).toMillis()))
                            .setExpectedCallVersion(g.authorizedCallVersion())
                            .setPayloadHash(
                                ByteString.copyFrom(
                                    HexFormat.of()
                                        .parseHex(
                                            HomeParticipationService.authorizationHash(r, action))))
                            .setAuthority(authority)
                            .setPayload(
                                ByteString.copyFrom(
                                    RpcBusinessHandler.encode(
                                        new RpcBusinessHandler.UserPayload(
                                            new UserCommand.QueryProof(r, sender),
                                            null,
                                            desired,
                                            desiredCommand == null
                                                ? null
                                                : new RpcBusinessHandler.CommandProofRequest(
                                                    desiredCommand,
                                                    granted.issued().snapshot(),
                                                    token,
                                                    participant)))))
                            .build();
                    return scope
                        .track(
                            network.callTracked(
                                CellRpcServer.Operation.RESERVE, wire, remaining(end)))
                        .thenApply(
                            reply -> {
                              if (!reply.getOperationId().equals(wire.getOperationId())
                                  || !reply.getCallId().equals(wire.getCallId())
                                  || !reply.getStatus().equals("READ")
                                  || reply.getAckCommitted()
                                  || !reply.getErrorCode().isEmpty())
                                throw new CompletionException(new AuthoritySql.FencedException());
                              return reply;
                            });
                  });
    } catch (RuntimeException failed) {
      logical = CompletableFuture.failedFuture(failed);
    }
    return scope.seal(logical);
  }

  private static Duration remaining(long end) {
    long left = end - System.nanoTime();
    if (left <= 0) throw new CompletionException(new TimeoutException());
    return Duration.ofNanos(left);
  }

  private static <T> T decode(InternalReply reply, Class<T> type) {
    try {
      if (reply.getResult().size() > 81920) throw new IllegalArgumentException();
      return JSON.readValue(reply.getResult().toByteArray(), type);
    } catch (Exception invalid) {
      throw new CompletionException(new IllegalStateException("Invalid native home proof reply"));
    }
  }
}
