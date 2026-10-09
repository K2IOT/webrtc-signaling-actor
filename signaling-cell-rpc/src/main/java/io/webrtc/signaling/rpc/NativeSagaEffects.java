package io.webrtc.signaling.rpc;

import com.fasterxml.jackson.databind.ObjectMapper;
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

/**
 * Transport orchestration holds no SQL locks. Each effect asks the current entity for a native
 * sealed grant.
 */
public final class NativeSagaEffects implements CrossCellSaga.Effects {
  public sealed interface Step permits HomeStep, CoordinatorStep, ProvenCoordinatorStep {}

  public record HomeStep(String destination, UserCommand.Operation operation, String sessionProof)
      implements Step {
    public HomeStep {
      Objects.requireNonNull(destination);
      Objects.requireNonNull(operation);
    }
  }

  public record CoordinatorStep(CallWorkflowService.Transition transition) implements Step {
    public CoordinatorStep {
      Objects.requireNonNull(transition);
    }
  }

  public record ProvenCoordinatorStep(
      CallWorkflowService.Transition transition,
      NativeWorkflowExecutor.ProofSpec caller,
      NativeWorkflowExecutor.ProofSpec callee)
      implements Step {
    public ProvenCoordinatorStep {
      Objects.requireNonNull(transition);
      Objects.requireNonNull(caller);
      Objects.requireNonNull(callee);
    }
  }

  @FunctionalInterface
  public interface Planner {
    Step next(CrossCellSaga.Phase phase);

    default void completed(CrossCellSaga.Phase phase, Object committedDto) {}
  }

  @FunctionalInterface
  public interface Network {
    CompletionStage<InternalReply> call(
        CellRpcServer.Operation operation, InternalCommand command, Duration budget);

    default RpcOperation<InternalReply> callTracked(
        CellRpcServer.Operation operation, InternalCommand command, Duration budget) {
      var stage = call(operation, command, budget);
      return new RpcOperation<>(stage, stage);
    }

    static Network from(CellRpcClient client) {
      Objects.requireNonNull(client);
      return new Network() {
        public CompletionStage<InternalReply> call(
            CellRpcServer.Operation op, InternalCommand c, Duration b) {
          return client.call(op, c, b);
        }

        public RpcOperation<InternalReply> callTracked(
            CellRpcServer.Operation op, InternalCommand c, Duration b) {
          return client.callTracked(op, c, b);
        }
      };
    }
  }

  private static final ObjectMapper JSON = new ObjectMapper().findAndRegisterModules();
  private final RpcBusinessHandler.ActorIngress actors;
  private final Network network;
  private final Planner planner;
  private final Clock clock;

  public NativeSagaEffects(
      RpcBusinessHandler.ActorIngress actors, Network network, Planner planner, Clock clock) {
    this.actors = Objects.requireNonNull(actors);
    this.network = Objects.requireNonNull(network);
    this.planner = Objects.requireNonNull(planner);
    this.clock = Objects.requireNonNull(clock);
  }

  @Override
  public CompletionStage<String> apply(
      CrossCellSaga.Phase phase, UUID originalOperation, Duration budget) {
    return applyTracked(phase, originalOperation, budget).logical();
  }

  @Override
  public RpcOperation<String> applyTracked(
      CrossCellSaga.Phase phase, UUID originalOperation, Duration budget) {
    var scope = new PhysicalScope();
    CompletionStage<String> logical;
    try {
      logical = dispatch(phase, originalOperation, budget, scope);
    } catch (RuntimeException failed) {
      logical = CompletableFuture.failedFuture(failed);
    }
    return scope.seal(logical);
  }

  private CompletionStage<String> dispatch(
      CrossCellSaga.Phase phase, UUID originalOperation, Duration budget, PhysicalScope scope) {
    if (budget == null || budget.isNegative() || budget.isZero())
      return CompletableFuture.completedFuture("OUTCOME_UNKNOWN");
    long end = System.nanoTime() + Math.min(budget.toNanos(), Duration.ofSeconds(2).toNanos());
    var step = planner.next(phase);
    step = bindPhase(step, phase, originalOperation);
    if (step instanceof ProvenCoordinatorStep local) {
      var t = local.transition();
      if (!t.operation().equals(phaseOperation(originalOperation, t.call(), phase)))
        throw new IllegalArgumentException("Saga operation changed");
      var executor =
          new NativeWorkflowExecutor(
              new NativeHomeProofClient(actors, network, clock), actors, clock);
      return scope
          .track(executor.applyTracked(t, local.caller(), local.callee(), remaining(end)))
          .thenApply(
              value -> {
                planner.completed(phase, value);
                return value.code();
              });
    }
    if (step instanceof CoordinatorStep local) {
      var t = local.transition();
      if (!t.operation().equals(phaseOperation(originalOperation, t.call(), phase)))
        throw new IllegalArgumentException("Saga operation changed");
      return scope
          .track(
              actors.progressTracked(
                  t,
                  clock.instant().plus(remaining(end)),
                  RpcBusinessHandler.encode(new RpcBusinessHandler.WorkflowPayload(t)).length))
          .thenApply(
              value -> {
                planner.completed(phase, value);
                return value.code();
              });
    }
    var home = (HomeStep) step;
    var request = RpcBusinessHandler.request(home.operation());
    UUID effectOperation = phaseOperation(originalOperation, request.call(), phase);
    if (!request.grant().operation().equals(effectOperation))
      throw new IllegalArgumentException("Saga operation changed");
    var action = RpcBusinessHandler.action(home.operation());
    return scope
        .track(
            actors.grantTracked(
                request,
                action,
                home.destination(),
                clock.instant().plus(remaining(end)),
                RpcBusinessHandler.encode(home.operation()).length))
        .thenCompose(
            granted -> {
              if (!granted.code().equals("GRANTED")
                  || granted.signedRequest() == null
                  || granted.issued() == null)
                return CompletableFuture.completedFuture(granted.code());
              var signed = granted.signedRequest();
              if (!sameIntent(request, signed)
                  || !signed.grant().operation().equals(effectOperation))
                return CompletableFuture.completedFuture("UNKNOWN");
              var operation = replace(home.operation(), signed);
              var token = granted.issued().token();
              var g = signed.grant();
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
              var requestType = type(operation);
              var command =
                  InternalCommand.newBuilder()
                      .setSchemaMajor(1)
                      .setSchemaMinor(0)
                      .setDestinationCell(home.destination())
                      .setOperationId(effectOperation.toString())
                      .setCallId(signed.call().value())
                      .setCommandScope(CommandScope.call(signed.call()).value())
                      .setType(requestType)
                      .setAuthority(authority)
                      .setExpectedCallVersion(g.authorizedCallVersion())
                      .setPayloadHash(
                          ByteString.copyFrom(
                              HexFormat.of()
                                  .parseHex(
                                      HomeParticipationService.authorizationHash(signed, action))))
                      .setRemainingBudgetMs(Math.max(1, remaining(end).toMillis()))
                      .setPayload(
                          ByteString.copyFrom(
                              RpcBusinessHandler.encode(
                                  new RpcBusinessHandler.UserPayload(
                                      operation, home.sessionProof()))))
                      .build();
              CellRpcServer.Operation rpc =
                  operation instanceof UserCommand.Accept
                      ? CellRpcServer.Operation.CLAIM
                      : operation instanceof UserCommand.Release
                          ? CellRpcServer.Operation.RELEASE
                          : operation instanceof UserCommand.Renew
                                  || operation instanceof UserCommand.Activate
                              ? CellRpcServer.Operation.EXECUTE
                              : CellRpcServer.Operation.RESERVE;
              return scope
                  .track(network.callTracked(rpc, command, remaining(end)))
                  .thenApply(
                      reply -> {
                        if (!reply.getOperationId().equals(effectOperation.toString())
                            || !reply.getCallId().equals(signed.call().value()))
                          return "OUTCOME_UNKNOWN";
                        if (!reply.getAckCommitted()
                            && !(operation instanceof UserCommand.QueryProof
                                && reply.getStatus().equals("READ")
                                && reply.getErrorCode().isEmpty()))
                          return reply.getErrorCode().isEmpty()
                              ? "OUTCOME_UNKNOWN"
                              : reply.getErrorCode();
                        try {
                          var result =
                              JSON.readValue(
                                  reply.getResult().toByteArray(), UserCommand.Result.class);
                          planner.completed(phase, result);
                          return result.code().name();
                        } catch (Exception invalid) {
                          return "OUTCOME_UNKNOWN";
                        }
                      });
            });
  }

  /**
   * Public consent/acquisition IDs survive; other phases use restart-stable domain-separated child
   * IDs.
   */
  public static UUID phaseOperation(
      UUID original, io.webrtc.signaling.protocol.Identity.CallId call, CrossCellSaga.Phase phase) {
    if (Set.of(
            CrossCellSaga.Phase.CLAIM_HOME,
            CrossCellSaga.Phase.ACCEPT_COORDINATOR,
            CrossCellSaga.Phase.RESERVE_HOME,
            CrossCellSaga.Phase.RING_COORDINATOR)
        .contains(phase)) return original;
    try {
      var digest =
          java.security.MessageDigest.getInstance("SHA-256")
              .digest(
                  ("signaling-saga-phase-v1\n"
                          + call.value()
                          + "\n"
                          + original
                          + "\n"
                          + phase.name())
                      .getBytes(java.nio.charset.StandardCharsets.UTF_8));
      digest[6] = (byte) ((digest[6] & 15) | 0x80);
      digest[8] = (byte) ((digest[8] & 63) | 0x80);
      var bytes = ByteBuffer.wrap(digest);
      return new UUID(bytes.getLong(), bytes.getLong());
    } catch (java.security.NoSuchAlgorithmException impossible) {
      throw new IllegalStateException(impossible);
    }
  }

  private static Step bindPhase(Step step, CrossCellSaga.Phase phase, UUID original) {
    if (step instanceof HomeStep home) {
      var request = RpcBusinessHandler.request(home.operation());
      UUID id = phaseOperation(original, request.call(), phase);
      requireOrigin(request.grant().operation(), original, id);
      return new HomeStep(
          home.destination(),
          replace(home.operation(), withOperation(request, id)),
          home.sessionProof());
    }
    var t =
        step instanceof CoordinatorStep c
            ? c.transition()
            : ((ProvenCoordinatorStep) step).transition();
    UUID id = phaseOperation(original, t.call(), phase);
    requireOrigin(t.operation(), original, id);
    var bound =
        new CallWorkflowService.Transition(
            t.call(),
            t.group(),
            t.directoryEpoch(),
            t.expectedVersion(),
            id,
            t.step(),
            t.winner(),
            t.offered(),
            t.activationId(),
            t.proofExpiresAt(),
            t.participantUntil(),
            t.proof(),
            t.reason());
    if (step instanceof CoordinatorStep) return new CoordinatorStep(bound);
    var proved = (ProvenCoordinatorStep) step;
    return new ProvenCoordinatorStep(
        bound, bindProof(proved.caller(), original, id), bindProof(proved.callee(), original, id));
  }

  private static NativeWorkflowExecutor.ProofSpec bindProof(
      NativeWorkflowExecutor.ProofSpec spec, UUID original, UUID id) {
    requireOrigin(spec.request().grant().operation(), original, id);
    return new NativeWorkflowExecutor.ProofSpec(
        withOperation(spec.request(), id), spec.home(), spec.sender(), spec.participant());
  }

  private static void requireOrigin(UUID actual, UUID original, UUID derived) {
    if (!actual.equals(original) && !actual.equals(derived))
      throw new IllegalArgumentException("Saga operation changed");
  }

  private static Request withOperation(Request r, UUID id) {
    var g = r.grant();
    return new Request(
        r.user(),
        r.call(),
        r.acquireOperation(),
        r.payloadHash(),
        r.directoryEpoch(),
        r.phase(),
        new Grant(
            g.cell(),
            g.storageEpoch(),
            g.hashVersion(),
            g.group(),
            g.groupEpoch(),
            g.sequence(),
            id,
            g.issuedAt(),
            g.expiresAt(),
            g.proof(),
            g.authorizedCallVersion()));
  }

  private static Duration remaining(long end) {
    long left = end - System.nanoTime();
    if (left <= 0) throw new CompletionException(new TimeoutException());
    return Duration.ofNanos(left);
  }

  private static boolean sameIntent(Request a, Request b) {
    return a.user().equals(b.user())
        && a.call().equals(b.call())
        && a.acquireOperation().equals(b.acquireOperation())
        && a.payloadHash().equals(b.payloadHash())
        && a.directoryEpoch() == b.directoryEpoch()
        && a.phase() == b.phase();
  }

  private static UserCommand.Operation replace(UserCommand.Operation op, Request r) {
    return switch (op) {
      case UserCommand.Reserve v -> new UserCommand.Reserve(r);
      case UserCommand.Renew v -> new UserCommand.Renew(r, v.reservation(), v.version());
      case UserCommand.Release v -> new UserCommand.Release(r, v.reservation(), v.version());
      case UserCommand.Accept v -> new UserCommand.Accept(r, v.reservation(), v.route());
      case UserCommand.Activate v ->
          new UserCommand.Activate(
              r,
              v.reservation(),
              v.version(),
              v.activation(),
              v.callVersion(),
              v.winner(),
              r.grant().operation());
      case UserCommand.QueryProof v -> new UserCommand.QueryProof(r, v.sender());
      default -> throw new IllegalArgumentException("Home effect required");
    };
  }

  private static String type(UserCommand.Operation op) {
    return switch (op) {
      case UserCommand.Reserve r -> "ReserveUser";
      case UserCommand.Renew r -> "RenewReservation";
      case UserCommand.Release r -> "ReleaseIfCallVersion";
      case UserCommand.Accept r -> "ClaimAccept";
      case UserCommand.Activate r -> "ConfirmActivation";
      case UserCommand.QueryProof r -> "QueryParticipation";
      default -> throw new IllegalArgumentException("Home effect required");
    };
  }
}
