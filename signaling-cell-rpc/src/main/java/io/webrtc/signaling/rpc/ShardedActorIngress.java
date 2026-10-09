package io.webrtc.signaling.rpc;

import io.webrtc.signaling.actors.admission.*;
import io.webrtc.signaling.actors.call.*;
import io.webrtc.signaling.actors.cluster.*;
import io.webrtc.signaling.actors.user.*;
import io.webrtc.signaling.protocol.*;
import io.webrtc.signaling.storage.*;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import org.apache.pekko.actor.typed.*;
import org.apache.pekko.actor.typed.javadsl.Adapter;
import org.apache.pekko.cluster.sharding.typed.javadsl.ClusterSharding;

/**
 * Shared admission before EntityRef enqueue; physical receipts survive logical timeout/entity
 * termination.
 */
public final class ShardedActorIngress implements RpcBusinessHandler.ActorIngress {
  private static final class PhysicalState {
    boolean draining;
    final Set<CompletableFuture<?>> active = new HashSet<>();
  }

  private static final Map<ActorSystem<?>, PhysicalState> PHYSICAL =
      Collections.synchronizedMap(new WeakHashMap<>());
  private final PhysicalState physicalState;
  private static final Map<ActorSystem<?>, EntityAdmission> ADMISSIONS =
      Collections.synchronizedMap(new WeakHashMap<>());
  private final ActorSystem<?> system;
  private final ClusterSharding sharding;
  private final Clock clock;
  private final EntityAdmission admission;
  private final ApplicationSerializer serializer;

  public ShardedActorIngress(ActorSystem<?> system, Clock clock) {
    this(
        system,
        clock,
        ADMISSIONS.computeIfAbsent(system, key -> EntityAdmission.forIngressProducers(7)));
  }

  public ShardedActorIngress(ActorSystem<?> system, Clock clock, EntityAdmission admission) {
    this.system = Objects.requireNonNull(system);
    physicalState = PHYSICAL.computeIfAbsent(system, key -> new PhysicalState());
    sharding = ClusterSharding.get(system);
    this.clock = Objects.requireNonNull(clock);
    this.admission = Objects.requireNonNull(admission);
    serializer = new ApplicationSerializer(Adapter.toClassic(system));
  }

  private Duration remaining(Instant deadline) {
    var duration = Duration.between(clock.instant(), deadline);
    if (duration.isNegative() || duration.isZero()) throw new DbOverloadedException();
    return duration.compareTo(Duration.ofSeconds(2)) > 0 ? Duration.ofSeconds(2) : duration;
  }

  @FunctionalInterface
  private interface WireSend<T> {
    void send(ActorRef<T> reply, CompletionReceipt receipt, EntityAdmission.Ticket ticket);
  }

  private <T> RpcOperation<T> ask(
      String entity,
      EntityAdmission.Priority priority,
      int bytes,
      Instant deadline,
      Class<T> type,
      WireSend<T> send) {
    synchronized (physicalState) {
      if (physicalState.draining) throw new EntityAdmission.Overloaded();
      int producers = 0;
      for (var member : org.apache.pekko.cluster.typed.Cluster.get(system).state().getMembers())
        if (member.hasRole("signaling-actor")) producers++;
      if (producers > 7) throw new EntityAdmission.Overloaded();
      var ticket = admission.acquire(entity, priority, bytes);
      try {
        var result =
            TrackedEntityAsk.ask(
                system,
                remaining(deadline),
                type,
                (reply, receipt) -> send.send(reply, receipt, ticket));
        ticket.releaseAfter(result.physicalCompletion());
        var physical = result.physicalCompletion().toCompletableFuture();
        physicalState.active.add(physical);
        physical.whenComplete(
            (v, error) -> {
              if (error == null)
                synchronized (physicalState) {
                  physicalState.active.remove(physical);
                }
            });
        return new RpcOperation<>(result.logical(), result.physicalCompletion());
      } catch (RuntimeException failed) {
        ticket.close();
        throw failed;
      }
    }
  }

  public CompletionStage<Void> settleAdmitted() {
    synchronized (physicalState) {
      return CompletableFuture.allOf(physicalState.active.toArray(CompletableFuture[]::new))
          .minimalCompletionStage();
    }
  }

  /** Invoke only after framework handoff: closes all producers in this ActorSystem permanently. */
  public CompletionStage<Void> drain() {
    synchronized (physicalState) {
      physicalState.draining = true;
      return settleAdmitted();
    }
  }

  public CompletionStage<CallActor.GrantReply> grant(
      HomeParticipationService.Request r,
      HomeParticipationService.AuthorizationIntent action,
      String destination,
      Instant deadline,
      int bytes) {
    return grantTracked(r, action, destination, deadline, bytes).logical();
  }

  public RpcOperation<CallActor.GrantReply> grantTracked(
      HomeParticipationService.Request r,
      HomeParticipationService.AuthorizationIntent action,
      String destination,
      Instant deadline,
      int bytes) {
    return ask(
        "call:" + r.call().value(),
        Set.of("RENEW", "RELEASE", "CONFIRM").contains(action.action())
            ? EntityAdmission.Priority.SAFETY
            : EntityAdmission.Priority.NORMAL,
        bytes,
        deadline,
        CallActor.GrantReply.class,
        (reply, receipt, ticket) ->
            NativeEnvelopeAdmission.send(
                ticket,
                charge ->
                    new CallActor.GrantToHome(
                        r, action, destination, reply, deadline, charge, receipt),
                sharding.entityRefFor(ShardingBootstrap.CALL_TYPE, r.call().value())::tell,
                serializer));
  }

  public CompletionStage<UserCommand.Result> user(
      UserCommand.Operation operation, Instant deadline, int bytes) {
    return userTracked(operation, deadline, bytes).logical();
  }

  public RpcOperation<UserCommand.Result> userTracked(
      UserCommand.Operation operation, Instant deadline, int bytes) {
    var priority =
        operation instanceof UserCommand.Renew
                || operation instanceof UserCommand.Release
                || operation instanceof UserCommand.Close
                || operation instanceof UserCommand.Refresh
                || operation instanceof UserCommand.Activate
            ? EntityAdmission.Priority.SAFETY
            : EntityAdmission.Priority.NORMAL;
    return ask(
        "user:" + operation.user().value(),
        priority,
        bytes,
        deadline,
        UserCommand.Result.class,
        (reply, receipt, ticket) ->
            NativeEnvelopeAdmission.send(
                ticket,
                charge -> new UserCommand.Mutate(operation, reply, deadline, charge, receipt),
                sharding.entityRefFor(ShardingBootstrap.USER_TYPE, operation.user().value())::tell,
                serializer));
  }

  public CompletionStage<CallCommandService.Outcome> call(
      CallCommand command, String proof, Instant deadline, int bytes) {
    return callTracked(command, proof, deadline, bytes).logical();
  }

  public RpcOperation<CallCommandService.Outcome> callTracked(
      CallCommand command, String proof, Instant deadline, int bytes) {
    if (command.callId() == null)
      throw new IllegalArgumentException("Candidate call must be selected before shard routing");
    var priority =
        Set.of(SignalEnvelope.Type.HANGUP, SignalEnvelope.Type.CANCEL, SignalEnvelope.Type.RESUME)
                .contains(command.type())
            ? EntityAdmission.Priority.SAFETY
            : EntityAdmission.Priority.NORMAL;
    return ask(
        "call:" + command.callId().value(),
        priority,
        bytes,
        deadline,
        CallCommandService.Outcome.class,
        (reply, receipt, ticket) ->
            NativeEnvelopeAdmission.send(
                ticket,
                charge -> new CallActor.Execute(command, proof, reply, deadline, charge, receipt),
                sharding.entityRefFor(ShardingBootstrap.CALL_TYPE, command.callId().value())::tell,
                serializer));
  }

  public CompletionStage<CallWorkflowService.Outcome> progress(
      CallWorkflowService.Transition transition, Instant deadline, int bytes) {
    return progressTracked(transition, deadline, bytes).logical();
  }

  public RpcOperation<CallWorkflowService.Outcome> progressTracked(
      CallWorkflowService.Transition transition, Instant deadline, int bytes) {
    return ask(
        "call:" + transition.call().value(),
        transition.step() == CallWorkflowService.Step.TERMINATE
            ? EntityAdmission.Priority.SAFETY
            : EntityAdmission.Priority.NORMAL,
        bytes,
        deadline,
        CallWorkflowService.Outcome.class,
        (reply, receipt, ticket) ->
            NativeEnvelopeAdmission.send(
                ticket,
                charge -> new CallActor.Progress(transition, reply, deadline, charge, receipt),
                sharding.entityRefFor(ShardingBootstrap.CALL_TYPE, transition.call().value())::tell,
                serializer));
  }
}
