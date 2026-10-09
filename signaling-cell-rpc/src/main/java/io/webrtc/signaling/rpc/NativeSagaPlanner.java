package io.webrtc.signaling.rpc;

import io.webrtc.signaling.actors.user.UserCommand;
import io.webrtc.signaling.protocol.Identity.*;
import io.webrtc.signaling.storage.*;
import io.webrtc.signaling.storage.CallSnapshotRepository.*;
import io.webrtc.signaling.storage.HomeParticipationService.*;
import java.time.*;
import java.util.*;

/**
 * Bounded per-operation planner. Inputs are guarded projections; every effect obtains new native
 * proofs.
 */
public final class NativeSagaPlanner implements NativeSagaEffects.Planner {
  private Snapshot snapshot;
  private final AuthoritySql.GroupToken group;
  private final String inviteHash, acceptProof;
  private final UUID original, activation;
  private final HomeProofReadService.View caller, callee;
  private final AuthenticatedSession accepting;
  private final Clock clock;
  private Participation callerParticipation, calleeParticipation;

  public NativeSagaPlanner(
      Snapshot snapshot,
      AuthoritySql.GroupToken group,
      String inviteHash,
      UUID original,
      HomeProofReadService.View caller,
      HomeProofReadService.View callee,
      AuthenticatedSession accepting,
      String acceptProof,
      Clock clock) {
    this.snapshot = Objects.requireNonNull(snapshot);
    this.group = Objects.requireNonNull(group);
    if (inviteHash == null || !inviteHash.matches("[0-9a-f]{64}"))
      throw new IllegalArgumentException("Invalid acquisition hash");
    this.inviteHash = inviteHash;
    this.original = Objects.requireNonNull(original);
    this.caller = Objects.requireNonNull(caller);
    this.callee = Objects.requireNonNull(callee);
    this.accepting = accepting;
    this.acceptProof = acceptProof;
    this.clock = Objects.requireNonNull(clock);
    if (snapshot.terminalAt() != null
        || !snapshot.callId().coordinatorCell().equals(group.cell())
        || snapshot.group() != group.group()
        || snapshot.hashVersion() != group.hashVersion()) throw new AuthoritySql.FencedException();
    checkHome(caller, snapshot.caller().user());
    checkHome(callee, snapshot.callee());
    callerParticipation = caller.participation();
    calleeParticipation = callee.participation();
    activation =
        snapshot.activationId() != null
            ? snapshot.activationId()
            : NativeSagaEffects.phaseOperation(
                original, snapshot.callId(), CrossCellSaga.Phase.ACTIVATE_COORDINATOR);
  }

  private void checkHome(HomeProofReadService.View view, UserId user) {
    var p = view.participation();
    if (!p.user().equals(user)
        || !p.call().equals(snapshot.callId())
        || !p.acquireOperation().equals(snapshot.inviteRequest().value())
        || !p.payloadHash().equals(inviteHash)
        || p.terminal()
        || p.highestGroupEpoch() > group.epoch()) throw new AuthoritySql.FencedException();
  }

  public UUID activationId() {
    return activation;
  }

  @Override
  public NativeSagaEffects.Step next(CrossCellSaga.Phase phase) {
    if (snapshot.terminalAt() != null) throw new AuthoritySql.FencedException();
    return switch (phase) {
      case CLAIM_HOME -> {
        if (accepting == null || acceptProof == null || acceptProof.isBlank())
          throw new AuthoritySql.FencedException();
        yield new NativeSagaEffects.HomeStep(
            callee.sourceCell(),
            new UserCommand.Accept(
                template(callee), calleeParticipation.reservationId(), route(callee, accepting)),
            acceptProof);
      }
      case CONFIRM_CALLER, CONFIRM_CALLEE -> {
        boolean isCaller = phase == CrossCellSaga.Phase.CONFIRM_CALLER;
        var view = isCaller ? caller : callee;
        var p = isCaller ? callerParticipation : calleeParticipation;
        if (!snapshot.state().equals("ACTIVATING") || !activation.equals(snapshot.activationId()))
          throw new AuthoritySql.FencedException();
        yield new NativeSagaEffects.HomeStep(
            view.sourceCell(),
            new UserCommand.Activate(
                template(view),
                p.reservationId(),
                p.version(),
                activation,
                snapshot.version(),
                p.winner(),
                original),
            null);
      }
      case RING_COORDINATOR, ACCEPT_COORDINATOR, ACTIVATE_COORDINATOR, READY_COORDINATOR ->
          coordinator(phase);
      case RESERVE_HOME, RELEASE_HOME ->
          throw new IllegalArgumentException(
              "Reserve/release require independently prepared native participation recovery");
    };
  }

  private NativeSagaEffects.Step coordinator(CrossCellSaga.Phase phase) {
    var step =
        switch (phase) {
          case RING_COORDINATOR -> CallWorkflowService.Step.RING;
          case ACCEPT_COORDINATOR -> CallWorkflowService.Step.ACCEPT;
          case ACTIVATE_COORDINATOR -> CallWorkflowService.Step.ACTIVATE;
          case READY_COORDINATOR -> CallWorkflowService.Step.READY;
          default -> throw new IllegalArgumentException();
        };
    var winner = snapshot.winner();
    if (step == CallWorkflowService.Step.ACCEPT) {
      var w = calleeParticipation.winner();
      if (w == null) throw new AuthoritySql.FencedException();
      winner = new Participant(snapshot.callee(), w.key(), w.incarnation(), w.generation());
    }
    var offered =
        step == CallWorkflowService.Step.RING
            ? callee.currentRoutes().stream()
                .map(
                    r ->
                        new Participant(
                            r.user(), r.key(), r.incarnation(), r.connectionGeneration()))
                .toList()
            : List.<Participant>of();
    var t =
        new CallWorkflowService.Transition(
            snapshot.callId(),
            group,
            caller.directoryEpoch(),
            snapshot.version(),
            original,
            step,
            winner,
            offered,
            step == CallWorkflowService.Step.ACTIVATE || step == CallWorkflowService.Step.READY
                ? activation
                : null,
            clock.instant().plusSeconds(5),
            null,
            "UNSIGNED_NATIVE_WORKFLOW",
            null);
    return new NativeSagaEffects.ProvenCoordinatorStep(
        t,
        new NativeWorkflowExecutor.ProofSpec(
            template(caller), caller.sourceCell(), null, snapshot.caller()),
        new NativeWorkflowExecutor.ProofSpec(
            template(callee),
            callee.sourceCell(),
            null,
            step == CallWorkflowService.Step.RING ? null : winner));
  }

  private HomeParticipationService.Request template(HomeProofReadService.View view) {
    var now = clock.instant();
    return new HomeParticipationService.Request(
        view.participation().user(),
        snapshot.callId(),
        snapshot.inviteRequest().value(),
        inviteHash,
        view.directoryEpoch(),
        HomeParticipationService.Phase.RINGING,
        new HomeParticipationService.Grant(
            group.cell(),
            group.storageEpoch(),
            group.hashVersion(),
            group.group(),
            group.epoch(),
            1,
            original,
            now,
            now.plusSeconds(5),
            "UNSIGNED"));
  }

  private static SessionRepository.Route route(
      HomeProofReadService.View view, AuthenticatedSession sender) {
    return view.currentRoutes().stream()
        .filter(
            r ->
                r.user().equals(sender.userId())
                    && r.key().equals(sender.key())
                    && r.incarnation().equals(sender.incarnation())
                    && r.connectionGeneration() == sender.connectionGeneration()
                    && r.connectionId().equals(sender.connectionId()))
        .findFirst()
        .orElseThrow(AuthoritySql.FencedException::new);
  }

  @Override
  public void completed(CrossCellSaga.Phase phase, Object committed) {
    if (committed instanceof CallWorkflowService.Outcome outcome) {
      var next = outcome.snapshot();
      if (next == null
          || !next.callId().equals(snapshot.callId())
          || next.version() < snapshot.version()) throw new AuthoritySql.FencedException();
      snapshot = next;
    } else if (committed instanceof UserCommand.Result result && result.participation() != null) {
      var p = result.participation();
      if (!p.call().equals(snapshot.callId()) || !p.payloadHash().equals(inviteHash))
        throw new AuthoritySql.FencedException();
      if (p.user().equals(snapshot.caller().user())) callerParticipation = p;
      else if (p.user().equals(snapshot.callee())) calleeParticipation = p;
      else throw new AuthoritySql.FencedException();
    }
  }
}
