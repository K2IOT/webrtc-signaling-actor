package io.webrtc.signaling.it;

import static org.assertj.core.api.Assertions.*;

import io.webrtc.signaling.actors.call.MediaRecoveryPolicy;
import io.webrtc.signaling.gateway.ResumeHandler;
import io.webrtc.signaling.protocol.*;
import io.webrtc.signaling.protocol.Identity.*;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import org.junit.jupiter.api.Test;

class ReconnectMediaRecoveryIT {
  final CallId call = CallId.create("c001", 1);
  final UUID activation = UUID.randomUUID();
  final AtomicLong now = new AtomicLong();
  final AuthenticatedSession caller = session("caller"), winner = session("winner");

  static AuthenticatedSession session(String user) {
    return new AuthenticatedSession(
        new UserId(user),
        new SessionKey("TEST_ONLY", user),
        new SessionIncarnation(UUID.randomUUID()),
        1,
        UUID.randomUUID());
  }

  MediaRecoveryPolicy.CommittedRound round(String state, long round, long ice) {
    return new MediaRecoveryPolicy.CommittedRound(
        call, activation, 4 + round, state, round, ice, caller, winner);
  }

  MediaRecoveryPolicy policy(String state) {
    var policy =
        new MediaRecoveryPolicy(Duration.ofSeconds(5), Duration.ofSeconds(30), 1, now::get);
    policy.attach(round(state, 1, 1));
    return policy;
  }

  MediaTelemetry event(MediaTelemetry.Event type, long sequence, long round, long ice) {
    return new MediaTelemetry(type, call, round, ice, sequence, null);
  }

  @Test
  void signalingLossDoesNotResetHealthyPeerConnectionOrStartMediaFailureTimer() {
    var policy = policy("ESTABLISHED");
    assertThat(policy.signalingLost(caller).action())
        .isEqualTo(MediaRecoveryPolicy.Action.CONTINUE_MEDIA);
    now.set(Duration.ofSeconds(60).toNanos());
    assertThat(policy.tick().action()).isEqualTo(MediaRecoveryPolicy.Action.NONE);
  }

  @Test
  void disconnectedIsDebouncedAndRepeatedReportsCannotExtendRecoveryWindow() {
    var policy = policy("ESTABLISHED");
    assertThat(
            policy
                .observe(caller, event(MediaTelemetry.Event.MEDIA_DISCONNECTED, 1, 1, 1))
                .action())
        .isEqualTo(MediaRecoveryPolicy.Action.WAIT);
    now.set(Duration.ofSeconds(4).toNanos());
    assertThat(policy.tick().action()).isEqualTo(MediaRecoveryPolicy.Action.WAIT);
    policy.observe(caller, event(MediaTelemetry.Event.MEDIA_DISCONNECTED, 2, 1, 1));
    now.set(Duration.ofSeconds(6).toNanos());
    assertThat(policy.tick().action()).isEqualTo(MediaRecoveryPolicy.Action.REQUEST_RESTART);
    assertThat(policy.tick().action()).isEqualTo(MediaRecoveryPolicy.Action.NEGOTIATION_BUSY);
    now.set(Duration.ofSeconds(31).toNanos());
    assertThat(policy.tick().action()).isEqualTo(MediaRecoveryPolicy.Action.TERMINALIZE);
  }

  @Test
  void oneCommittedRestartRejectsOldGenerationAndASecondFailureTerminatesWithinPolicy() {
    var policy = policy("ESTABLISHED");
    assertThat(policy.observe(caller, event(MediaTelemetry.Event.MEDIA_FAILED, 1, 1, 1)).action())
        .isEqualTo(MediaRecoveryPolicy.Action.REQUEST_RESTART);
    assertThat(policy.observe(winner, event(MediaTelemetry.Event.ICE_RESTARTING, 1, 1, 1)).action())
        .isEqualTo(MediaRecoveryPolicy.Action.NEGOTIATION_BUSY);
    policy.restartGranted(round("ESTABLISHED", 2, 2));
    assertThat(
            policy.observe(caller, event(MediaTelemetry.Event.MEDIA_CONNECTED, 2, 1, 1)).action())
        .isEqualTo(MediaRecoveryPolicy.Action.STALE_GENERATION);
    assertThat(policy.observe(caller, event(MediaTelemetry.Event.MEDIA_FAILED, 1, 2, 2)).action())
        .isEqualTo(MediaRecoveryPolicy.Action.TERMINALIZE);
  }

  @Test
  void bothMediaObservationsMustMatchCommittedRoundAndWinner() {
    var policy = policy("CONNECTING");
    assertThat(
            policy.observe(caller, event(MediaTelemetry.Event.MEDIA_CONNECTED, 1, 1, 1)).action())
        .isEqualTo(MediaRecoveryPolicy.Action.CONFIRM_MEDIA);
    assertThat(
            policy.observe(winner, event(MediaTelemetry.Event.MEDIA_CONNECTED, 1, 1, 2)).action())
        .isEqualTo(MediaRecoveryPolicy.Action.STALE_GENERATION);
    assertThat(
            policy
                .observe(session("intruder"), event(MediaTelemetry.Event.MEDIA_CONNECTED, 1, 1, 1))
                .action())
        .isEqualTo(MediaRecoveryPolicy.Action.UNAUTHORIZED);
    assertThat(
            policy.observe(winner, event(MediaTelemetry.Event.MEDIA_CONNECTED, 1, 1, 1)).action())
        .isEqualTo(MediaRecoveryPolicy.Action.CONFIRM_BOTH_MEDIA);
    assertThat(
            policy.observe(winner, event(MediaTelemetry.Event.MEDIA_CONNECTED, 1, 1, 1)).action())
        .isEqualTo(MediaRecoveryPolicy.Action.DUPLICATE);
  }

  @Test
  void successfulMediaRecoveryCancelsOnlyTheMediaFailureWindow() {
    var policy = policy("ESTABLISHED");
    policy.observe(caller, event(MediaTelemetry.Event.MEDIA_DISCONNECTED, 1, 1, 1));
    now.set(Duration.ofSeconds(2).toNanos());
    assertThat(
            policy.observe(caller, event(MediaTelemetry.Event.MEDIA_RECOVERED, 2, 1, 1)).action())
        .isEqualTo(MediaRecoveryPolicy.Action.CONTINUE_MEDIA);
    now.set(Duration.ofSeconds(60).toNanos());
    assertThat(policy.tick().action()).isEqualTo(MediaRecoveryPolicy.Action.NONE);
  }

  CallCommand resume(AuthenticatedSession sender, SignalEnvelope.Type type) {
    return new CallCommand(
        type,
        sender,
        new RequestId(UUID.randomUUID()),
        call,
        CommandScope.call(call),
        null,
        null,
        null,
        "{}",
        "a".repeat(64));
  }

  ResumeHandler.Snapshot snapshot(AuthenticatedSession rebound, boolean retained, String state) {
    return new ResumeHandler.Snapshot(
        call, 9, state, activation, rebound, winner, 1, 1, retained, Instant.now().plusSeconds(75));
  }

  @Test
  void resumeWaitsForNativeRebindAndPreservesHealthyEstablishedMedia() {
    var newer =
        new AuthenticatedSession(
            caller.userId(), caller.key(), caller.incarnation(), 2, UUID.randomUUID());
    var nativeResult = new CompletableFuture<ResumeHandler.Snapshot>();
    var handler = new ResumeHandler((command, budget) -> nativeResult, Clock.systemUTC());
    var response =
        handler.handle(resume(newer, SignalEnvelope.Type.RESUME), true, Duration.ofSeconds(2));
    assertThat(response.toCompletableFuture()).isNotDone();
    nativeResult.complete(snapshot(newer, false, "ESTABLISHED"));
    var result = response.toCompletableFuture().join();
    assertThat(result.code()).isEqualTo("CONTINUE_MEDIA");
    assertThat(result.resetPeerConnection()).isFalse();
    assertThat(result.snapshot().caller()).isEqualTo(newer);
  }

  @Test
  void volatileSdpLossReturnsResyncAndTerminalSyncNeverReplaysAnOldDescription() {
    var handler =
        new ResumeHandler(
            (command, budget) ->
                CompletableFuture.completedFuture(snapshot(caller, false, "CONNECTING")),
            Clock.systemUTC());
    var result =
        handler
            .handle(resume(caller, SignalEnvelope.Type.SYNC_CALL), false, Duration.ofSeconds(2))
            .toCompletableFuture()
            .join();
    assertThat(result.code()).isEqualTo("RESYNC_REQUIRED");
    assertThat(result.resetPeerConnection()).isFalse();
    var terminal =
        new ResumeHandler(
            (command, budget) ->
                CompletableFuture.completedFuture(snapshot(caller, false, "TERMINAL")),
            Clock.systemUTC());
    assertThat(
            terminal
                .handle(resume(caller, SignalEnvelope.Type.SYNC_CALL), false, Duration.ofSeconds(2))
                .toCompletableFuture()
                .join()
                .code())
        .isEqualTo("TERMINAL");
  }

  @Test
  void anOldRouteCannotReceiveSuccessfulResumeForTheReboundGeneration() {
    var newer =
        new AuthenticatedSession(
            caller.userId(), caller.key(), caller.incarnation(), 2, UUID.randomUUID());
    var handler =
        new ResumeHandler(
            (command, budget) ->
                CompletableFuture.completedFuture(snapshot(newer, true, "ESTABLISHED")),
            Clock.systemUTC());
    assertThatThrownBy(
            () ->
                handler
                    .handle(resume(caller, SignalEnvelope.Type.RESUME), true, Duration.ofSeconds(2))
                    .toCompletableFuture()
                    .join())
        .hasCauseInstanceOf(IllegalStateException.class);
  }

  @Test
  void terminalSnapshotRemainsReadableAfterCurrentAuthenticationReplacesOldRoute() {
    var current =
        new AuthenticatedSession(
            caller.userId(), caller.key(), caller.incarnation(), 2, UUID.randomUUID());
    var terminal =
        new ResumeHandler(
            (command, budget) ->
                CompletableFuture.completedFuture(snapshot(caller, false, "TERMINAL")),
            Clock.systemUTC());
    assertThat(
            terminal
                .handle(
                    resume(current, SignalEnvelope.Type.SYNC_CALL), false, Duration.ofSeconds(2))
                .toCompletableFuture()
                .join()
                .code())
        .isEqualTo("TERMINAL");
  }
}
