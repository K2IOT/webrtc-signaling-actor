package io.webrtc.signaling.actors.call;

import io.webrtc.signaling.protocol.*;
import io.webrtc.signaling.protocol.Identity.*;
import java.time.Duration;
import java.util.*;
import java.util.function.LongSupplier;

/** Local scheduling policy proposes native commands; observations alone never commit call state. */
public final class MediaRecoveryPolicy {
  public enum Action {
    NONE,
    WAIT,
    CONTINUE_MEDIA,
    REQUEST_RESTART,
    NEGOTIATION_BUSY,
    TERMINALIZE,
    CONFIRM_MEDIA,
    CONFIRM_BOTH_MEDIA,
    STALE_GENERATION,
    UNAUTHORIZED,
    DUPLICATE
  }

  public record Decision(Action action) {}

  public record CommittedRound(
      CallId call,
      UUID activationId,
      long callVersion,
      String state,
      long negotiationId,
      long iceGeneration,
      AuthenticatedSession caller,
      AuthenticatedSession winner) {
    public CommittedRound {
      Objects.requireNonNull(call);
      Objects.requireNonNull(activationId);
      Objects.requireNonNull(caller);
      Objects.requireNonNull(winner);
      if (callVersion < 1
          || negotiationId < 1
          || iceGeneration < 1
          || !Set.of("CONNECTING", "ESTABLISHED").contains(state)
          || caller.equals(winner))
        throw new IllegalArgumentException("Invalid committed media round");
    }
  }

  private final long debounce, window;
  private final int maximumAttempts;
  private final LongSupplier clock;
  private CommittedRound round;
  private long callerSequence, winnerSequence, lostAt;
  private int attempts;
  private boolean callerConnected, winnerConnected, lost, restartPending, restartActive, terminal;

  public MediaRecoveryPolicy(
      Duration debounce, Duration window, int maximumAttempts, LongSupplier clock) {
    if (debounce == null
        || window == null
        || debounce.isNegative()
        || debounce.isZero()
        || window.compareTo(debounce) <= 0
        || window.compareTo(Duration.ofSeconds(75)) > 0
        || maximumAttempts < 1
        || maximumAttempts > 3) throw new IllegalArgumentException("Invalid bounded media policy");
    this.debounce = debounce.toNanos();
    this.window = window.toNanos();
    this.maximumAttempts = maximumAttempts;
    this.clock = Objects.requireNonNull(clock);
  }

  public synchronized void attach(CommittedRound value) {
    if (round != null
        && (!round.call().equals(value.call()) || value.callVersion() < round.callVersion()))
      throw new IllegalArgumentException("Stale committed round");
    round = value;
    callerSequence = winnerSequence = 0;
    callerConnected = winnerConnected = value.state().equals("ESTABLISHED");
  }

  public synchronized void restartGranted(CommittedRound next) {
    if (round == null
        || !restartPending
        || attempts >= maximumAttempts
        || !round.call().equals(next.call())
        || !round.activationId().equals(next.activationId())
        || next.negotiationId() <= round.negotiationId()
        || next.iceGeneration() <= round.iceGeneration()
        || next.callVersion() <= round.callVersion())
      throw new IllegalStateException("Uncommitted or conflicting restart grant");
    attach(next);
    callerConnected = winnerConnected = false;
    restartPending = false;
    restartActive = true;
    attempts++;
  }

  public synchronized Decision signalingLost(AuthenticatedSession sender) {
    if (!participant(sender)) return decision(Action.UNAUTHORIZED);
    return decision(round.state().equals("ESTABLISHED") ? Action.CONTINUE_MEDIA : Action.WAIT);
  }

  public synchronized Decision observe(AuthenticatedSession sender, MediaTelemetry observation) {
    if (!participant(sender) || !round.call().equals(observation.callId()))
      return decision(Action.UNAUTHORIZED);
    if (round.negotiationId() != observation.negotiationId()
        || round.iceGeneration() != observation.iceGeneration())
      return decision(Action.STALE_GENERATION);
    boolean caller = round.caller().equals(sender);
    long sequence = caller ? callerSequence : winnerSequence;
    if (observation.senderSequence() <= sequence) return decision(Action.DUPLICATE);
    if (caller) callerSequence = observation.senderSequence();
    else winnerSequence = observation.senderSequence();
    if (terminal) return decision(Action.TERMINALIZE);
    return switch (observation.event()) {
      case MEDIA_CONNECTED -> {
        if (caller) callerConnected = true;
        else winnerConnected = true;
        if (callerConnected && winnerConnected) {
          lost = false;
          restartPending = restartActive = false;
          yield decision(Action.CONFIRM_BOTH_MEDIA);
        }
        yield decision(Action.CONFIRM_MEDIA);
      }
      case MEDIA_RECOVERED -> {
        if (caller) callerConnected = true;
        else winnerConnected = true;
        if (callerConnected && winnerConnected) {
          lost = false;
          restartPending = restartActive = false;
          yield decision(Action.CONTINUE_MEDIA);
        }
        yield decision(Action.WAIT);
      }
      case MEDIA_DISCONNECTED -> {
        if (caller) callerConnected = false;
        else winnerConnected = false;
        startWindow();
        yield decision(Action.WAIT);
      }
      case MEDIA_FAILED -> {
        if (caller) callerConnected = false;
        else winnerConnected = false;
        startWindow();
        yield requestRestart();
      }
      case ICE_RESTARTING -> {
        startWindow();
        yield requestRestart();
      }
    };
  }

  private void startWindow() {
    if (!lost) {
      lost = true;
      lostAt = clock.getAsLong();
    }
  }

  private Decision requestRestart() {
    if (restartPending) return decision(Action.NEGOTIATION_BUSY);
    if (restartActive || attempts >= maximumAttempts) {
      terminal = true;
      return decision(Action.TERMINALIZE);
    }
    restartPending = true;
    return decision(Action.REQUEST_RESTART);
  }

  public synchronized Decision tick() {
    if (terminal) return decision(Action.TERMINALIZE);
    if (!lost) return decision(Action.NONE);
    long age = clock.getAsLong() - lostAt;
    if (age < 0 || age >= window) {
      terminal = true;
      return decision(Action.TERMINALIZE);
    }
    if (age < debounce) return decision(Action.WAIT);
    return requestRestart();
  }

  private boolean participant(AuthenticatedSession sender) {
    return round != null && (round.caller().equals(sender) || round.winner().equals(sender));
  }

  private static Decision decision(Action value) {
    return new Decision(value);
  }
}
