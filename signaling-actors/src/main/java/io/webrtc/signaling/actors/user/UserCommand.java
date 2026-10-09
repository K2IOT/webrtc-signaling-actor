package io.webrtc.signaling.actors.user;

import io.webrtc.signaling.actors.cluster.*;
import io.webrtc.signaling.auth.AuthPrincipal;
import io.webrtc.signaling.protocol.Identity.*;
import io.webrtc.signaling.storage.*;
import io.webrtc.signaling.storage.HomeParticipationService.*;
import java.time.Instant;
import java.util.*;
import org.apache.pekko.actor.typed.ActorRef;

public interface UserCommand extends UserMessage {
  enum Code {
    PROOF_READ,
    REGISTERED,
    REFRESHED,
    CLOSED,
    RESERVED,
    RENEWED,
    RELEASED,
    CLAIMED,
    CONFIRMED,
    ANSWERED_ELSEWHERE,
    TERMINAL,
    STALE_BINDING,
    USER_BUSY,
    OVERLOADED,
    EXPIRED,
    UNKNOWN,
    UNAVAILABLE,
    BINDING_REJECTED,
    INTENT_CONFLICT,
    INVALID
  }

  record Result(
      Code code,
      SessionRepository.Route route,
      Participation participation,
      AcceptWinnerService.Claim winner,
      HomeActivationService.Confirmation confirmation,
      HomeProofReadService.View proofView)
      implements io.webrtc.signaling.protocol.ApplicationSerializable {
    public Result {
      Objects.requireNonNull(code);
    }

    public Result(
        Code code,
        SessionRepository.Route route,
        Participation participation,
        AcceptWinnerService.Claim winner) {
      this(code, route, participation, winner, null, null);
    }

    public Result(
        Code code,
        SessionRepository.Route route,
        Participation participation,
        AcceptWinnerService.Claim winner,
        HomeActivationService.Confirmation confirmation) {
      this(code, route, participation, winner, confirmation, null);
    }

    public static Result error(Code code) {
      return new Result(code, null, null, null);
    }
  }

  @com.fasterxml.jackson.annotation.JsonTypeInfo(
      use = com.fasterxml.jackson.annotation.JsonTypeInfo.Id.NAME,
      property = "kind")
  @com.fasterxml.jackson.annotation.JsonSubTypes({
    @com.fasterxml.jackson.annotation.JsonSubTypes.Type(value = Register.class, name = "register"),
    @com.fasterxml.jackson.annotation.JsonSubTypes.Type(value = Refresh.class, name = "refresh"),
    @com.fasterxml.jackson.annotation.JsonSubTypes.Type(value = Close.class, name = "close"),
    @com.fasterxml.jackson.annotation.JsonSubTypes.Type(value = Reserve.class, name = "reserve"),
    @com.fasterxml.jackson.annotation.JsonSubTypes.Type(value = Renew.class, name = "renew"),
    @com.fasterxml.jackson.annotation.JsonSubTypes.Type(value = Release.class, name = "release"),
    @com.fasterxml.jackson.annotation.JsonSubTypes.Type(value = Accept.class, name = "accept"),
    @com.fasterxml.jackson.annotation.JsonSubTypes.Type(value = Activate.class, name = "activate"),
    @com.fasterxml.jackson.annotation.JsonSubTypes.Type(
        value = QueryProof.class,
        name = "query-proof")
  })
  sealed interface Operation
      permits Register, Refresh, Close, Reserve, Renew, Release, Accept, Activate, QueryProof {
    UserId user();

    long directoryEpoch();
  }

  record Register(
      AuthPrincipal principal,
      GatewayLeaseRepository.Boot boot,
      UUID connection,
      long directoryEpoch)
      implements Operation {
    public UserId user() {
      return principal.userId();
    }
  }

  record Refresh(SessionRepository.Route route, AuthPrincipal principal, long directoryEpoch)
      implements Operation {
    public UserId user() {
      return route.user();
    }
  }

  record Close(SessionRepository.Route route, long directoryEpoch) implements Operation {
    public UserId user() {
      return route.user();
    }
  }

  record QueryProof(Request request, AuthenticatedSession sender) implements Operation {
    public UserId user() {
      return request.user();
    }

    public long directoryEpoch() {
      return request.directoryEpoch();
    }
  }

  record Reserve(Request request) implements Operation {
    public UserId user() {
      return request.user();
    }

    public long directoryEpoch() {
      return request.directoryEpoch();
    }
  }

  record Renew(Request request, UUID reservation, long version) implements Operation {
    public UserId user() {
      return request.user();
    }

    public long directoryEpoch() {
      return request.directoryEpoch();
    }
  }

  record Release(Request request, UUID reservation, long version) implements Operation {
    public UserId user() {
      return request.user();
    }

    public long directoryEpoch() {
      return request.directoryEpoch();
    }
  }

  record Accept(Request request, UUID reservation, SessionRepository.Route route)
      implements Operation {
    public UserId user() {
      return request.user();
    }

    public long directoryEpoch() {
      return request.directoryEpoch();
    }
  }

  record Activate(
      Request request,
      UUID reservation,
      long version,
      UUID activation,
      long callVersion,
      Winner winner,
      UUID operation)
      implements Operation {
    public UserId user() {
      return request.user();
    }

    public long directoryEpoch() {
      return request.directoryEpoch();
    }
  }

  record Mutate(
      Operation operation,
      ActorRef<Result> replyTo,
      Instant deadline,
      int encodedBytes,
      io.webrtc.signaling.actors.admission.CompletionReceipt receipt)
      implements UserCommand {
    public Mutate(
        Operation operation, ActorRef<Result> replyTo, Instant deadline, int encodedBytes) {
      this(operation, replyTo, deadline, encodedBytes, null);
    }

    public Mutate {
      Objects.requireNonNull(operation);
      Objects.requireNonNull(replyTo);
      Objects.requireNonNull(deadline);
      if (encodedBytes < 1 || encodedBytes > 96 * 1024)
        throw new IllegalArgumentException("Invalid envelope budget");
    }
  }

  record GetState(ActorRef<UserState> replyTo) implements UserCommand {
    public GetState {
      Objects.requireNonNull(replyTo);
    }
  }

  enum Stop implements UserCommand {
    INSTANCE
  }
}
