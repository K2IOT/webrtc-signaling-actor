package io.webrtc.signaling.actors.user;
import io.webrtc.signaling.actors.cluster.*;
import io.webrtc.signaling.auth.AuthPrincipal;
import io.webrtc.signaling.protocol.Identity.*;
import io.webrtc.signaling.storage.*;
import io.webrtc.signaling.storage.HomeParticipationService.*;
import org.apache.pekko.actor.typed.ActorRef;
import java.time.Instant;
import java.util.*;
public interface UserCommand extends UserMessage {
    enum Code {REGISTERED,REFRESHED,CLOSED,RESERVED,RENEWED,RELEASED,CLAIMED,ANSWERED_ELSEWHERE,TERMINAL,STALE_BINDING,USER_BUSY,OVERLOADED,EXPIRED,UNKNOWN,UNAVAILABLE,BINDING_REJECTED,INTENT_CONFLICT,INVALID}
    record Result(Code code,SessionRepository.Route route,Participation participation,AcceptWinnerService.Claim winner) implements CborSerializable {public Result {Objects.requireNonNull(code);}public static Result error(Code code){return new Result(code,null,null,null);}}
    sealed interface Operation permits Register,Refresh,Close,Reserve,Renew,Release,Accept {UserId user();long directoryEpoch();}
    record Register(AuthPrincipal principal,GatewayLeaseRepository.Boot boot,UUID connection,long directoryEpoch) implements Operation {public UserId user(){return principal.userId();}}
    record Refresh(SessionRepository.Route route,AuthPrincipal principal,long directoryEpoch) implements Operation {public UserId user(){return route.user();}}
    record Close(SessionRepository.Route route,long directoryEpoch) implements Operation {public UserId user(){return route.user();}}
    record Reserve(Request request) implements Operation {public UserId user(){return request.user();}public long directoryEpoch(){return request.directoryEpoch();}}
    record Renew(Request request,UUID reservation,long version) implements Operation {public UserId user(){return request.user();}public long directoryEpoch(){return request.directoryEpoch();}}
    record Release(Request request,UUID reservation,long version) implements Operation {public UserId user(){return request.user();}public long directoryEpoch(){return request.directoryEpoch();}}
    record Accept(Request request,UUID reservation,SessionRepository.Route route) implements Operation {public UserId user(){return request.user();}public long directoryEpoch(){return request.directoryEpoch();}}
    record Mutate(Operation operation,ActorRef<Result> replyTo,Instant deadline,int encodedBytes) implements UserCommand {public Mutate {Objects.requireNonNull(operation);Objects.requireNonNull(replyTo);Objects.requireNonNull(deadline);if(encodedBytes<1||encodedBytes>96*1024)throw new IllegalArgumentException("Invalid envelope budget");}}
    record GetState(ActorRef<UserState> replyTo) implements UserCommand {public GetState{Objects.requireNonNull(replyTo);}}
    enum Stop implements UserCommand {INSTANCE}
}
