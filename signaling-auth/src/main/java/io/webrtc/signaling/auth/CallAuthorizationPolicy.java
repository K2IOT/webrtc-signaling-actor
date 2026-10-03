package io.webrtc.signaling.auth;
import java.time.Duration;
import java.util.concurrent.*;
import java.util.function.Function;
public interface CallAuthorizationPolicy {
    CompletionStage<AuthorizationDecision> authorize(CallAuthorizationRequest request);
    static CallAuthorizationPolicy denyAll(){return r->CompletableFuture.completedFuture(AuthorizationDecision.denied());}
    static CallAuthorizationPolicy openAuthenticated(String version,Duration maximumAge){if(version==null||version.isBlank()||maximumAge.isNegative()||maximumAge.isZero())throw new IllegalArgumentException("explicit policy required");return r->CompletableFuture.completedFuture(new AuthorizationDecision(r.caller().expiresAt().isAfter(r.now())&&!r.caller().userId().equals(r.target()),version,r.now().plus(maximumAge).isBefore(r.caller().expiresAt())?r.now().plus(maximumAge):r.caller().expiresAt()));}
    static CallAuthorizationPolicy guarded(Function<CallAuthorizationRequest,CompletionStage<AuthorizationDecision>> dependency){return r->{try{return dependency.apply(r).handle((d,e)->e==null&&d!=null&&d.allowed()&&d.expiresAt().isAfter(r.now())?d:AuthorizationDecision.denied());}catch(RuntimeException e){return CompletableFuture.completedFuture(AuthorizationDecision.denied());}};}
}
