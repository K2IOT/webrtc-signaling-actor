package io.webrtc.signaling.auth;
import io.webrtc.signaling.protocol.Identity.*;
import java.time.*;
import java.util.Objects;
public final class RevocationState {
    public record Event(String issuer,UserId userId,String jti,long epoch,long sourceOffset,Instant committedAt){public Event{new SessionKey(issuer,jti==null?"user-scope":jti);Objects.requireNonNull(userId);Objects.requireNonNull(committedAt);if(epoch<0||sourceOffset<0)throw new IllegalArgumentException("security epoch");}}
    public record Progress(long offset,Instant checkedAt){}
    /** Implementations commit atomically before returning; runtime storage supplies a PostgreSQL adapter. */
    public interface Store {boolean apply(Event event);long epoch(String issuer,UserId user,String jti);Progress progress();void reconcile(long offset,Instant checkedAt);}
    private final Store store;private final Duration freshness;
    public RevocationState(Store store,Duration freshness){this.store=Objects.requireNonNull(store);if(freshness==null||freshness.isNegative()||freshness.isZero())throw new IllegalArgumentException("freshness bound");this.freshness=freshness;}
    public boolean apply(Event event){return store.apply(event);}
    public void markReconciled(long offset,Instant now){store.reconcile(offset,now);}
    public AuthorizationStatus checkRevocation(AuthPrincipal principal,Instant now){
        if(!principal.expiresAt().isAfter(now))return AuthorizationStatus.TOKEN_EXPIRED;
        if(store.epoch(principal.key().issuer(),principal.userId(),principal.key().jti())>=principal.securityEpoch()||store.epoch(principal.key().issuer(),principal.userId(),null)>=principal.securityEpoch())return AuthorizationStatus.REVOKED;
        var p=store.progress();if(p==null||p.checkedAt().isAfter(now)||Duration.between(p.checkedAt(),now).compareTo(freshness)>0)return AuthorizationStatus.FRESHNESS_UNKNOWN;
        return AuthorizationStatus.ALLOWED;
    }
}
