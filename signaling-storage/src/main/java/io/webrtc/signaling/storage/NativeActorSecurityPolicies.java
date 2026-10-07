package io.webrtc.signaling.storage;

import io.webrtc.signaling.auth.AuthPrincipal;
import io.webrtc.signaling.protocol.Identity.UserId;
import io.webrtc.signaling.storage.worker.RevocationReconciler;
import java.sql.*;
import java.util.Objects;
import java.util.function.BooleanSupplier;

/** Actor policies execute only inside the caller's already-admitted, guarded native transaction. */
public final class NativeActorSecurityPolicies implements SessionRegistryService.NativeSecurityPolicy,
        HomeProofReadService.SecurityPolicy,AcceptWinnerService.RouteSecurityPolicy {
    private final RevocationReconciler source;
    private final BooleanSupplier trustedClock;
    public NativeActorSecurityPolicies(RevocationReconciler source,BooleanSupplier trustedClock){
        this.source=Objects.requireNonNull(source);this.trustedClock=Objects.requireNonNull(trustedClock);
    }
    @Override public boolean allowed(Connection c,AuthPrincipal principal)throws SQLException{
        Objects.requireNonNull(principal);
        return keyKnown(principal.signingKeyId())&&trustedClock.getAsBoolean()
            &&source.sourceCurrent(c,principal.expiresAt())&&source.allowed(c,principal)&&trustedClock.getAsBoolean();
    }
    @Override public boolean allowed(Connection c,SessionRepository.Route route)throws SQLException{
        Objects.requireNonNull(route);
        return keyKnown(route.signingKeyId())&&trustedClock.getAsBoolean()
            &&source.sourceCurrent(c,route.tokenExpiresAt())&&source.allowedRoute(c,route)&&trustedClock.getAsBoolean();
    }
    /** User-only callbacks establish source freshness; scoped identity permission requires allowed(...). */
    @Override public boolean current(Connection c,UserId user)throws SQLException{
        Objects.requireNonNull(user);
        return trustedClock.getAsBoolean()&&source.sourceCurrent(c)&&trustedClock.getAsBoolean();
    }
    private static boolean keyKnown(String key){return key!=null&&!key.isBlank();}
}
