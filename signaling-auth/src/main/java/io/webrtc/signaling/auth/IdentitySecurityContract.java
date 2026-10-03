package io.webrtc.signaling.auth;
import io.webrtc.signaling.protocol.Identity.SessionKey;
import java.time.Duration;
public record IdentitySecurityContract(String issuer,String audience,Duration maximumJwtLifetime,Duration clockSkew,
        Duration revocationPropagationSLO,Duration hardSafetyBound,boolean preserveJtiOnRefresh,String highWaterSource) {
    public IdentitySecurityContract {
        new SessionKey(issuer,"contract");new SessionKey(audience,"contract");new SessionKey(highWaterSource,"contract");
        if(issuer.contains("${")||audience.contains("${")||highWaterSource.contains("${"))throw new IllegalArgumentException("identity contract unresolved");
        for(var d:new Duration[]{maximumJwtLifetime,revocationPropagationSLO,hardSafetyBound})if(d==null||d.isNegative()||d.isZero())throw new IllegalArgumentException("identity contract requires positive bounds");
        if(clockSkew==null||clockSkew.isNegative()||clockSkew.compareTo(Duration.ofSeconds(30))>0||revocationPropagationSLO.compareTo(hardSafetyBound)>=0||hardSafetyBound.compareTo(maximumJwtLifetime)>=0||!preserveJtiOnRefresh)
            throw new IllegalArgumentException("unsafe identity security contract; new-jti refresh requires a separate approved replacement contract");
    }
}
