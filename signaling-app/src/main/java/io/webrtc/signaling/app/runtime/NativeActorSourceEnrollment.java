package io.webrtc.signaling.app.runtime;

import io.webrtc.signaling.auth.IdentitySecurityContract;
import java.net.URI;
import java.security.PublicKey;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import javax.net.ssl.SSLContext;

/** Explicit deployment enrollment; no endpoints, trust keys or identity bounds are inferred. */
public record NativeActorSourceEnrollment(String cell, long storageEpoch, UUID podUid, UUID processBoot,
        IdentitySecurityContract identity, Endpoint clock, Endpoint revocations) {
    public NativeActorSourceEnrollment {
        if(cell==null||!cell.matches("[a-z][a-z0-9-]{0,23}")||storageEpoch<1||podUid==null||processBoot==null
                ||identity==null||clock==null||revocations==null
                ||identity.hardSafetyBound().compareTo(Duration.ofMillis(200))<0)
            throw new IllegalArgumentException("Invalid native source enrollment");
    }
    public record Endpoint(URI uri, SSLContext tls, Map<String, PublicKey> keys) {
        public Endpoint {
            // Reuse the transport's canonical validation. Construction performs no I/O.
            if(tls==null||keys==null||keys.isEmpty()||keys.size()>256)
                throw new IllegalArgumentException("Explicit native source trust required");
            new NativeSourceHttp(uri,tls,new UUID(0,0),new UUID(0,0));
            keys=Map.copyOf(keys);
        }
    }
}
