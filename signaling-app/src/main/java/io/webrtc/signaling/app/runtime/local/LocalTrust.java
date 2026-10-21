package io.webrtc.signaling.app.runtime.local;

import io.webrtc.signaling.app.runtime.NativeActorSourceEnrollment;
import io.webrtc.signaling.app.runtime.local.LocalRuntimeFailure.Category;
import io.webrtc.signaling.app.runtime.local.LocalRuntimeFailure.Role;
import io.webrtc.signaling.auth.BoundedTokenVerifier;
import io.webrtc.signaling.auth.ClockSafetyMonitor;
import io.webrtc.signaling.auth.Rs256TokenVerifier;
import io.webrtc.signaling.auth.TrustedRsaKeys;
import io.webrtc.signaling.rpc.HomeAuthorizationProof;
import io.webrtc.signaling.rpc.RelaySessionAuthorizationProof;

import javax.net.ssl.SSLContext;
import java.net.URI;
import java.security.GeneralSecurityException;
import java.security.KeyFactory;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.interfaces.EdECPublicKey;
import java.security.interfaces.RSAPublicKey;
import java.security.spec.PKCS8EncodedKeySpec;
import java.security.spec.X509EncodedKeySpec;
import java.time.Clock;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Token, proof and source-trust owners for one LOCAL_TEST_ONLY container. Keys are PKCS#8 PEM
 * (private) and X.509 PEM (public) files named by the mounted contract and opened only through
 * {@link LocalRuntimeContract#resolve(String)}. Constructors are the existing ones; no cryptography
 * lives here. Failures carry a category only, never a path, key material or a cause.
 */
public final class LocalTrust {
    private LocalTrust() {
    }

    /** RS256 verification for the contract's JWT keys, bounded to one worker and eight queued. */
    public static BoundedTokenVerifier tokens(LocalRuntimeContract c) {
        var keys = new LinkedHashMap<String, RSAPublicKey>();
        c.contract().jwtPublicKeys().forEach((kid, path) -> keys.put(kid, rsaPublic(c, path)));
        var verifier =
                new Rs256TokenVerifier(
                        c.contract().identity(),
                        new TrustedRsaKeys(Map.copyOf(keys), null, Duration.ofMinutes(1)),
                        8192);
        return new BoundedTokenVerifier(verifier, 1, 8, Duration.ofSeconds(1));
    }

    /** Signs with this cell's key; trusts exactly the contract's proof keys, named {@code cell/keyId}. */
    public static HomeAuthorizationProof homeProof(LocalRuntimeContract c) {
        var proofs = c.contract().proofs();
        try {
            return new HomeAuthorizationProof(
                    c.contract().cell(),
                    proofs.keyId(),
                    ed25519Private(c, proofs.privateKey()),
                    ed25519Keys(c, proofs.publicKeys()));
        } catch (IllegalArgumentException rejected) {
            throw LocalJson.failure(Role.CONTRACT, Category.INVALID);
        }
    }

    /** Relay session proofs over the same signing key and trusted keys as {@link #homeProof}. */
    public static RelaySessionAuthorizationProof relayProof(LocalRuntimeContract c) {
        var proofs = c.contract().proofs();
        try {
            return new RelaySessionAuthorizationProof(
                    c.contract().cell(),
                    proofs.keyId(),
                    ed25519Private(c, proofs.privateKey()),
                    ed25519Keys(c, proofs.publicKeys()));
        } catch (IllegalArgumentException rejected) {
            throw LocalJson.failure(Role.CONTRACT, Category.INVALID);
        }
    }

    /** Public Ed25519 keys by the caller's own ids; any other key type is refused. */
    public static Map<String, PublicKey> ed25519Keys(LocalRuntimeContract c, Map<String, String> paths) {
        var keys = new LinkedHashMap<String, PublicKey>();
        paths.forEach((id, path) -> keys.put(id, ed25519Public(c, path)));
        return Map.copyOf(keys);
    }

    /**
     * Pinned to this pod and this JVM's fresh process boot, verifying the contract's source keys
     * against the real wall and monotonic clocks. It starts invalid until a signed report arrives.
     */
    public static ClockSafetyMonitor clockMonitor(LocalRuntimeContract c) {
        try {
            return new ClockSafetyMonitor(
                    c.contract().cell(),
                    c.contract().storageEpoch(),
                    c.profile().podUid(),
                    c.processBoot(),
                    ed25519Keys(c, c.contract().sources().publicKeys()),
                    Clock.systemUTC(),
                    System::nanoTime);
        } catch (IllegalArgumentException rejected) {
            throw LocalJson.failure(Role.CONTRACT, Category.INVALID);
        }
    }

    /** Pinned HTTPS source endpoint; construction performs no I/O. */
    public static NativeActorSourceEnrollment.Endpoint endpoint(
            String url, SSLContext tls, Map<String, PublicKey> keys) {
        return new NativeActorSourceEnrollment.Endpoint(URI.create(url), tls, keys);
    }

    private static RSAPublicKey rsaPublic(LocalRuntimeContract c, String path) {
        try {
            var der = LocalPem.der(c.resolve(path), "PUBLIC KEY");
            if (KeyFactory.getInstance("RSA").generatePublic(new X509EncodedKeySpec(der))
                    instanceof RSAPublicKey key) return key;
        } catch (GeneralSecurityException | RuntimeException invalid) {
            if (invalid instanceof LocalRuntimeFailure failure) throw failure;
        }
        throw LocalJson.failure(Role.CONTRACT, Category.INVALID);
    }

    private static PublicKey ed25519Public(LocalRuntimeContract c, String path) {
        try {
            var der = LocalPem.der(c.resolve(path), "PUBLIC KEY");
            var key = KeyFactory.getInstance("Ed25519").generatePublic(new X509EncodedKeySpec(der));
            if (key instanceof EdECPublicKey) return key;
        } catch (GeneralSecurityException | RuntimeException invalid) {
            if (invalid instanceof LocalRuntimeFailure failure) throw failure;
        }
        throw LocalJson.failure(Role.CONTRACT, Category.INVALID);
    }

    private static PrivateKey ed25519Private(LocalRuntimeContract c, String path) {
        try {
            var der = LocalPem.der(c.resolve(path), "PRIVATE KEY");
            return KeyFactory.getInstance("Ed25519").generatePrivate(new PKCS8EncodedKeySpec(der));
        } catch (GeneralSecurityException invalid) {
            throw LocalJson.failure(Role.CONTRACT, Category.INVALID);
        }
    }
}
