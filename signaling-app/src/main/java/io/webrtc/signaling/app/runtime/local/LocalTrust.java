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
import java.security.Signature;
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

    /**
     * RS256 verification for the contract's JWT keys, bounded to one worker and eight queued.
     *
     * <p>Ownership: the result owns a thread pool and is {@link AutoCloseable}. The caller owns it
     * and must close it; the plane installer registers it as a bean with {@code destroyMethod}
     * {@code close}, so it is closed with the application context. Every call builds a new one.
     */
    public static BoundedTokenVerifier tokens(LocalRuntimeContract c) {
        return bounded(jwtVerifier(c));
    }

    /**
     * The RS256 verifier over the contract's JWT keys. It owns no thread, so an installer can build
     * it, and fail on a bad key, before any bean exists. Keys the verifier refuses (for example an
     * RSA modulus below 2048 bits) fail as {@code CONTRACT/INVALID}.
     */
    public static Rs256TokenVerifier jwtVerifier(LocalRuntimeContract c) {
        var keys = new LinkedHashMap<String, RSAPublicKey>();
        c.contract().jwtPublicKeys().forEach((kid, path) -> keys.put(kid, rsaPublic(c, path)));
        try {
            return new Rs256TokenVerifier(
                    c.contract().identity(),
                    new TrustedRsaKeys(Map.copyOf(keys), null, Duration.ofMinutes(1)),
                    8192);
        } catch (IllegalArgumentException rejected) {
            throw LocalJson.failure(Role.CONTRACT, Category.INVALID);
        }
    }

    /**
     * Bounds {@code verifier} to one worker and eight queued. The result owns a thread pool; the
     * caller owns it and must close it, as for {@link #tokens}.
     */
    public static BoundedTokenVerifier bounded(Rs256TokenVerifier verifier) {
        return new BoundedTokenVerifier(verifier, 1, 8, Duration.ofSeconds(1));
    }

    /**
     * Signs with this cell's key; trusts exactly the contract's proof keys, named {@code cell/keyId}.
     * Fails with {@code KEY_MISMATCH} when the private key is not the pair of the public key the
     * contract trusts for this cell and key id.
     */
    public static HomeAuthorizationProof homeProof(LocalRuntimeContract c) {
        var keys = proofKeys(c);
        try {
            return new HomeAuthorizationProof(
                    c.contract().cell(), c.contract().proofs().keyId(), keys.signing(), keys.trusted());
        } catch (IllegalArgumentException rejected) {
            throw LocalJson.failure(Role.CONTRACT, Category.INVALID);
        }
    }

    /** Relay session proofs over the same signing key and trusted keys as {@link #homeProof}. */
    public static RelaySessionAuthorizationProof relayProof(LocalRuntimeContract c) {
        var keys = proofKeys(c);
        try {
            return new RelaySessionAuthorizationProof(
                    c.contract().cell(), c.contract().proofs().keyId(), keys.signing(), keys.trusted());
        } catch (IllegalArgumentException rejected) {
            throw LocalJson.failure(Role.CONTRACT, Category.INVALID);
        }
    }

    /**
     * Verify-only relay proofs over the contract's trusted proof public keys. The signing key named
     * by {@code proofs.privateKey} is never opened, so a process that only verifies (the
     * internet-facing gateway) holds no key that could issue a proof.
     */
    public static RelaySessionAuthorizationProof relayVerifier(LocalRuntimeContract c) {
        var trusted = ed25519Keys(c, c.contract().proofs().publicKeys());
        try {
            return new RelaySessionAuthorizationProof(trusted);
        } catch (IllegalArgumentException rejected) {
            throw LocalJson.failure(Role.CONTRACT, Category.INVALID);
        }
    }

    private record ProofKeys(PrivateKey signing, Map<String, PublicKey> trusted) {
    }

    /**
     * Neither proof constructor checks that the signing key matches the trusted public key, so a
     * wrong pair would start up and then fail every proof. A probe signature rejects it here.
     */
    private static ProofKeys proofKeys(LocalRuntimeContract c) {
        var proofs = c.contract().proofs();
        var signing = ed25519Private(c, proofs.privateKey());
        var trusted = ed25519Keys(c, proofs.publicKeys());
        var own = trusted.get(c.contract().cell() + "/" + proofs.keyId());
        try {
            var probe = new byte[32];
            new java.security.SecureRandom().nextBytes(probe);
            var signer = Signature.getInstance("Ed25519");
            signer.initSign(signing);
            signer.update(probe);
            var signature = signer.sign();
            var verifier = Signature.getInstance("Ed25519");
            verifier.initVerify(own);
            verifier.update(probe);
            if (own != null && verifier.verify(signature)) return new ProofKeys(signing, trusted);
        } catch (GeneralSecurityException | RuntimeException mismatch) {
            // Falls through: any failure of the probe is a mismatch, and no detail is kept.
        }
        throw LocalJson.failure(Role.CONTRACT, Category.KEY_MISMATCH);
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

    /** The contract's two signed safety sources, as every plane that polls them enrolls them. */
    public record SourceEndpoints(
            NativeActorSourceEnrollment.Endpoint clock, NativeActorSourceEnrollment.Endpoint revocations) {
    }

    /**
     * Clock and revocation endpoints over the pod's mutual-TLS client identity ({@link
     * LocalTls#jdkMutualClient}), each trusting exactly the contract's source keys. A URL the
     * endpoint refuses is {@code CONTRACT/INVALID}; construction performs no I/O.
     */
    public static SourceEndpoints sourceEndpoints(LocalRuntimeContract c) {
        var sources = c.contract().sources();
        var tls = LocalTls.jdkMutualClient(c);
        var keys = ed25519Keys(c, sources.publicKeys());
        try {
            return new SourceEndpoints(
                    endpoint(sources.clockUrl(), tls, keys), endpoint(sources.revocationUrl(), tls, keys));
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
        var der = LocalPem.der(c.resolve(path), "PUBLIC KEY");
        try {
            if (KeyFactory.getInstance("RSA").generatePublic(new X509EncodedKeySpec(der))
                    instanceof RSAPublicKey key) return key;
        } catch (GeneralSecurityException invalid) {
            // Not an RSA key: reported below without detail.
        }
        throw LocalJson.failure(Role.CONTRACT, Category.INVALID);
    }

    private static PublicKey ed25519Public(LocalRuntimeContract c, String path) {
        var der = LocalPem.der(c.resolve(path), "PUBLIC KEY");
        try {
            var key = KeyFactory.getInstance("Ed25519").generatePublic(new X509EncodedKeySpec(der));
            if (key instanceof EdECPublicKey) return key;
        } catch (GeneralSecurityException invalid) {
            // Not an Ed25519 key: reported below without detail.
        }
        throw LocalJson.failure(Role.CONTRACT, Category.INVALID);
    }

    private static PrivateKey ed25519Private(LocalRuntimeContract c, String path) {
        var der = LocalPem.der(c.resolve(path), "PRIVATE KEY");
        try {
            return KeyFactory.getInstance("Ed25519").generatePrivate(new PKCS8EncodedKeySpec(der));
        } catch (GeneralSecurityException invalid) {
            throw LocalJson.failure(Role.CONTRACT, Category.INVALID);
        }
    }
}
