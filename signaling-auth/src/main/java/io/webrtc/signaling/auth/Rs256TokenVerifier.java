package io.webrtc.signaling.auth;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.StreamReadConstraints;
import com.fasterxml.jackson.core.StreamReadFeature;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.webrtc.signaling.protocol.Identity.SessionKey;
import io.webrtc.signaling.protocol.Identity.UserId;

import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.security.Signature;
import java.time.Duration;
import java.time.Instant;
import java.util.Base64;
import java.util.Objects;

/**
 * CPU-bound verifier; gateway/control callers use BoundedTokenVerifier rather than run this on
 * event loops.
 */
public final class Rs256TokenVerifier implements TokenVerifier {
    private final IdentitySecurityContract contract;
    private final TrustedRsaKeys keys;
    private final int maxBytes;
    private final ObjectMapper json =
            new ObjectMapper(
                    JsonFactory.builder()
                            .enable(StreamReadFeature.STRICT_DUPLICATE_DETECTION)
                            .streamReadConstraints(
                                    StreamReadConstraints.builder()
                                            .maxNestingDepth(6)
                                            .maxStringLength(8192)
                                            .maxNumberLength(20)
                                            .build())
                            .build())
                    .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS);

    public Rs256TokenVerifier(IdentitySecurityContract contract, TrustedRsaKeys keys, int maxBytes) {
        this.contract = Objects.requireNonNull(contract);
        this.keys = Objects.requireNonNull(keys);
        if (maxBytes <= 0 || maxBytes > 8192) throw new IllegalArgumentException("JWT limit");
        this.maxBytes = maxBytes;
    }

    @Override
    public AuthPrincipal validate(String token, Instant now) {
        try {
            if (token == null
                    || token.length() > maxBytes
                    || !token.matches("[A-Za-z0-9_-]+\\.[A-Za-z0-9_-]+\\.[A-Za-z0-9_-]+"))
                throw new AuthException();
            var parts = token.split("\\.");
            var header = json.readTree(utf8(decode(parts[0])));
            if (!header.isObject()
                    || !header.path("alg").isTextual()
                    || !"RS256".equals(header.path("alg").textValue())
                    || header.has("jku")
                    || header.has("x5u")
                    || header.has("crit")) throw new AuthException();
            String kid = header.has("kid") ? bounded(header, "kid", 256) : null;
            var rsa = keys.lookup(kid, now).orElseThrow(AuthException::new);
            var verifier = Signature.getInstance("SHA256withRSA");
            verifier.initVerify(rsa);
            verifier.update((parts[0] + "." + parts[1]).getBytes(StandardCharsets.US_ASCII));
            if (!verifier.verify(decode(parts[2]))) throw new AuthException();
            var claims = json.readTree(utf8(decode(parts[1])));
            if (!claims.isObject()) throw new AuthException();
            String issuer = bounded(claims, "iss", 512), jti = bounded(claims, "jti", 256);
            var user = new UserId(bounded(claims, "userId", 256));
            if (!issuer.equals(contract.issuer())) throw new AuthException();
            var aud = claims.path("aud");
            boolean audience = aud.isTextual() && contract.audience().equals(aud.textValue());
            if (aud.isArray() && aud.size() <= 16)
                for (var a : aud) audience |= a.isTextual() && contract.audience().equals(a.textValue());
            if (!audience) throw new AuthException();
            Instant issued = claims.has("iat") ? Instant.ofEpochSecond(number(claims, "iat")) : null;
            Instant expires = Instant.ofEpochSecond(number(claims, "exp"));
            if (!now.minus(contract.clockSkew()).isBefore(expires)) throw new AuthException();
            if (issued != null) {
                if (!expires.isAfter(issued)
                        || Duration.between(issued, expires).compareTo(contract.maximumJwtLifetime()) > 0
                        || issued.isAfter(now.plus(contract.clockSkew()))) throw new AuthException();
            } else if (contract.claimLifetimePolicy().mode()
                    != ClaimLifetimePolicy.Mode.OPTIONAL_IAT_ATTESTED_LIFETIME
                    || expires.isAfter(now.plus(contract.maximumJwtLifetime()).plus(contract.clockSkew()))) {
                // The bound on total issuance lifetime is issuer-attested, not inferred from exp-now.
                throw new AuthException();
            }
            if (claims.has("nbf")
                    && Instant.ofEpochSecond(number(claims, "nbf")).isAfter(now.plus(contract.clockSkew())))
                throw new AuthException();
            long epoch = claims.has("securityEpoch") ? number(claims, "securityEpoch") : 0;
            if (epoch < 0) throw new AuthException();
            return new AuthPrincipal(
                    user, new SessionKey(issuer, jti), expires, issued, kid == null ? "pinned" : kid, epoch);
        } catch (Exception e) {
            throw new AuthException();
        }
    }

    private byte[] decode(String s) {
        var bytes = Base64.getUrlDecoder().decode(s);
        if (!Base64.getUrlEncoder().withoutPadding().encodeToString(bytes).equals(s))
            throw new AuthException();
        return bytes;
    }

    private String utf8(byte[] b) throws CharacterCodingException {
        return StandardCharsets.UTF_8
                .newDecoder()
                .onMalformedInput(CodingErrorAction.REPORT)
                .onUnmappableCharacter(CodingErrorAction.REPORT)
                .decode(ByteBuffer.wrap(b))
                .toString();
    }

    private String bounded(JsonNode n, String field, int max) {
        var v = n.path(field);
        if (!v.isTextual()) throw new AuthException();
        String s = v.textValue();
        new SessionKey(s, "claim");
        if (s.getBytes(StandardCharsets.UTF_8).length > max) throw new AuthException();
        return s;
    }

    private long number(JsonNode n, String field) {
        var v = n.path(field);
        if (!v.isIntegralNumber() || !v.canConvertToLong()) throw new AuthException();
        return v.longValue();
    }
}
