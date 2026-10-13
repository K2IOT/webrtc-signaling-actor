package io.webrtc.signaling.identity;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.webrtc.signaling.auth.IdentitySecurityContract;
import io.webrtc.signaling.auth.Rs256TokenVerifier;
import io.webrtc.signaling.auth.TrustedRsaKeys;
import org.junit.jupiter.api.Test;
import org.springframework.core.io.ClassPathResource;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class JwtTokenIssuerTest {
    private static final Instant NOW = Instant.parse("2026-10-13T00:00:00Z");
    private final ObjectMapper mapper = new ObjectMapper();

    private static IdentityProperties properties() {
        return new IdentityProperties(
                "http://localhost:8081",
                "webrtc-signaling",
                Duration.ofMinutes(15),
                "test-key-1",
                List.of(new IdentityProperties.Account("alice", "alice123", "alice")));
    }

    private static JsonNode decode(String token, int index) throws Exception {
        return new ObjectMapper().readTree(Base64.getUrlDecoder().decode(token.split("\\.")[index]));
    }

    private static Rs256TokenVerifier verifier(IdentityProperties properties, RsaKeyMaterial keys) {
        return new Rs256TokenVerifier(
                new IdentitySecurityContract(
                        properties.issuer(),
                        properties.audience(),
                        properties.tokenTtl(),
                        Duration.ZERO,
                        Duration.ofSeconds(2),
                        Duration.ofSeconds(5),
                        true,
                        "test://ordered-security-log"),
                new TrustedRsaKeys(
                        Map.of(properties.keyId(), keys.publicKey()), null, Duration.ofSeconds(1)),
                8192);
    }

    @Test
    void issuesVerifiableClaimsWithUniqueIdsAndConfiguredLifetime() throws Exception {
        var properties = properties();
        var keys = keys(properties);
        var issuer = issuer(properties, keys);
        String first = issuer.issue("alice");
        String second = issuer.issue("alice");

        JsonNode header = decode(first, 0);
        JsonNode claims = decode(first, 1);
        assertThat(header.path("alg").asText()).isEqualTo("RS256");
        assertThat(header.path("kid").asText()).isEqualTo("test-key-1");
        assertThat(header.path("typ").asText()).isEqualTo("JWT");
        assertThat(claims.path("iss").asText()).isEqualTo("http://localhost:8081");
        assertThat(claims.path("aud").asText()).isEqualTo("webrtc-signaling");
        assertThat(claims.path("userId").asText()).isEqualTo("alice");
        assertThat(claims.path("iat").asLong()).isEqualTo(NOW.getEpochSecond());
        assertThat(claims.path("exp").asLong() - claims.path("iat").asLong()).isEqualTo(900);
        assertThat(java.util.UUID.fromString(claims.path("jti").asText())).isNotNull();
        assertThat(decode(second, 1).path("jti").asText()).isNotEqualTo(claims.path("jti").asText());

        var verifier = verifier(properties, keys);
        assertThat(verifier.validate(first, NOW).userId().value()).isEqualTo("alice");
        String[] parts = first.split("\\.");
        String modifiedPayload =
                Base64.getUrlEncoder()
                        .withoutPadding()
                        .encodeToString(parts[1].replace("alice", "bob").getBytes(StandardCharsets.UTF_8));
        assertThatThrownBy(
                () -> verifier.validate(parts[0] + "." + modifiedPayload + "." + parts[2], NOW))
                .isInstanceOf(RuntimeException.class);
        assertThatThrownBy(() -> verifier.validate(first, NOW.plusSeconds(900)))
                .isInstanceOf(RuntimeException.class);
    }

    @Test
    void signsConcurrentTokensWithIndependentSignatureInstances() throws Exception {
        var keys = keys(properties());
        var tokenIssuer = issuer(properties(), keys);
        try (var executor = Executors.newFixedThreadPool(8)) {
            var tokens =
                    java.util.stream.IntStream.range(0, 32)
                            .mapToObj(i -> executor.submit(() -> tokenIssuer.issue("alice")))
                            .map(
                                    future -> {
                                        try {
                                            return future.get();
                                        } catch (Exception e) {
                                            throw new RuntimeException(e);
                                        }
                                    })
                            .toList();
            Set<String> jtis = ConcurrentHashMap.newKeySet();
            for (String token : tokens) {
                assertThat(keys.publicKey().getModulus().bitLength()).isGreaterThanOrEqualTo(2048);
                var parts = token.split("\\.");
                var signature = java.security.Signature.getInstance("SHA256withRSA");
                signature.initVerify(keys.publicKey());
                signature.update((parts[0] + "." + parts[1]).getBytes(StandardCharsets.US_ASCII));
                assertThat(signature.verify(Base64.getUrlDecoder().decode(parts[2]))).isTrue();
                assertThat(jtis.add(decode(token, 1).path("jti").asText())).isTrue();
            }
        }
    }

    private RsaKeyMaterial keys(IdentityProperties properties) {
        return RsaKeyMaterial.load(
                new ClassPathResource("keys/private.pem"),
                new ClassPathResource("keys/public.pem"),
                properties.keyId());
    }

    private JwtTokenIssuer issuer(IdentityProperties properties, RsaKeyMaterial keys) {
        return new JwtTokenIssuer(properties, keys, mapper, Clock.fixed(NOW, ZoneOffset.UTC));
    }
}
