package io.webrtc.signaling.identity;

import static org.assertj.core.api.Assertions.*;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.webrtc.signaling.auth.IdentitySecurityContract;
import io.webrtc.signaling.auth.Rs256TokenVerifier;
import io.webrtc.signaling.auth.TrustedRsaKeys;
import java.math.BigInteger;
import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.util.Base64;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class IdentityHttpTest {
  @Autowired private TestRestTemplate http;
  private final ObjectMapper mapper = new ObjectMapper();

  @Test
  void loginAndJwksExposeCompatibleSignedToken() throws Exception {
    var response =
        http.postForEntity("/api/v1/auth/login", json("alice", "alice123"), String.class);
    assertThat(response.getStatusCode().value()).isEqualTo(200);
    assertThat(response.getHeaders().getCacheControl()).contains("no-store");
    JsonNode body = mapper.readTree(response.getBody());
    assertThat(body.path("token_type").asText()).isEqualTo("Bearer");
    assertThat(body.path("expires_in").asLong()).isEqualTo(900);
    String token = body.path("access_token").asText();
    String[] parts = token.split("\\.");
    JsonNode header = mapper.readTree(Base64.getUrlDecoder().decode(parts[0]));
    JsonNode claims = mapper.readTree(Base64.getUrlDecoder().decode(parts[1]));
    assertThat(header.path("alg").asText()).isEqualTo("RS256");
    assertThat(header.path("kid").asText()).isEqualTo("test-key-1");
    assertThat(claims.path("userId").asText()).isEqualTo("alice");

    var bobResponse =
        http.postForEntity("/api/v1/auth/login", json("bob", "bob123"), JsonNode.class);
    assertThat(bobResponse.getStatusCode().value()).isEqualTo(200);
    String bobToken = bobResponse.getBody().path("access_token").asText();
    assertThat(
            mapper
                .readTree(Base64.getUrlDecoder().decode(bobToken.split("\\.")[1]))
                .path("userId")
                .asText())
        .isEqualTo("bob");

    var jwksResponse = http.getForEntity("/.well-known/jwks.json", JsonNode.class);
    assertThat(jwksResponse.getStatusCode().value()).isEqualTo(200);
    JsonNode jwk = jwksResponse.getBody().path("keys").get(0);
    assertThat(jwk.path("kid").asText()).isEqualTo(header.path("kid").asText());
    var publicKey =
        java.security.KeyFactory.getInstance("RSA")
            .generatePublic(
                new java.security.spec.RSAPublicKeySpec(
                    new BigInteger(1, Base64.getUrlDecoder().decode(jwk.path("n").asText())),
                    new BigInteger(1, Base64.getUrlDecoder().decode(jwk.path("e").asText()))));
    var verifier =
        new Rs256TokenVerifier(
            new IdentitySecurityContract(
                claims.path("iss").asText(),
                claims.path("aud").asText(),
                Duration.ofMinutes(15),
                Duration.ofSeconds(30),
                Duration.ofSeconds(2),
                Duration.ofSeconds(5),
                true,
                "test://ordered-security-log"),
            new TrustedRsaKeys(
                Map.of(
                    header.path("kid").asText(), (java.security.interfaces.RSAPublicKey) publicKey),
                null,
                Duration.ofSeconds(1)),
            8192);
    assertThat(verifier.validate(token, Instant.now()).userId().value()).isEqualTo("alice");
  }

  @Test
  void rejectsBadCredentialsUniformlyAndMalformedRequests() {
    var wrongPassword =
        http.postForEntity("/api/v1/auth/login", json("alice", "bad"), String.class);
    var unknownUser =
        http.postForEntity("/api/v1/auth/login", json("missing", "bad"), String.class);
    assertThat(wrongPassword.getStatusCode().value()).isEqualTo(401);
    assertThat(unknownUser.getStatusCode().value()).isEqualTo(401);
    assertThat(wrongPassword.getBody()).isEqualTo(unknownUser.getBody());

    for (String request :
        new String[] {
          "{}", "{\"username\":\" \",\"password\":\"x\"}",
          "{\"username\":null,\"password\":\"x\"}", "not-json"
        }) {
      var response = http.postForEntity("/api/v1/auth/login", rawJson(request), String.class);
      assertThat(response.getStatusCode().value()).isEqualTo(400);
      assertThat(response.getBody()).contains("invalid_request");
    }
    var padded =
        http.postForEntity("/api/v1/auth/login", json("alice", " alice123 "), String.class);
    assertThat(padded.getStatusCode().value()).isEqualTo(401);
  }

  @Test
  void rejectsUnsupportedMediaType() {
    var headers = new HttpHeaders();
    headers.setContentType(MediaType.TEXT_PLAIN);
    var response =
        http.exchange(
            URI.create("/api/v1/auth/login"),
            HttpMethod.POST,
            new HttpEntity<>("alice alice123", headers),
            String.class);
    assertThat(response.getStatusCode().value()).isEqualTo(415);
  }

  private static HttpEntity<Map<String, String>> json(String username, String password) {
    return new HttpEntity<>(Map.of("username", username, "password", password));
  }

  private static HttpEntity<String> rawJson(String body) {
    var headers = new HttpHeaders();
    headers.setContentType(MediaType.APPLICATION_JSON);
    return new HttpEntity<>(body, headers);
  }
}
