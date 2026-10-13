package io.webrtc.signaling.identity;

import static org.assertj.core.api.Assertions.*;

import java.nio.charset.StandardCharsets;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.interfaces.RSAPublicKey;
import java.util.Base64;
import java.util.Map;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.core.io.Resource;

class RsaKeyMaterialTest {
  private static KeyPair validPair;
  private static KeyPair otherPair;

  @BeforeAll
  static void generatePairs() throws Exception {
    var generator = KeyPairGenerator.getInstance("RSA");
    generator.initialize(2048);
    validPair = generator.generateKeyPair();
    otherPair = generator.generateKeyPair();
  }

  @Test
  void loadsPairAndPublishesReconstructablePublicOnlyJwks() {
    var material = load(validPair);
    assertThat(material.privateKey()).isEqualTo(validPair.getPrivate());
    assertThat(material.publicKey()).isEqualTo(validPair.getPublic());
    assertThat(validPair.getPublic().getEncoded()).isEqualTo(material.publicKey().getEncoded());

    var jwkList = (java.util.List<?>) material.jwks().get("keys");
    Map<?, ?> jwk = (Map<?, ?>) jwkList.getFirst();
    assertThat(jwk.get("kty")).isEqualTo("RSA");
    assertThat(jwk.get("kid")).isEqualTo("test-key-1");
    assertThat(jwk.get("use")).isEqualTo("sig");
    assertThat(jwk.get("alg")).isEqualTo("RS256");
    assertThat(jwk.get("e")).isEqualTo("AQAB");
    assertThat(jwk.containsKey("n")).isTrue();
    for (String privateParameter : new String[] {"d", "p", "q", "dp", "dq", "qi"}) {
      assertThat(jwk.containsKey(privateParameter)).isFalse();
    }

    byte[] modulusBytes = Base64.getUrlDecoder().decode((String) jwk.get("n"));
    var modulus = new java.math.BigInteger(1, modulusBytes);
    var exponent =
        new java.math.BigInteger(1, Base64.getUrlDecoder().decode((String) jwk.get("e")));
    assertThat(modulus).isEqualTo(((RSAPublicKey) validPair.getPublic()).getModulus());
    assertThat(exponent).isEqualTo(((RSAPublicKey) validPair.getPublic()).getPublicExponent());
    assertThat(((RSAPublicKey) validPair.getPublic()).getModulus().toByteArray()[0]).isZero();
  }

  @Test
  void rejectsMissingMalformedWeakAndMismatchedKeys() throws Exception {
    Resource absent = new ByteArrayResource(new byte[0]);
    Resource malformed = pemResource("PRIVATE KEY", "not base64!");
    assertThatThrownBy(() -> RsaKeyMaterial.load(absent, publicResource(validPair), "test-key-1"))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(
            () -> RsaKeyMaterial.load(malformed, publicResource(validPair), "test-key-1"))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(
            () ->
                RsaKeyMaterial.load(
                    privateResource(validPair), publicResource(otherPair), "test-key-1"))
        .isInstanceOf(IllegalArgumentException.class);

    var generator = KeyPairGenerator.getInstance("RSA");
    generator.initialize(1024);
    var weak = generator.generateKeyPair();
    assertThatThrownBy(
            () -> RsaKeyMaterial.load(privateResource(weak), publicResource(weak), "test-key-1"))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void rejectsInvalidKeyIds() {
    assertThatThrownBy(() -> load(validPair, " ")).isInstanceOf(IllegalArgumentException.class);
  }

  private static RsaKeyMaterial load(KeyPair pair) {
    return load(pair, "test-key-1");
  }

  private static RsaKeyMaterial load(KeyPair pair, String keyId) {
    return RsaKeyMaterial.load(privateResource(pair), publicResource(pair), keyId);
  }

  private static Resource privateResource(KeyPair pair) {
    return pemResource(
        "PRIVATE KEY",
        Base64.getMimeEncoder(64, new byte[] {'\n'})
            .encodeToString(pair.getPrivate().getEncoded()));
  }

  private static Resource publicResource(KeyPair pair) {
    return pemResource(
        "PUBLIC KEY",
        Base64.getMimeEncoder(64, new byte[] {'\n'}).encodeToString(pair.getPublic().getEncoded()));
  }

  private static Resource pemResource(String type, String content) {
    String pem = "-----BEGIN " + type + "-----\n" + content + "\n-----END " + type + "-----\n";
    return new ByteArrayResource(pem.getBytes(StandardCharsets.US_ASCII));
  }
}
