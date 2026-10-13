package io.webrtc.signaling.identity;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.KeyFactory;
import java.security.Signature;
import java.security.interfaces.RSAPrivateKey;
import java.security.interfaces.RSAPublicKey;
import java.security.spec.PKCS8EncodedKeySpec;
import java.security.spec.X509EncodedKeySpec;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import org.springframework.core.io.Resource;

public final class RsaKeyMaterial {
  private final RSAPrivateKey privateKey;
  private final RSAPublicKey publicKey;
  private final Map<String, Object> jwks;

  private RsaKeyMaterial(
      RSAPrivateKey privateKey, RSAPublicKey publicKey, Map<String, Object> jwks) {
    this.privateKey = privateKey;
    this.publicKey = publicKey;
    this.jwks = jwks;
  }

  public static RsaKeyMaterial load(Resource privatePem, Resource publicPem, String keyId) {
    try {
      validateKeyId(keyId);
      var factory = KeyFactory.getInstance("RSA");
      var privateKey =
          (RSAPrivateKey)
              factory.generatePrivate(new PKCS8EncodedKeySpec(readPem(privatePem, "PRIVATE KEY")));
      var publicKey =
          (RSAPublicKey)
              factory.generatePublic(new X509EncodedKeySpec(readPem(publicPem, "PUBLIC KEY")));
      if (privateKey.getModulus().bitLength() < 2048 || publicKey.getModulus().bitLength() < 2048) {
        throw new IllegalArgumentException("RSA key size must be at least 2048 bits");
      }
      var signer = Signature.getInstance("SHA256withRSA");
      byte[] probe = "signaling-identity-key-pair-check".getBytes(StandardCharsets.US_ASCII);
      signer.initSign(privateKey);
      signer.update(probe);
      byte[] signature = signer.sign();
      signer.initVerify(publicKey);
      signer.update(probe);
      if (!signer.verify(signature))
        throw new IllegalArgumentException("RSA key pair does not match");

      var jwk =
          Map.<String, Object>of(
              "kty",
              "RSA",
              "kid",
              keyId,
              "use",
              "sig",
              "alg",
              "RS256",
              "n",
              base64UrlUnsigned(publicKey.getModulus().toByteArray()),
              "e",
              base64UrlUnsigned(publicKey.getPublicExponent().toByteArray()));
      return new RsaKeyMaterial(privateKey, publicKey, Map.of("keys", List.of(jwk)));
    } catch (IOException | java.security.GeneralSecurityException | ClassCastException e) {
      throw new IllegalArgumentException("Unable to load configured RSA key pair", e);
    }
  }

  public RSAPrivateKey privateKey() {
    return privateKey;
  }

  public RSAPublicKey publicKey() {
    return publicKey;
  }

  public Map<String, Object> jwks() {
    return jwks;
  }

  private static byte[] readPem(Resource resource, String type)
      throws IOException, java.security.GeneralSecurityException {
    if (resource == null || !resource.exists()) throw new IOException("PEM resource is missing");
    byte[] data;
    try (var input = resource.getInputStream()) {
      data = input.readNBytes(32_769);
    }
    if (data.length > 32_768) throw new IOException("PEM resource is too large");
    String text = new String(data, StandardCharsets.US_ASCII);
    String begin = "-----BEGIN " + type + "-----";
    String end = "-----END " + type + "-----";
    int startIndex = text.indexOf(begin), endIndex = text.indexOf(end);
    if (startIndex < 0
        || endIndex <= startIndex
        || text.indexOf(begin, startIndex + begin.length()) >= 0) {
      throw new IOException("PEM content has an invalid envelope");
    }
    String encoded = text.substring(startIndex + begin.length(), endIndex).replaceAll("\\s", "");
    if (encoded.isEmpty()
        || !text.substring(0, startIndex).isBlank()
        || !text.substring(endIndex + end.length()).isBlank()) {
      throw new IOException("PEM content has unexpected data");
    }
    return Base64.getDecoder().decode(encoded);
  }

  private static String base64UrlUnsigned(byte[] signed) {
    int offset = signed.length > 1 && signed[0] == 0 ? 1 : 0;
    return Base64.getUrlEncoder()
        .withoutPadding()
        .encodeToString(java.util.Arrays.copyOfRange(signed, offset, signed.length));
  }

  private static void validateKeyId(String keyId) {
    if (keyId == null
        || keyId.isBlank()
        || !keyId.equals(keyId.strip())
        || keyId.getBytes(StandardCharsets.UTF_8).length > 256
        || keyId.codePoints().anyMatch(Character::isISOControl)) {
      throw new IllegalArgumentException("invalid key ID");
    }
  }
}
