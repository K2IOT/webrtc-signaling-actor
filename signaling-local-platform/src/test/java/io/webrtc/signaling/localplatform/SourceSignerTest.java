package io.webrtc.signaling.localplatform;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyPair;
import java.security.Signature;
import java.util.Base64;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class SourceSignerTest {

  @TempDir Path dir;

  private static boolean verifies(java.security.PublicKey key, byte[] message, byte[] signature)
      throws Exception {
    Signature verifier = Signature.getInstance("Ed25519");
    verifier.initVerify(key);
    verifier.update(message);
    return verifier.verify(signature);
  }

  @Test
  void loadsEd25519KeyFromPemAndSignsVerifiably() throws Exception {
    KeyPair pair = TestKeys.generate("Ed25519");
    Path file = TestKeys.writePkcs8Pem(dir, "key.pem", pair);

    SourceSigner signer = SourceSigner.load(file, "local-source");

    assertThat(signer.keyId()).isEqualTo("local-source");
    assertThat(signer.publicKey()).isEmpty();
    byte[] message = "message".getBytes(StandardCharsets.UTF_8);
    byte[] signature = signer.sign(message);
    assertThat(signature).hasSize(64);
    assertThat(verifies(pair.getPublic(), message, signature)).isTrue();
    assertThat(verifies(pair.getPublic(), "other".getBytes(StandardCharsets.UTF_8), signature))
        .isFalse();
  }

  @Test
  void ofPairSignsVerifiablyAndExposesThePublicKey() throws Exception {
    KeyPair pair = TestKeys.generate("Ed25519");

    SourceSigner signer = SourceSigner.of(pair, "local-source");

    assertThat(signer.publicKey()).contains(pair.getPublic());
    byte[] message = "message".getBytes(StandardCharsets.UTF_8);
    assertThat(verifies(pair.getPublic(), message, signer.sign(message))).isTrue();
  }

  @Test
  void ofPairRejectsNonEd25519AndBadKeyIds() {
    for (String algorithm : new String[] {"RSA", "EC", "Ed448", "X25519"}) {
      KeyPair other = TestKeys.generate(algorithm);
      assertThatThrownBy(() -> SourceSigner.of(other, "local-source"))
          .isInstanceOfSatisfying(
              LocalPlatformFailure.class,
              failure ->
                  assertThat(failure.category()).isEqualTo(LocalPlatformFailure.Category.KEY));
    }
    KeyPair pair = TestKeys.generate("Ed25519");
    for (String bad : new String[] {"", "a.b", "a:b", "x".repeat(65)}) {
      assertThatThrownBy(() -> SourceSigner.of(pair, bad)).isInstanceOf(LocalPlatformFailure.class);
    }
    assertThatThrownBy(() -> SourceSigner.of(null, "local-source"))
        .isInstanceOf(LocalPlatformFailure.class);
    assertThatThrownBy(() -> SourceSigner.of(pair, null)).isInstanceOf(LocalPlatformFailure.class);
  }

  @Test
  void rejectsKeysThatAreNotEd25519WithKeyCategory() throws Exception {
    for (String algorithm : new String[] {"RSA", "EC", "Ed448", "X25519"}) {
      Path file = TestKeys.writePkcs8Pem(dir, algorithm + ".pem", TestKeys.generate(algorithm));
      assertThatThrownBy(() -> SourceSigner.load(file, "local-source"))
          .isInstanceOfSatisfying(
              LocalPlatformFailure.class,
              failure ->
                  assertThat(failure.category()).isEqualTo(LocalPlatformFailure.Category.KEY));
    }
  }

  @Test
  void rejectsUnreadableOrMalformedFilesWithKeyCategoryAndNoDetail() throws Exception {
    Path garbage = dir.resolve("garbage.pem");
    Files.writeString(
        garbage, "-----BEGIN PRIVATE KEY-----\n!!notbase64!!\n-----END PRIVATE KEY-----\n");
    Path publicOnly =
        TestKeys.writePem(
            dir, "public.pem", "PUBLIC KEY", TestKeys.generate("Ed25519").getPublic().getEncoded());
    Path empty = Files.writeString(dir.resolve("empty.pem"), "");
    Path huge = Files.write(dir.resolve("huge.pem"), new byte[1 << 20]);
    for (Path file : new Path[] {garbage, publicOnly, empty, huge, dir.resolve("missing.pem")}) {
      assertThatThrownBy(() -> SourceSigner.load(file, "local-source"))
          .isInstanceOfSatisfying(
              LocalPlatformFailure.class,
              failure -> {
                assertThat(failure.category()).isEqualTo(LocalPlatformFailure.Category.KEY);
                assertThat(failure.getMessage()).isEqualTo("KEY");
                assertThat(failure.getCause()).isNull();
              });
    }
  }

  @Test
  void aFileFarAboveTheCapIsRefusedWithoutBeingReadWhole() throws Exception {
    // Sparse 3 GiB file: reading it whole cannot succeed in a JVM array, so the cap must apply
    // while reading, not after.
    Path sparse = dir.resolve("sparse.pem");
    try (java.io.RandomAccessFile file = new java.io.RandomAccessFile(sparse.toFile(), "rw")) {
      file.setLength(3L << 30);
    }

    assertThatThrownBy(() -> SourceSigner.load(sparse, "local-source"))
        .isInstanceOfSatisfying(
            LocalPlatformFailure.class,
            failure -> {
              assertThat(failure.category()).isEqualTo(LocalPlatformFailure.Category.KEY);
              assertThat(failure.getCause()).isNull();
            });
  }

  @Test
  void keyIdMustSatisfyBothClockAndRevocationPatterns() throws Exception {
    Path file = TestKeys.writePkcs8Pem(dir, "key.pem", TestKeys.generate("Ed25519"));
    // Valid for the clock pattern only (dot, colon) or too long for the revocation pattern.
    for (String bad :
        new String[] {"", "a.b", "a:b", "a b", "a/b", "x".repeat(65), "naïve", "line\nbreak"}) {
      assertThatThrownBy(() -> SourceSigner.load(file, bad))
          .isInstanceOfSatisfying(
              LocalPlatformFailure.class,
              failure ->
                  assertThat(failure.category()).isEqualTo(LocalPlatformFailure.Category.KEY));
    }
    assertThatThrownBy(() -> SourceSigner.load(file, null))
        .isInstanceOf(LocalPlatformFailure.class);
    assertThat(SourceSigner.load(file, "x".repeat(64)).keyId()).hasSize(64);
    assertThat(SourceSigner.load(file, "Local_source-1").keyId()).isEqualTo("Local_source-1");
  }

  @Test
  void toStringNeverPrintsKeyMaterial() throws Exception {
    KeyPair pair = TestKeys.generate("Ed25519");
    SourceSigner signer =
        SourceSigner.load(TestKeys.writePkcs8Pem(dir, "key.pem", pair), "local-source");

    String rendered = signer.toString();

    assertThat(rendered).isEqualTo("SourceSigner");
    assertThat(rendered)
        .doesNotContain(Base64.getEncoder().encodeToString(pair.getPrivate().getEncoded()));
    assertThat(rendered).doesNotContain("local-source");
  }
}
