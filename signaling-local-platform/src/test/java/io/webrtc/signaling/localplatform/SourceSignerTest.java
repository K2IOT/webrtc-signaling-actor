package io.webrtc.signaling.localplatform;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyPair;
import java.security.Signature;
import java.security.interfaces.EdECPublicKey;
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
  void loadsEd25519KeyAndSignsVerifiablyAndDerivesTheMatchingPublicKey() throws Exception {
    KeyPair pair = TestKeys.generate("Ed25519");
    Path file = TestKeys.writePkcs8Pem(dir, "key.pem", pair);

    SourceSigner signer = SourceSigner.load(file, "local-source");

    assertThat(signer.keyId()).isEqualTo("local-source");
    assertThat(signer.publicKey()).isInstanceOf(EdECPublicKey.class);
    assertThat(signer.publicKey().getEncoded()).isEqualTo(pair.getPublic().getEncoded());
    byte[] message = "message".getBytes(StandardCharsets.UTF_8);
    byte[] signature = signer.sign(message);
    assertThat(signature).hasSize(64);
    assertThat(verifies(pair.getPublic(), message, signature)).isTrue();
    assertThat(verifies(signer.publicKey(), message, signature)).isTrue();
  }

  @Test
  void derivedPublicKeyMatchesForManyKeys() throws Exception {
    for (int i = 0; i < 25; i++) {
      KeyPair pair = TestKeys.generate("Ed25519");
      SourceSigner signer = SourceSigner.load(TestKeys.writePkcs8Pem(dir, "k" + i, pair), "k-" + i);
      assertThat(signer.publicKey().getEncoded()).isEqualTo(pair.getPublic().getEncoded());
    }
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
