package io.webrtc.signaling.localplatform;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.util.Base64;

/** Generates throwaway keys in the test; no private key is ever committed. */
final class TestKeys {

  private TestKeys() {}

  static KeyPair generate(String algorithm) {
    try {
      return KeyPairGenerator.getInstance(algorithm).generateKeyPair();
    } catch (GeneralSecurityException e) {
      throw new IllegalStateException(e);
    }
  }

  static Path writePkcs8Pem(Path directory, String name, KeyPair pair) throws IOException {
    return writePem(directory, name, "PRIVATE KEY", pair.getPrivate().getEncoded());
  }

  static Path writePem(Path directory, String name, String label, byte[] der) throws IOException {
    String body =
        Base64.getMimeEncoder(64, "\n".getBytes(StandardCharsets.US_ASCII)).encodeToString(der);
    String pem = "-----BEGIN " + label + "-----\n" + body + "\n-----END " + label + "-----\n";
    return Files.writeString(directory.resolve(name), pem, StandardCharsets.US_ASCII);
  }
}
