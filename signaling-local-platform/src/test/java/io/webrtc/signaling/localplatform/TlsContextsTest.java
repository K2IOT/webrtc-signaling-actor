package io.webrtc.signaling.localplatform;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.net.InetAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import javax.net.ssl.SSLContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class TlsContextsTest {

  private static final InetAddress LOOPBACK = InetAddress.getLoopbackAddress();

  @TempDir Path dir;

  private Path keystore;
  private Path password;
  private Path trust;
  private TestPki.Authority clients;

  @BeforeEach
  void pki() throws IOException {
    TestPki.Authority servers = TestPki.authority("server-ca");
    clients = TestPki.authority("workload-ca");
    keystore =
        servers
            .issue("source", TestPki.Usage.SERVER, "ip:127.0.0.1")
            .writePkcs12(dir.resolve("source.p12"));
    password = Files.writeString(dir.resolve("source.pass"), TestPki.KEYSTORE_PASSWORD + "\n");
    trust = clients.writePem(dir.resolve("trust.pem"));
  }

  private LocalPlatformConfig.Listener listener(Path keystore, Path password, Path trust) {
    return new LocalPlatformConfig.Listener(LOOPBACK, 0, keystore, password, trust);
  }

  private static void assertTlsFailure(LocalPlatformConfig.Listener listener) {
    assertThatThrownBy(() -> TlsContexts.server(listener))
        .isInstanceOfSatisfying(
            LocalPlatformFailure.class,
            failure -> {
              assertThat(failure.category()).isEqualTo(LocalPlatformFailure.Category.TLS);
              assertThat(failure.getMessage()).isEqualTo("TLS");
              assertThat(failure.getCause()).isNull();
            });
  }

  @Test
  void validMaterialBuildsATls13Context() {
    SSLContext context = TlsContexts.server(listener(keystore, password, trust));

    assertThat(context.getProtocol()).isEqualTo("TLSv1.3");
  }

  @Test
  void passwordWithoutTrailingNewlineIsAccepted() throws IOException {
    Files.writeString(password, TestPki.KEYSTORE_PASSWORD);

    assertThat(TlsContexts.server(listener(keystore, password, trust))).isNotNull();
  }

  @Test
  void wrongPasswordIsATlsFailure() throws IOException {
    Files.writeString(password, "TEST_ONLY-wrong\n");

    assertTlsFailure(listener(keystore, password, trust));
  }

  @Test
  void emptyOrOversizedPasswordFileIsATlsFailure() throws IOException {
    Files.writeString(password, "\n");
    assertTlsFailure(listener(keystore, password, trust));
    Files.writeString(password, "x".repeat(2000));
    assertTlsFailure(listener(keystore, password, trust));
  }

  @Test
  void missingFilesAreTlsFailures() {
    Path absent = dir.resolve("absent");

    assertTlsFailure(listener(absent, password, trust));
    assertTlsFailure(listener(keystore, absent, trust));
    assertTlsFailure(listener(keystore, password, absent));
  }

  @Test
  void trustBundleMustHoldCaCertificates() throws IOException {
    Path empty = Files.writeString(dir.resolve("empty.pem"), "");
    Path garbage = Files.writeString(dir.resolve("garbage.pem"), "not a certificate");
    Path leaf =
        TestPki.writeCertificatesPem(
            dir.resolve("leaf.pem"),
            clients.issue("leaf", TestPki.Usage.CLIENT, "dns:test-only.invalid").certificate());

    assertTlsFailure(listener(keystore, password, empty));
    assertTlsFailure(listener(keystore, password, garbage));
    assertTlsFailure(listener(keystore, password, leaf));
  }

  @Test
  void keystoreWithoutAKeyIsATlsFailure() throws IOException {
    // A PEM certificate file is not a PKCS12 keystore.
    assertTlsFailure(listener(trust, password, trust));
  }
}
