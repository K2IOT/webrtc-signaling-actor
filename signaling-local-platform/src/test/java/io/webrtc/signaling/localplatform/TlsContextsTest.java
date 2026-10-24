package io.webrtc.signaling.localplatform;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.net.InetAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.security.KeyStoreSpi;
import java.util.concurrent.atomic.AtomicInteger;
import javax.net.ssl.SSLContext;
import javax.net.ssl.X509KeyManager;
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

  /** A PKCS12 store that counts private-key reads (each is a PBKDF2 key derivation). */
  private static KeyStore counting(Path keystore, AtomicInteger reads) throws Exception {
    KeyStore real = KeyStore.getInstance("PKCS12");
    try (var in = Files.newInputStream(keystore)) {
      real.load(in, TestPki.KEYSTORE_PASSWORD.toCharArray());
    }
    KeyStoreSpi spi =
        new KeyStoreSpi() {
          public java.security.Key engineGetKey(String alias, char[] password)
              throws java.security.NoSuchAlgorithmException,
                  java.security.UnrecoverableKeyException {
            reads.incrementAndGet();
            try {
              return real.getKey(alias, password);
            } catch (java.security.KeyStoreException e) {
              throw new IllegalStateException(e);
            }
          }

          public java.security.cert.Certificate[] engineGetCertificateChain(String alias) {
            return call(() -> real.getCertificateChain(alias));
          }

          public java.security.cert.Certificate engineGetCertificate(String alias) {
            return call(() -> real.getCertificate(alias));
          }

          public java.util.Date engineGetCreationDate(String alias) {
            return call(() -> real.getCreationDate(alias));
          }

          public void engineSetKeyEntry(
              String a, java.security.Key k, char[] p, java.security.cert.Certificate[] c) {
            throw new UnsupportedOperationException();
          }

          public void engineSetKeyEntry(String a, byte[] k, java.security.cert.Certificate[] c) {
            throw new UnsupportedOperationException();
          }

          public void engineSetCertificateEntry(String a, java.security.cert.Certificate c) {
            throw new UnsupportedOperationException();
          }

          public void engineDeleteEntry(String a) {
            throw new UnsupportedOperationException();
          }

          public java.util.Enumeration<String> engineAliases() {
            return call(real::aliases);
          }

          public boolean engineContainsAlias(String alias) {
            return call(() -> real.containsAlias(alias));
          }

          public int engineSize() {
            return call(real::size);
          }

          public boolean engineIsKeyEntry(String alias) {
            return call(() -> real.isKeyEntry(alias));
          }

          public boolean engineIsCertificateEntry(String alias) {
            return call(() -> real.isCertificateEntry(alias));
          }

          public String engineGetCertificateAlias(java.security.cert.Certificate c) {
            return call(() -> real.getCertificateAlias(c));
          }

          public void engineStore(java.io.OutputStream s, char[] p) {
            throw new UnsupportedOperationException();
          }

          public void engineLoad(java.io.InputStream s, char[] p) {}
        };
    KeyStore store = new KeyStore(spi, real.getProvider(), "PKCS12") {};
    store.load(null, null);
    return store;
  }

  private interface Lookup<T> {
    T get() throws java.security.KeyStoreException;
  }

  private static <T> T call(Lookup<T> lookup) {
    try {
      return lookup.get();
    } catch (java.security.KeyStoreException e) {
      throw new IllegalStateException(e);
    }
  }

  @Test
  void handshakesReuseThePrivateKeyInsteadOfDecryptingTheKeystoreEachTime() throws Exception {
    var reads = new AtomicInteger();
    KeyStore store = counting(keystore, reads);
    var managers = TlsContexts.keyManagers(store, TestPki.KEYSTORE_PASSWORD.toCharArray());
    var manager = (X509KeyManager) managers[0];
    String alias = manager.chooseServerAlias("EC", null, null);
    if (alias == null) alias = manager.chooseServerAlias("RSA", null, null);
    int afterInit = reads.get();
    for (int handshake = 0; handshake < 20; handshake++)
      assertThat(manager.getPrivateKey(alias)).isNotNull();
    assertThat(reads.get()).isEqualTo(afterInit);
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
