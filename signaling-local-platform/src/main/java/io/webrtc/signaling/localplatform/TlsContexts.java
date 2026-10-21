package io.webrtc.signaling.localplatform;

import com.sun.net.httpserver.HttpsConfigurator;
import com.sun.net.httpserver.HttpsParameters;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.SecureRandom;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.TrustManagerFactory;

/**
 * Server TLS for the mTLS listeners: TLS 1.3 only, client certificate required.
 *
 * <p>The server key comes from a PKCS12 keystore holding exactly one private-key entry, opened with
 * the password read from the configured file (never from the environment). Client trust is a PEM
 * bundle of 1..{@value #MAX_TRUST_CERTIFICATES} CA certificates loaded into an in-memory store, so
 * each listener trusts exactly its own CA set: the source the workload CA, the admin listener the
 * admin CA. Any problem is {@link LocalPlatformFailure.Category#TLS} without a path, cause or
 * value.
 *
 * <p>Status: LOCAL_TEST_ONLY, NOT_QUALIFIED.
 */
final class TlsContexts {

  static final String PROTOCOL = "TLSv1.3";

  private static final int MAX_KEYSTORE_BYTES = 64 * 1024;
  private static final int MAX_TRUST_BYTES = 64 * 1024;
  private static final int MAX_TRUST_CERTIFICATES = 16;

  private TlsContexts() {}

  /** Builds the server context of {@code listener}; fails with category {@code TLS}. */
  static SSLContext server(LocalPlatformConfig.Listener listener) {
    char[] password = null;
    try {
      password = SecretFiles.readPassword(listener.keystorePasswordFile());
      KeyStore keys = KeyStore.getInstance("PKCS12");
      keys.load(
          new ByteArrayInputStream(
              SecretFiles.readBounded(listener.keystore(), MAX_KEYSTORE_BYTES)),
          password);
      requireSinglePrivateKey(keys);
      KeyManagerFactory keyManagers = KeyManagerFactory.getInstance("PKIX");
      keyManagers.init(keys, password);
      TrustManagerFactory trustManagers = TrustManagerFactory.getInstance("PKIX");
      trustManagers.init(clientTrust(listener));
      SSLContext context = SSLContext.getInstance(PROTOCOL);
      context.init(
          keyManagers.getKeyManagers(), trustManagers.getTrustManagers(), new SecureRandom());
      return context;
    } catch (IOException | GeneralSecurityException | RuntimeException invalid) {
      throw new LocalPlatformFailure(LocalPlatformFailure.Category.TLS);
    } finally {
      if (password != null) {
        Arrays.fill(password, '\0');
      }
    }
  }

  /**
   * Per-connection parameters: only {@value #PROTOCOL}, and the handshake fails without a client
   * certificate that chains to the listener's trust bundle.
   */
  static HttpsConfigurator configurator(SSLContext context) {
    return new HttpsConfigurator(context) {
      @Override
      public void configure(HttpsParameters params) {
        SSLParameters parameters = getSSLContext().getDefaultSSLParameters();
        parameters.setProtocols(new String[] {PROTOCOL});
        parameters.setNeedClientAuth(true);
        params.setSSLParameters(parameters);
      }
    };
  }

  private static void requireSinglePrivateKey(KeyStore keys) throws GeneralSecurityException {
    int privateKeys = 0;
    for (String alias : Collections.list(keys.aliases())) {
      if (keys.entryInstanceOf(alias, KeyStore.PrivateKeyEntry.class)) {
        privateKeys++;
      }
    }
    if (privateKeys != 1) {
      throw new GeneralSecurityException();
    }
  }

  private static KeyStore clientTrust(LocalPlatformConfig.Listener listener)
      throws IOException, GeneralSecurityException {
    Collection<? extends Certificate> certificates =
        CertificateFactory.getInstance("X.509")
            .generateCertificates(
                new ByteArrayInputStream(
                    SecretFiles.readBounded(listener.clientTrust(), MAX_TRUST_BYTES)));
    if (certificates.isEmpty() || certificates.size() > MAX_TRUST_CERTIFICATES) {
      throw new GeneralSecurityException();
    }
    KeyStore trust = KeyStore.getInstance("PKCS12");
    trust.load(null, null);
    int index = 0;
    for (Certificate certificate : certificates) {
      if (!(certificate instanceof X509Certificate ca) || ca.getBasicConstraints() < 0) {
        throw new GeneralSecurityException();
      }
      trust.setCertificateEntry("client-ca-" + index++, ca);
    }
    return trust;
  }
}
