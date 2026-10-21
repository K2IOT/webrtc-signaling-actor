package io.webrtc.signaling.localplatform;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.math.BigInteger;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.KeyStore;
import java.security.SecureRandom;
import java.security.Signature;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.security.spec.ECGenParameterSpec;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;

/**
 * Throw-away TEST_ONLY PKI generated in-process at test time. No private key is ever written to the
 * source tree; keystores land in a JUnit temporary directory.
 *
 * <p>Certificates are hand-encoded DER (X.509 v3, EC P-256, ecdsa-with-SHA256) and parsed back by
 * the JDK {@link CertificateFactory}; no {@code sun.security.x509} internals are used. Every
 * subject is {@code O=TEST_ONLY, CN=TEST_ONLY <name>}; validity is one day back to seven days
 * ahead.
 *
 * <p>Status: LOCAL_TEST_ONLY, NOT_QUALIFIED.
 */
final class TestPki {

  /** SPIFFE prefix of the placeholder trust domain. */
  static final String BASE = "spiffe://signaling/local-test-only";

  /** Password of every keystore {@link Leaf#writePkcs12} writes. TEST_ONLY. */
  static final String KEYSTORE_PASSWORD = "TEST_ONLY-keystore";

  private static final SecureRandom RANDOM = new SecureRandom();
  private static final DateTimeFormatter UTC_TIME =
      DateTimeFormatter.ofPattern("yyMMddHHmmss'Z'").withZone(ZoneOffset.UTC);

  private static final String ECDSA_SHA256 = "1.2.840.10045.4.3.2";
  private static final String ORGANIZATION = "2.5.4.10";
  private static final String COMMON_NAME = "2.5.4.3";
  private static final String BASIC_CONSTRAINTS = "2.5.29.19";
  private static final String KEY_USAGE = "2.5.29.15";
  private static final String EXT_KEY_USAGE = "2.5.29.37";
  private static final String SUBJECT_ALT_NAME = "2.5.29.17";
  private static final String SERVER_AUTH = "1.3.6.1.5.5.7.3.1";
  private static final String CLIENT_AUTH = "1.3.6.1.5.5.7.3.2";

  /** What a leaf certificate is for (its extended key usage). */
  enum Usage {
    SERVER,
    CLIENT
  }

  private TestPki() {}

  /** A self-signed TEST_ONLY certificate authority. */
  static final class Authority {
    private final String name;
    private final KeyPair keys;
    private final X509Certificate certificate;

    private Authority(String name, KeyPair keys, X509Certificate certificate) {
      this.name = name;
      this.keys = keys;
      this.certificate = certificate;
    }

    X509Certificate certificate() {
      return certificate;
    }

    /**
     * Issues a leaf. Each SAN is {@code uri:<value>}, {@code dns:<value>} or {@code ip:<literal>};
     * no SAN at all is allowed.
     */
    Leaf issue(String leafName, Usage usage, String... sans) {
      KeyPair leafKeys = ecKeys();
      List<byte[]> extensions = new ArrayList<>();
      extensions.add(extension(BASIC_CONSTRAINTS, true, seq()));
      extensions.add(extension(KEY_USAGE, true, bitString(new byte[] {(byte) 0x80}, 7)));
      extensions.add(
          extension(
              EXT_KEY_USAGE, false, seq(oid(usage == Usage.SERVER ? SERVER_AUTH : CLIENT_AUTH))));
      if (sans.length > 0) {
        extensions.add(extension(SUBJECT_ALT_NAME, false, generalNames(sans)));
      }
      X509Certificate leaf =
          sign(name(name), name(leafName), leafKeys, keys, extensions.toArray(new byte[0][]));
      return new Leaf(leafKeys, leaf, this);
    }

    /** Writes the CA certificate as a PEM file (a client-trust bundle of one). */
    Path writePem(Path file) throws IOException {
      return writeCertificatesPem(file, certificate);
    }
  }

  /** A leaf key pair and certificate together with its issuing authority. */
  static final class Leaf {
    private final KeyPair keys;
    private final X509Certificate certificate;
    private final Authority issuer;

    private Leaf(KeyPair keys, X509Certificate certificate, Authority issuer) {
      this.keys = keys;
      this.certificate = certificate;
      this.issuer = issuer;
    }

    X509Certificate certificate() {
      return certificate;
    }

    /** Writes a PKCS12 keystore (key + chain to the issuer) protected by the TEST_ONLY password. */
    Path writePkcs12(Path file) throws IOException {
      try (OutputStream out = Files.newOutputStream(file)) {
        keyStore().store(out, KEYSTORE_PASSWORD.toCharArray());
        return file;
      } catch (GeneralSecurityException e) {
        throw new IllegalStateException(e);
      }
    }

    private KeyStore keyStore() throws GeneralSecurityException, IOException {
      KeyStore store = KeyStore.getInstance("PKCS12");
      store.load(null, null);
      store.setKeyEntry(
          "leaf",
          keys.getPrivate(),
          KEYSTORE_PASSWORD.toCharArray(),
          new Certificate[] {certificate, issuer.certificate});
      return store;
    }
  }

  /** Creates a self-signed CA. */
  static Authority authority(String name) {
    KeyPair keys = ecKeys();
    X509Certificate certificate =
        sign(
            name(name),
            name(name),
            keys,
            keys,
            extension(BASIC_CONSTRAINTS, true, seq(bool(true))),
            // keyCertSign (bit 5) and cRLSign (bit 6): 0b0000_0110 with one unused bit.
            extension(KEY_USAGE, true, bitString(new byte[] {0x06}, 1)));
    return new Authority(name, keys, certificate);
  }

  /**
   * A client or server SSL context ({@code TLS}, all protocols the JDK enables) that presents
   * {@code identity} (none when null) and trusts {@code trusted}.
   */
  static SSLContext context(Leaf identity, Authority... trusted) {
    try {
      KeyManagerFactory keyManagers = KeyManagerFactory.getInstance("PKIX");
      if (identity == null) {
        KeyStore empty = KeyStore.getInstance("PKCS12");
        empty.load(null, null);
        keyManagers.init(empty, new char[0]);
      } else {
        keyManagers.init(identity.keyStore(), KEYSTORE_PASSWORD.toCharArray());
      }
      KeyStore trust = KeyStore.getInstance("PKCS12");
      trust.load(null, null);
      for (int i = 0; i < trusted.length; i++) {
        trust.setCertificateEntry("trust-" + i, trusted[i].certificate);
      }
      TrustManagerFactory trustManagers = TrustManagerFactory.getInstance("PKIX");
      trustManagers.init(trust);
      SSLContext context = SSLContext.getInstance("TLS");
      context.init(keyManagers.getKeyManagers(), trustManagers.getTrustManagers(), RANDOM);
      return context;
    } catch (GeneralSecurityException | IOException e) {
      throw new IllegalStateException(e);
    }
  }

  /** Writes certificates as one PEM bundle. */
  static Path writeCertificatesPem(Path file, X509Certificate... certificates) throws IOException {
    StringBuilder pem = new StringBuilder();
    Base64.Encoder encoder = Base64.getMimeEncoder(64, "\n".getBytes(StandardCharsets.US_ASCII));
    for (X509Certificate certificate : certificates) {
      try {
        pem.append("-----BEGIN CERTIFICATE-----\n")
            .append(encoder.encodeToString(certificate.getEncoded()))
            .append("\n-----END CERTIFICATE-----\n");
      } catch (GeneralSecurityException e) {
        throw new IllegalStateException(e);
      }
    }
    return Files.writeString(file, pem, StandardCharsets.US_ASCII);
  }

  private static KeyPair ecKeys() {
    try {
      KeyPairGenerator generator = KeyPairGenerator.getInstance("EC");
      generator.initialize(new ECGenParameterSpec("secp256r1"), RANDOM);
      return generator.generateKeyPair();
    } catch (GeneralSecurityException e) {
      throw new IllegalStateException(e);
    }
  }

  private static X509Certificate sign(
      byte[] issuer,
      byte[] subject,
      KeyPair subjectKeys,
      KeyPair issuerKeys,
      byte[]... extensions) {
    Instant now = Instant.now();
    byte[] algorithm = seq(oid(ECDSA_SHA256));
    byte[] tbs =
        seq(
            explicit(0, integer(BigInteger.TWO)),
            integer(new BigInteger(127, RANDOM).add(BigInteger.ONE)),
            algorithm,
            issuer,
            seq(utcTime(now.minus(Duration.ofDays(1))), utcTime(now.plus(Duration.ofDays(7)))),
            subject,
            subjectKeys.getPublic().getEncoded(),
            explicit(3, seq(extensions)));
    try {
      Signature signature = Signature.getInstance("SHA256withECDSA");
      signature.initSign(issuerKeys.getPrivate());
      signature.update(tbs);
      byte[] der = seq(tbs, algorithm, bitString(signature.sign(), 0));
      return (X509Certificate)
          CertificateFactory.getInstance("X.509")
              .generateCertificate(new ByteArrayInputStream(der));
    } catch (GeneralSecurityException e) {
      throw new IllegalStateException(e);
    }
  }

  private static byte[] name(String name) {
    return seq(
        set(seq(oid(ORGANIZATION), utf8("TEST_ONLY"))),
        set(seq(oid(COMMON_NAME), utf8("TEST_ONLY " + name))));
  }

  private static byte[] extension(String id, boolean critical, byte[] value) {
    return critical ? seq(oid(id), bool(true), tlv(0x04, value)) : seq(oid(id), tlv(0x04, value));
  }

  private static byte[] generalNames(String... sans) {
    byte[][] names = new byte[sans.length][];
    for (int i = 0; i < sans.length; i++) {
      String san = sans[i];
      if (san.startsWith("uri:")) {
        names[i] = tlv(0x86, san.substring(4).getBytes(StandardCharsets.US_ASCII));
      } else if (san.startsWith("dns:")) {
        names[i] = tlv(0x82, san.substring(4).getBytes(StandardCharsets.US_ASCII));
      } else if (san.startsWith("ip:")) {
        try {
          names[i] = tlv(0x87, InetAddress.getByName(san.substring(3)).getAddress());
        } catch (IOException e) {
          throw new IllegalArgumentException(e);
        }
      } else {
        throw new IllegalArgumentException("unsupported SAN type");
      }
    }
    return seq(names);
  }

  // --- minimal DER encoding -----------------------------------------------------------------

  private static byte[] tlv(int tag, byte[] content) {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    out.write(tag);
    int length = content.length;
    if (length < 0x80) {
      out.write(length);
    } else {
      byte[] bytes = BigInteger.valueOf(length).toByteArray();
      int start = bytes[0] == 0 ? 1 : 0;
      out.write(0x80 | (bytes.length - start));
      out.write(bytes, start, bytes.length - start);
    }
    out.writeBytes(content);
    return out.toByteArray();
  }

  private static byte[] concat(byte[]... parts) {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    for (byte[] part : parts) {
      out.writeBytes(part);
    }
    return out.toByteArray();
  }

  private static byte[] seq(byte[]... parts) {
    return tlv(0x30, concat(parts));
  }

  private static byte[] set(byte[]... parts) {
    return tlv(0x31, concat(parts));
  }

  private static byte[] explicit(int number, byte[] content) {
    return tlv(0xA0 | number, content);
  }

  private static byte[] integer(BigInteger value) {
    return tlv(0x02, value.toByteArray());
  }

  private static byte[] bool(boolean value) {
    return tlv(0x01, new byte[] {(byte) (value ? 0xFF : 0x00)});
  }

  private static byte[] utf8(String value) {
    return tlv(0x0C, value.getBytes(StandardCharsets.UTF_8));
  }

  private static byte[] utcTime(Instant instant) {
    return tlv(0x17, UTC_TIME.format(instant).getBytes(StandardCharsets.US_ASCII));
  }

  private static byte[] bitString(byte[] bits, int unusedBits) {
    return tlv(0x03, concat(new byte[] {(byte) unusedBits}, bits));
  }

  private static byte[] oid(String dotted) {
    String[] arcs = dotted.split("\\.");
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    out.write(Integer.parseInt(arcs[0]) * 40 + Integer.parseInt(arcs[1]));
    for (int i = 2; i < arcs.length; i++) {
      long arc = Long.parseLong(arcs[i]);
      byte[] groups = new byte[10];
      int count = 0;
      do {
        groups[count++] = (byte) (arc & 0x7F);
        arc >>>= 7;
      } while (arc != 0);
      for (int j = count - 1; j >= 0; j--) {
        out.write(groups[j] | (j == 0 ? 0 : 0x80));
      }
    }
    return tlv(0x06, out.toByteArray());
  }
}
