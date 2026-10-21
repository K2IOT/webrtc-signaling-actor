package io.webrtc.signaling.storage;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyFactory;
import java.security.KeyPair;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.SecureRandom;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.security.spec.PKCS8EncodedKeySpec;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HexFormat;
import java.util.List;
import java.util.concurrent.TimeUnit;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;

/**
 * Ephemeral TEST_ONLY PKI for the local composition test, minted with the host {@code openssl}
 * (as the compose provisioner does). The committed {@code test-only-pki} fixtures cannot be used:
 * their CA keys were discarded and their SPIFFE namespace is {@code test}, not {@code
 * local-test-only}. Every key and certificate lives in a temporary directory, is valid for three
 * days and is never deployable.
 */
final class LocalCompositionPki {
    static final String STORE_PASSWORD = "TEST_ONLY_REMOTING_STORE";
    private final Path dir;
    final Ca workload;
    final Ca remoting;

    private LocalCompositionPki(Path dir, Ca workload, Ca remoting) {
        this.dir = dir;
        this.workload = workload;
        this.remoting = remoting;
    }

    /** A CA certificate and its private key (PEM). */
    record Ca(Path certificate, Path key) {
    }

    /** A leaf certificate (PEM) and its PKCS#8 private key (PEM). */
    record Leaf(Path certificate, Path key) {
    }

    static LocalCompositionPki create(Path dir) throws Exception {
        Files.createDirectories(dir);
        return new LocalCompositionPki(
                dir, ca(dir, "workload-ca"), ca(dir, "remoting-ca"));
    }

    /** {@code san} is an openssl subjectAltName list, e.g. {@code URI:spiffe://...,DNS:x,IP:y}. */
    Leaf leaf(Ca ca, String name, String san) throws Exception {
        var key = dir.resolve(name + ".key");
        var request = dir.resolve(name + ".csr");
        var certificate = dir.resolve(name + ".crt");
        var extensions = dir.resolve(name + ".ext");
        openssl(
                "req", "-new", "-newkey", "ec", "-pkeyopt", "ec_paramgen_curve:P-256", "-nodes",
                "-keyout", key.toString(), "-out", request.toString(), "-subj", "/CN=TEST_ONLY_" + name);
        Files.writeString(
                extensions,
                "basicConstraints=critical,CA:FALSE\n"
                        + "keyUsage=critical,digitalSignature\n"
                        + "extendedKeyUsage=serverAuth,clientAuth\n"
                        + "subjectAltName="
                        + san
                        + "\n");
        openssl(
                "x509", "-req", "-in", request.toString(), "-CA", ca.certificate().toString(),
                "-CAkey", ca.key().toString(), "-set_serial", "0x" + serial(), "-days", "3",
                "-sha256", "-extfile", extensions.toString(), "-out", certificate.toString());
        return new Leaf(certificate, key);
    }

    /** PKCS12 key store holding the leaf key with its chain (leaf, CA). */
    void keyStore(Leaf leaf, Ca ca, Path target) throws Exception {
        var store = KeyStore.getInstance("PKCS12");
        store.load(null, null);
        store.setKeyEntry(
                "leaf",
                privateKey(leaf.key()),
                STORE_PASSWORD.toCharArray(),
                new Certificate[]{certificate(leaf.certificate()), certificate(ca.certificate())});
        try (var out = Files.newOutputStream(target)) {
            store.store(out, STORE_PASSWORD.toCharArray());
        }
    }

    /** PKCS12 trust store holding only the CA certificate as a trusted entry. */
    void trustStore(Ca ca, Path target) throws Exception {
        var store = KeyStore.getInstance("PKCS12");
        store.load(null, null);
        store.setCertificateEntry("ca", certificate(ca.certificate()));
        try (var out = Files.newOutputStream(target)) {
            store.store(out, STORE_PASSWORD.toCharArray());
        }
    }

    /** A JDK TLSv1.3 context presenting {@code leaf} (may be null) and trusting {@code ca}. */
    static SSLContext context(Leaf leaf, Ca ca) throws Exception {
        var trust = KeyStore.getInstance("PKCS12");
        trust.load(null, null);
        trust.setCertificateEntry("ca", certificate(ca.certificate()));
        var trustManagers = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        trustManagers.init(trust);
        javax.net.ssl.KeyManager[] keyManagers = null;
        if (leaf != null) {
            var keys = KeyStore.getInstance("PKCS12");
            keys.load(null, null);
            keys.setKeyEntry(
                    "leaf",
                    privateKey(leaf.key()),
                    new char[0],
                    new Certificate[]{certificate(leaf.certificate()), certificate(ca.certificate())});
            var factory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
            factory.init(keys, new char[0]);
            keyManagers = factory.getKeyManagers();
        }
        var context = SSLContext.getInstance("TLSv1.3");
        context.init(keyManagers, trustManagers.getTrustManagers(), null);
        return context;
    }

    static Certificate certificate(Path pem) throws Exception {
        try (var in = Files.newInputStream(pem)) {
            return CertificateFactory.getInstance("X.509").generateCertificate(in);
        }
    }

    static PrivateKey privateKey(Path pem) throws Exception {
        var der = der(Files.readString(pem, StandardCharsets.US_ASCII), "PRIVATE KEY");
        for (var algorithm : List.of("EC", "Ed25519", "RSA"))
            try {
                return KeyFactory.getInstance(algorithm).generatePrivate(new PKCS8EncodedKeySpec(der));
            } catch (java.security.spec.InvalidKeySpecException other) {
                // Try the next algorithm.
            }
        throw new IllegalArgumentException("TEST_ONLY key of unknown type");
    }

    /** Writes the public key as X.509 PEM and the private key as PKCS#8 PEM. */
    static void writeKeyPair(KeyPair keys, Path publicPem, Path privatePem) throws Exception {
        if (publicPem != null) Files.writeString(publicPem, pem("PUBLIC KEY", keys.getPublic().getEncoded()));
        if (privatePem != null)
            Files.writeString(privatePem, pem("PRIVATE KEY", keys.getPrivate().getEncoded()));
    }

    static String pem(String label, byte[] der) {
        return "-----BEGIN " + label + "-----\n"
                + Base64.getMimeEncoder(64, "\n".getBytes(StandardCharsets.US_ASCII)).encodeToString(der)
                + "\n-----END " + label + "-----\n";
    }

    private static byte[] der(String text, String label) {
        var body =
                text.replace("-----BEGIN " + label + "-----", "")
                        .replace("-----END " + label + "-----", "")
                        .replaceAll("\\s", "");
        return Base64.getDecoder().decode(body);
    }

    /** Every certificate of a PEM bundle, for trust checks in tests. */
    static List<Certificate> certificates(Path pem) throws Exception {
        return new ArrayList<>(
                CertificateFactory.getInstance("X.509")
                        .generateCertificates(new ByteArrayInputStream(Files.readAllBytes(pem))));
    }

    private static Ca ca(Path dir, String name) throws Exception {
        var key = dir.resolve(name + ".key");
        var certificate = dir.resolve(name + ".crt");
        openssl(
                "req", "-x509", "-newkey", "ec", "-pkeyopt", "ec_paramgen_curve:P-256", "-nodes",
                "-keyout", key.toString(), "-out", certificate.toString(), "-days", "3", "-sha256",
                "-subj", "/CN=TEST_ONLY_" + name, "-addext", "basicConstraints=critical,CA:TRUE",
                "-addext", "keyUsage=critical,keyCertSign,cRLSign");
        return new Ca(certificate, key);
    }

    private static String serial() {
        var bytes = new byte[16];
        new SecureRandom().nextBytes(bytes);
        bytes[0] &= 0x7f;
        return HexFormat.of().formatHex(bytes);
    }

    /** Output goes to a file, never a pipe, so the deadline applies to openssl itself. */
    private static void openssl(String... arguments) throws Exception {
        var command = new ArrayList<String>();
        command.add("openssl");
        command.addAll(List.of(arguments));
        var output = Files.createTempFile("TEST_ONLY_openssl", ".log");
        try {
            var process =
                    new ProcessBuilder(command).redirectErrorStream(true).redirectOutput(output.toFile()).start();
            if (!process.waitFor(60, TimeUnit.SECONDS)) {
                process.destroyForcibly();
                throw new IllegalStateException("openssl timed out: " + Files.readString(output));
            }
            if (process.exitValue() != 0)
                throw new IllegalStateException("openssl failed: " + Files.readString(output));
        } finally {
            Files.deleteIfExists(output);
        }
    }
}
