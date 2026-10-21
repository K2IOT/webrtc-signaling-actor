package io.webrtc.signaling.app.runtime.local;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.security.KeyStore;
import java.util.Base64;
import java.util.concurrent.TimeUnit;

/**
 * Ephemeral TEST_ONLY PKI built with the JDK's own {@code keytool}: no openssl and no library, so
 * unit tests can mint leaves with arbitrary URI SANs. Nothing here is a deployable value.
 */
final class LocalTestPki {
    static final String PASSWORD = "TEST_ONLY_STORE";
    static final String ACTOR_URI = "spiffe://signaling/local-test-only/cell/c001/actor";

    private final Path ca, caStore, trust;

    private LocalTestPki(Path ca, Path caStore, Path trust) {
        this.ca = ca;
        this.caStore = caStore;
        this.trust = trust;
    }

    /** A leaf as PEM certificate, PKCS#8 PEM key and a PKCS12 holding the key plus chain. */
    record Leaf(Path certificate, Path key, Path keyStore) {
    }

    static LocalTestPki create(Path dir) throws Exception {
        Files.createDirectories(dir);
        var store = dir.resolve("ca.p12");
        var pem = dir.resolve("ca.crt");
        keytool(
                "-genkeypair", "-alias", "ca", "-keyalg", "EC", "-groupname", "secp256r1",
                "-dname", "CN=TEST_ONLY_CA", "-validity", "3", "-ext", "bc:c",
                "-ext", "ku:c=keyCertSign,cRLSign", "-keystore", store.toString(),
                "-storetype", "PKCS12", "-storepass", PASSWORD);
        keytool(
                "-exportcert", "-rfc", "-alias", "ca", "-keystore", store.toString(),
                "-storepass", PASSWORD, "-file", pem.toString());
        var trust = dir.resolve("trust.p12");
        keytool(
                "-importcert", "-noprompt", "-alias", "ca", "-file", pem.toString(), "-keystore",
                trust.toString(), "-storetype", "PKCS12", "-storepass", PASSWORD);
        return new LocalTestPki(pem, store, trust);
    }

    Path ca() {
        return ca;
    }

    /** {@code san} is the keytool SAN list, e.g. {@code URI:spiffe://...,DNS:localhost}. */
    Leaf leaf(Path dir, String name, String san) throws Exception {
        Files.createDirectories(dir);
        var store = dir.resolve(name + ".p12");
        var request = dir.resolve(name + ".csr");
        var signed = dir.resolve(name + ".crt");
        var key = dir.resolve(name + ".key");
        keytool(
                "-genkeypair", "-alias", "leaf", "-keyalg", "EC", "-groupname", "secp256r1",
                "-dname", "CN=TEST_ONLY_" + name, "-validity", "3", "-keystore", store.toString(),
                "-storetype", "PKCS12", "-storepass", PASSWORD);
        keytool(
                "-certreq", "-alias", "leaf", "-keystore", store.toString(), "-storepass", PASSWORD,
                "-file", request.toString());
        var gencert =
                new java.util.ArrayList<>(
                        java.util.List.of(
                                "-gencert", "-rfc", "-alias", "ca", "-keystore", caStore.toString(),
                                "-storepass", PASSWORD, "-infile", request.toString(), "-outfile",
                                signed.toString(), "-validity", "3", "-ext",
                                "eku=serverAuth,clientAuth", "-ext", "ku:c=digitalSignature"));
        if (san != null) gencert.addAll(java.util.List.of("-ext", "SAN=" + san));
        keytool(gencert.toArray(String[]::new));
        keytool(
                "-importcert", "-noprompt", "-alias", "ca", "-file", ca.toString(), "-keystore",
                store.toString(), "-storepass", PASSWORD);
        keytool(
                "-importcert", "-noprompt", "-alias", "leaf", "-file", signed.toString(), "-keystore",
                store.toString(), "-storepass", PASSWORD);
        var keys = KeyStore.getInstance("PKCS12");
        try (var in = Files.newInputStream(store)) {
            keys.load(in, PASSWORD.toCharArray());
        }
        Files.writeString(
                key,
                pem("PRIVATE KEY", keys.getKey("leaf", PASSWORD.toCharArray()).getEncoded()),
                StandardCharsets.US_ASCII);
        return new Leaf(signed, key, store);
    }

    /** Places a leaf where {@link LocalRuntimeFixtures} points the contract's pod files. */
    void install(Path mount, Leaf leaf) throws Exception {
        Files.createDirectories(mount.resolve("pod"));
        copy(ca, mount.resolve("ca.crt"));
        copy(leaf.certificate(), mount.resolve("pod/leaf.crt"));
        copy(leaf.key(), mount.resolve("pod/leaf.key"));
        copy(leaf.keyStore(), mount.resolve("pod/remoting.p12"));
        copy(trust, mount.resolve("trust.p12"));
    }

    static String pem(String label, byte[] der) {
        return "-----BEGIN " + label + "-----\n"
                + Base64.getMimeEncoder(64, "\n".getBytes(StandardCharsets.US_ASCII)).encodeToString(der)
                + "\n-----END " + label + "-----\n";
    }

    private static void copy(Path from, Path to) throws IOException {
        Files.copy(from, to, StandardCopyOption.REPLACE_EXISTING);
    }

    /**
     * Output goes to a file, never a pipe: a full pipe would block keytool and make the timeout
     * the only way out, and the deadline must apply to the process itself.
     */
    private static void keytool(String... arguments) throws Exception {
        var command = new java.util.ArrayList<String>();
        command.add(Path.of(System.getProperty("java.home"), "bin", "keytool").toString());
        command.addAll(java.util.List.of(arguments));
        var output = Files.createTempFile("TEST_ONLY_keytool", ".log");
        try {
            var process =
                    new ProcessBuilder(command)
                            .redirectErrorStream(true)
                            .redirectOutput(output.toFile())
                            .start();
            if (!process.waitFor(60, TimeUnit.SECONDS)) {
                process.destroyForcibly();
                throw new IllegalStateException("keytool timed out: " + Files.readString(output));
            }
            if (process.exitValue() != 0)
                throw new IllegalStateException("keytool failed: " + Files.readString(output));
        } finally {
            Files.deleteIfExists(output);
        }
    }
}
