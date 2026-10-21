package io.webrtc.signaling.rpc;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Ephemeral TEST_ONLY PKI minted with the JDK's own {@code keytool}, so tests can issue leaves with
 * arbitrary SPIFFE URI SANs. The checked-in fixtures cannot: their CA key was deleted. Nothing here
 * is a deployable value.
 */
final class RpcTestPki {
    private static final String PASSWORD = "TEST_ONLY_STORE";
    private final Path dir, ca, caStore;

    private RpcTestPki(Path dir, Path ca, Path caStore) {
        this.dir = dir;
        this.ca = ca;
        this.caStore = caStore;
    }

    /** PEM certificate and PKCS#8 PEM key of one leaf. */
    record Leaf(Path certificate, Path key) {
    }

    static RpcTestPki create(Path dir) throws Exception {
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
        return new RpcTestPki(dir, pem, store);
    }

    Path ca() {
        return ca;
    }

    /** {@code san} is the keytool SAN list, e.g. {@code URI:spiffe://...,DNS:localhost}. */
    Leaf leaf(String name, String san) throws Exception {
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
        keytool(
                "-gencert", "-rfc", "-alias", "ca", "-keystore", caStore.toString(), "-storepass",
                PASSWORD, "-infile", request.toString(), "-outfile", signed.toString(), "-validity",
                "3", "-ext", "eku=serverAuth,clientAuth", "-ext", "ku:c=digitalSignature", "-ext",
                "SAN=" + san);
        var keys = KeyStore.getInstance("PKCS12");
        try (var in = Files.newInputStream(store)) {
            keys.load(in, PASSWORD.toCharArray());
        }
        var der = keys.getKey("leaf", PASSWORD.toCharArray()).getEncoded();
        Files.writeString(
                key,
                "-----BEGIN PRIVATE KEY-----\n"
                        + Base64.getMimeEncoder(64, "\n".getBytes(StandardCharsets.US_ASCII))
                        .encodeToString(der)
                        + "\n-----END PRIVATE KEY-----\n",
                StandardCharsets.US_ASCII);
        return new Leaf(signed, key);
    }

    /** Output goes to a file, never a pipe, so the deadline applies to keytool itself. */
    private static void keytool(String... arguments) throws Exception {
        var command = new ArrayList<String>();
        command.add(Path.of(System.getProperty("java.home"), "bin", "keytool").toString());
        command.addAll(List.of(arguments));
        var output = Files.createTempFile("TEST_ONLY_keytool", ".log");
        try {
            var process =
                    new ProcessBuilder(command).redirectErrorStream(true).redirectOutput(output.toFile()).start();
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
