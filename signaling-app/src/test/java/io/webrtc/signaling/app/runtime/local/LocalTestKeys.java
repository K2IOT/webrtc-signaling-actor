package io.webrtc.signaling.app.runtime.local;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyPair;
import java.security.KeyPairGenerator;

/** Ephemeral TEST_ONLY signing keys written where {@link LocalRuntimeFixtures} points the contract. */
final class LocalTestKeys {
    final KeyPair proof, source, rsa;

    private LocalTestKeys(KeyPair proof, KeyPair source, KeyPair rsa) {
        this.proof = proof;
        this.source = source;
        this.rsa = rsa;
    }

    static LocalTestKeys install(Path mount) throws Exception {
        var ed = KeyPairGenerator.getInstance("Ed25519");
        var rsa = KeyPairGenerator.getInstance("RSA");
        rsa.initialize(2048);
        var keys = new LocalTestKeys(ed.generateKeyPair(), ed.generateKeyPair(), rsa.generateKeyPair());
        write(mount.resolve("proof.key"), "PRIVATE KEY", keys.proof.getPrivate().getEncoded());
        write(mount.resolve("proof.pub"), "PUBLIC KEY", keys.proof.getPublic().getEncoded());
        write(mount.resolve("source.pub"), "PUBLIC KEY", keys.source.getPublic().getEncoded());
        write(mount.resolve("rsa.pub"), "PUBLIC KEY", keys.rsa.getPublic().getEncoded());
        return keys;
    }

    static void write(Path file, String label, byte[] der) throws Exception {
        Files.writeString(file, LocalTestPki.pem(label, der), StandardCharsets.US_ASCII);
    }
}
