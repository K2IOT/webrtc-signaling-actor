package io.webrtc.signaling.app.runtime.local;

import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Consumer;

import static io.webrtc.signaling.app.runtime.local.LocalRuntimeFixtures.JSON;

/**
 * TEST_ONLY actor mount: two enrolled cells and two gateways, a distinct directory credential and a
 * directory snapshot whose digest the contract pins. PKI and signing keys are installed separately.
 */
final class LocalActorFixture {
    static final UUID BOOT_C001 = UUID.fromString("00000000-0000-4000-8000-0000000000b1");
    static final UUID BOOT_C002 = UUID.fromString("00000000-0000-4000-8000-0000000000b2");
    static final String DIRECTORY_URL =
            "jdbc:postgresql://directory.platform.svc:5432/directory?sslmode=verify-full";

    private LocalActorFixture() {
    }

    /** Writes a complete actor mount; {@code snapshot} is written byte for byte and pinned. */
    static LocalRuntimeContract write(
            Path mount, Consumer<Map<String, Object>> profileEdit, byte[] snapshot) throws Exception {
        var env = LocalRuntimeFixtures.install(mount, "ACTOR");
        var key = new byte[32];
        new SecureRandom().nextBytes(key);
        Files.write(mount.resolve("privacy.key"), key);

        var profile = LocalRuntimeFixtures.profile(mount, "ACTOR");
        @SuppressWarnings("unchecked")
        var compatibility = (Map<String, Object>) profile.get("compatibility");
        compatibility.put("major", 1);
        compatibility.put("minor", 2);
        compatibility.put("executableId", LocalPekkoConfigTest.EXECUTABLE);
        compatibility.put("features", List.of("session-s1", "full-connection-active"));
        compatibility.put("lowerMinor", 1);
        compatibility.put("upperMinor", 2);
        profile.put("privacyPseudonymKey", mount.resolve("privacy.key").toString());
        profileEdit.accept(profile);
        Files.writeString(mount.resolve("process.json"), JSON.writeValueAsString(profile));

        Files.write(mount.resolve("directory.json"), snapshot);
        var contract = LocalRuntimeFixtures.contract(mount);
        var directory = LocalRuntimeFixtures.database("c001");
        directory.put("jdbcUrl", DIRECTORY_URL);
        directory.put("username", "directory");
        contract.put("directoryDatabase", directory);
        contract.put("cells", List.of(cell("c001"), cell("c002")));
        contract.put("gateways", List.of(gateway("c001"), gateway("c002")));
        contract.put("directorySnapshotSha256", sha256(snapshot));
        Files.writeString(mount.resolve("provider.json"), JSON.writeValueAsString(contract));
        return LocalRuntimeContract.load(env);
    }

    static LocalRuntimeContract write(
            Path mount, Consumer<Map<String, Object>> profileEdit, Consumer<Map<String, Object>> snapshotEdit)
            throws Exception {
        var snapshot = snapshot();
        snapshotEdit.accept(snapshot);
        return write(mount, profileEdit, JSON.writeValueAsBytes(snapshot));
    }

    static LocalRuntimeContract write(Path mount) throws Exception {
        return write(mount, p -> {
        }, s -> {
        });
    }

    static Map<String, Object> cell(String cell) {
        var host = "actor." + cell + ".svc";
        return Map.of("cell", cell, "host", host, "port", 8443, "tlsAuthority", host, "storageEpoch", 1);
    }

    static Map<String, Object> gateway(String cell) {
        var host = "gateway." + cell + ".svc";
        return Map.of(
                "cell", cell, "gatewayId", "local-" + cell, "host", host, "port", 9443, "tlsAuthority", host);
    }

    static Map<String, Object> snapshot() {
        var document = new LinkedHashMap<String, Object>();
        document.put("schemaVersion", 1);
        document.put("qualification", "LOCAL_TEST_ONLY");
        document.put(
                "buckets", new ArrayList<>(List.of(range(0, 8191, "c001"), range(8192, 16383, "c002"))));
        document.put(
                "gatewayBoots",
                new ArrayList<>(List.of(boot("c001", BOOT_C001), boot("c002", BOOT_C002))));
        return document;
    }

    static Map<String, Object> range(int first, int last, String cell) {
        var range = new LinkedHashMap<String, Object>();
        range.put("first", first);
        range.put("last", last);
        range.put("cell", cell);
        range.put("directoryEpoch", 3);
        range.put("storageEpoch", 1);
        return range;
    }

    static Map<String, Object> boot(String cell, UUID boot) {
        return Map.of("cell", cell, "gatewayId", "local-" + cell, "bootId", boot.toString());
    }

    static String sha256(byte[] bytes) throws Exception {
        return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes));
    }
}
