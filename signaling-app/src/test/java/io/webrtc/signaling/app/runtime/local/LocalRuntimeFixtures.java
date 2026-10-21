package io.webrtc.signaling.app.runtime.local;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.mock.env.MockEnvironment;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/** TEST_ONLY documents: reserved addresses, no deployable values. */
final class LocalRuntimeFixtures {
    static final ObjectMapper JSON = new ObjectMapper().findAndRegisterModules();
    static final UUID POD_UID = UUID.fromString("00000000-0000-4000-8000-000000000001");

    private LocalRuntimeFixtures() {
    }

    /** Writes provider.json and process.json into {@code mount}; returns the matching environment. */
    static MockEnvironment install(Path mount, String plane) throws Exception {
        Files.writeString(mount.resolve("provider.json"), JSON.writeValueAsString(contract(mount)));
        Files.writeString(mount.resolve("process.json"), JSON.writeValueAsString(profile(mount, plane)));
        Files.writeString(mount.resolve("kube-ca.crt"), "TEST_ONLY");
        Files.writeString(mount.resolve("kube-token"), "TEST_ONLY");
        return environment(mount);
    }

    static MockEnvironment environment(Path mount) {
        return new MockEnvironment()
                .withProperty("SIGNALING_DEPLOYMENT_MODE", "local-minikube")
                .withProperty("SIGNALING_LOCAL_ACKNOWLEDGEMENT", "LOCAL_TEST_ONLY")
                .withProperty("SIGNALING_NATIVE_PROVIDER_CONTRACT", mount.resolve("provider.json").toString())
                .withProperty("SIGNALING_LOCAL_PROCESS_PROFILE", mount.resolve("process.json").toString())
                .withProperty("SIGNALING_POD_UID", POD_UID.toString())
                .withProperty("SIGNALING_CELL_ID", "c001");
    }

    static Map<String, Object> profile(Path mount, String plane) {
        var document = new LinkedHashMap<String, Object>();
        document.put("schemaVersion", 1);
        document.put("plane", plane);
        document.put("podUid", POD_UID.toString());
        document.put("podIp", "192.0.2.10");
        document.put("businessPort", 8443);
        document.put("healthPort", 9090);
        document.put("relayPort", 9443);
        document.put("azRole", "az-a");
        document.put("gatewayId", null);
        document.put("gatewayBootId", null);
        var wal = new LinkedHashMap<String, Object>();
        wal.put("systemIdentifier", "7000000000000000001");
        wal.put("timeline", 1);
        wal.put("replicas", List.of("replica-a"));
        wal.put("synchronousReplicas", 1);
        wal.put("maximumLagBytes", 1048576);
        wal.put("freshness", "PT1S");
        wal.put("maximumReplyAge", "PT2S");
        document.put("wal", wal);
        var compatibility = new LinkedHashMap<String, Object>();
        compatibility.put("major", 0);
        compatibility.put("minor", 1);
        compatibility.put("executableId", "TEST_ONLY_EXECUTABLE");
        compatibility.put("features", List.of("test-only"));
        compatibility.put("lowerMinor", 0);
        compatibility.put("upperMinor", 1);
        document.put("compatibility", compatibility);
        var kube = new LinkedHashMap<String, Object>();
        kube.put("apiHost", "kubernetes.default.svc");
        kube.put("apiPort", 443);
        kube.put("caPath", mount.resolve("kube-ca.crt").toString());
        kube.put("tokenPath", mount.resolve("kube-token").toString());
        kube.put("namespace", "signaling");
        document.put("kube", kube);
        document.put("transferCells", new ArrayList<>());
        document.put("privacyPseudonymKey", "TEST_ONLY_PRIVATE_PSEUDONYM_KEY");
        document.put("allowedOrigins", List.of("https://app.example.test"));
        return document;
    }

    static Map<String, Object> contract(Path mount) {
        var document = new LinkedHashMap<String, Object>();
        document.put("schemaVersion", 1);
        document.put("qualification", "LOCAL_TEST_ONLY");
        document.put("cell", "c001");
        document.put("storageEpoch", 1);
        document.put("routingEpoch", 1);
        document.put("environment", "local-test-only");
        document.put(
                "identity",
                Map.of(
                        "issuer", "LOCAL_TEST_ONLY_ISSUER",
                        "audience", "LOCAL_TEST_ONLY_AUDIENCE",
                        "maximumJwtLifetime", "PT15M",
                        "clockSkew", "PT30S",
                        "revocationPropagationSLO", "PT1S",
                        "hardSafetyBound", "PT5S",
                        "preserveJtiOnRefresh", true,
                        "highWaterSource", "LOCAL_TEST_ONLY_LOG"));
        var database = database("c001");
        document.put("cellDatabase", database);
        document.put("directoryDatabase", database);
        document.put(
                "tls",
                Map.of(
                        "ca", file(mount, "ca.crt"),
                        "certificate", file(mount, "pod/leaf.crt"),
                        "privateKey", file(mount, "pod/leaf.key"),
                        "remotingKeyStore", file(mount, "pod/remoting.p12"),
                        "remotingTrustStore", file(mount, "trust.p12")));
        document.put(
                "sources",
                Map.of(
                        "clockUrl", "https://source.platform.svc/v1/c001/clock",
                        "revocationUrl", "https://source.platform.svc/v1/c001/revocations",
                        "publicKeys", Map.of("source", file(mount, "source.pub"))));
        document.put(
                "proofs",
                Map.of(
                        "keyId", "local",
                        "privateKey", file(mount, "proof.key"),
                        "publicKeys", Map.of("c001/local", file(mount, "proof.pub"))));
        document.put("jwtPublicKeys", Map.of("rsa", file(mount, "rsa.pub")));
        document.put(
                "cells",
                List.of(
                        Map.of(
                                "cell", "c001",
                                "host", "actor.c001.svc",
                                "port", 8443,
                                "tlsAuthority", "actor.c001.svc",
                                "storageEpoch", 1)));
        document.put(
                "gateways",
                List.of(
                        Map.of(
                                "cell", "c001",
                                "gatewayId", "local-c001",
                                "host", "gateway.c001.svc",
                                "port", 9443,
                                "tlsAuthority", "gateway.c001.svc")));
        document.put("directorySnapshot", file(mount, "directory.json"));
        document.put("directorySnapshotSha256", "a".repeat(64));
        return document;
    }

    static Map<String, Object> database(String cell) {
        return new LinkedHashMap<>(
                Map.of(
                        "jdbcUrl",
                        "jdbc:postgresql://postgres." + cell + ".svc:5432/signaling?sslmode=verify-full",
                        "username", "signaling",
                        "password", "TEST_ONLY_PRIVATE_PASSWORD",
                        "quotas",
                        Map.of(
                                "CRITICAL", 2, "NORMAL", 1, "OUTBOX", 1, "MAINTENANCE", 1,
                                "RENEWAL", 1, "TERMINATION", 1, "RECOVERY", 2),
                        "safetyMax", 4,
                        "controlMax", 5));
    }

    static Map<String, Object> transferCell(String cell, long storageEpoch) {
        var document = new LinkedHashMap<String, Object>();
        document.put("cell", cell);
        document.put("storageEpoch", storageEpoch);
        document.put("database", database(cell));
        return document;
    }

    private static String file(Path mount, String name) {
        return mount.resolve(name).toString();
    }
}
