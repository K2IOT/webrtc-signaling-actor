package io.webrtc.signaling.storage;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.File;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.security.Signature;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * Composes one LOCAL_TEST_ONLY cell from mounted files and environment only, as the compose
 * provisioner (SP3) will: per-process contract ({@code provider.json}), process profile
 * ({@code process.json}), directory snapshot, keys and certificates, plus the real OS environment
 * of a separate JVM. The application under test receives nothing else: no registered singleton, no
 * bean, no system property and no test class (the child classpath drops every test output).
 *
 * <p>The outside services (PostgreSQL primary and streaming standby, signed source, fake
 * Kubernetes API, PKI) are TEST_ONLY and never part of an image. Loopback addresses stand in for
 * the compose network: actors on {@code 127.0.0.11-16} (or {@code .51-56}), the gateway on
 * {@code .21}, control on {@code .22}, the source on {@code .30} and the Kubernetes API on
 * {@code .31}; Linux routes all of {@code 127.0.0.0/8} to the loopback interface.
 */
final class LocalCompositionFixture implements AutoCloseable {
    static final String CELL = "c001";
    static final String OTHER_CELL = "c002";
    static final String ENVIRONMENT = "local-test-only";
    static final String ISSUER = "https://issuer.local-composition.test";
    static final String AUDIENCE = "signaling-local-composition-test";
    static final String FINGERPRINT = sha256("TEST_ONLY_LOCAL_COMPOSITION_FINGERPRINT");
    static final String EXECUTABLE = sha256("TEST_ONLY_LOCAL_COMPOSITION_EXECUTABLE");
    static final String NAMESPACE = "test-only-c001";
    static final String GATEWAY_ID = "gw-c001-a";
    static final String JWT_KID = "local-rsa";
    static final String ORIGIN = "https://app.local-composition.test";
    static final String GATEWAY_IP = "127.0.0.21";
    static final String CONTROL_IP = "127.0.0.22";
    static final String SOURCE_IP = "127.0.0.30";
    static final String KUBE_IP = "127.0.0.31";
    static final String C002_IP = "127.0.0.41";
    static final String SPIFFE = "spiffe://signaling/" + ENVIRONMENT + "/cell/";
    static final List<String> FEATURES =
            List.of(
                    "session-s1",
                    "full-connection-active",
                    "causal-route-loss",
                    "native-renewal-v1",
                    "native-recovery-receipt-v1",
                    "native-wake-receipt-v1",
                    "native-control-receipt-v1",
                    "home-proof-high-water-v1",
                    "storage-domain-adoption-v1",
                    "native-actor-trace-v1",
                    "native-initial-ice-receipt-v1");
    static final ObjectMapper JSON = new ObjectMapper();
    /** {@code -Dlocal.composition.diagnostics=true} gives children readable logs; never in CI. */
    static final String DIAGNOSTICS = "local.composition.diagnostics";
    private static final java.util.Set<Process> CHILDREN = java.util.concurrent.ConcurrentHashMap.newKeySet();
    private static final java.util.concurrent.atomic.AtomicBoolean HOOK_INSTALLED =
            new java.util.concurrent.atomic.AtomicBoolean();

    final Path root;
    final Path logs;
    final LocalCompositionPki pki;
    final KeyPair sourceKeys;
    final KeyPair proofKeys;
    final KeyPair jwtKeys;
    final LocalCompositionDatabase database;
    final LocalCompositionServices services;
    final UUID gatewayBoot = UUID.randomUUID();
    final int gatewayWssPort;
    final int gatewayRelayPort;
    final int gatewayHealthPort;
    final int controlPort;
    final int controlHealthPort;
    final int c002Port;
    /** The cell RPC entry point every plane dials: the first actor's business listener. */
    final int entryPort;
    private final String kubeToken = "TEST_ONLY_KUBE_TOKEN_" + UUID.randomUUID();
    private final List<Started> started = new ArrayList<>();

    /** One process launched by the fixture, with its log. */
    record Started(String name, Process process, Path log, String healthHost, int healthPort) {
        String transcript() {
            try {
                return Files.readString(log, StandardCharsets.UTF_8);
            } catch (Exception unreadable) {
                return "<unreadable log " + log + ">";
            }
        }
    }

    /** One actor container: its address, zone role and ports. */
    record Actor(String name, String ip, String az, UUID pod, int businessPort, int healthPort, int relayPort) {
    }

    LocalCompositionFixture(Path root, Path logs) throws Exception {
        this.root = root;
        this.logs = logs;
        Files.createDirectories(logs);
        pki = LocalCompositionPki.create(root.resolve("pki"));
        sourceKeys = KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
        proofKeys = KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
        var rsa = KeyPairGenerator.getInstance("RSA");
        rsa.initialize(2048);
        jwtKeys = rsa.generateKeyPair();
        gatewayWssPort = freePort();
        gatewayRelayPort = freePort();
        gatewayHealthPort = freePort();
        controlPort = freePort();
        controlHealthPort = freePort();
        c002Port = freePort();
        entryPort = freePort();
        var pg = pki.leaf(pki.workload, "postgres", "DNS:localhost,IP:127.0.0.1");
        var source = pki.leaf(pki.workload, "source", "DNS:source.local-composition.test,IP:" + SOURCE_IP);
        var kube = pki.leaf(pki.workload, "kube-api", "IP:" + KUBE_IP);
        LocalCompositionDatabase db = null;
        try {
            db = new LocalCompositionDatabase(pg);
            db.seedDirectory(wssUrl(CELL, gatewayWssPort), wssUrl(OTHER_CELL, 8444));
            services =
                    new LocalCompositionServices(
                            source,
                            kube,
                            pki.workload,
                            sourceKeys,
                            CELL,
                            ISSUER,
                            new InetSocketAddress(SOURCE_IP, 0),
                            new InetSocketAddress(KUBE_IP, 0),
                            NAMESPACE,
                            kubeToken);
        } catch (Exception | AssertionError failure) {
            if (db != null) db.close();
            throw failure;
        }
        database = db;
    }

    static String wssUrl(String cell, int port) {
        return "wss://" + cell + ".signaling.local-composition.test:" + port + "/ws";
    }

    /** Six actors on {@code base+1 .. base+6}; the first takes the cell RPC entry port. */
    List<Actor> actors(int base, List<String> zones) throws Exception {
        var actors = new ArrayList<Actor>();
        for (int i = 0; i < zones.size(); i++)
            actors.add(
                    new Actor(
                            "actor-" + (base + i + 1),
                            "127.0.0." + (base + i + 1),
                            zones.get(i),
                            UUID.randomUUID(),
                            i == 0 ? entryPort : freePort(),
                            freePort(),
                            freePort()));
        return actors;
    }

    // ---------------------------------------------------------------- mounts

    Path mountActor(Actor actor, String entryHost, List<String> cellZones) throws Exception {
        var mount = mount(actor.name());
        var workload =
                pki.leaf(
                        pki.workload,
                        actor.name() + "-workload",
                        "URI:" + SPIFFE + CELL + "/actor,DNS:actor.c001.local-composition.test,IP:" + actor.ip());
        install(workload, mount);
        var remoting = pki.leaf(pki.remoting, actor.name() + "-remoting", "IP:" + actor.ip());
        pki.keyStore(remoting, pki.remoting, mount.resolve("remoting.p12"));
        pki.trustStore(pki.remoting, mount.resolve("trust.p12"));
        LocalCompositionPki.writeKeyPair(proofKeys, null, mount.resolve("proof.key"));
        Files.copy(pki.workload.certificate(), mount.resolve("kube-ca.crt"));
        Files.writeString(mount.resolve("kube-token"), kubeToken);
        var privacy = new byte[32];
        new SecureRandom().nextBytes(privacy);
        Files.write(mount.resolve("privacy.key"), privacy);
        var profile = profile("ACTOR", actor.pod(), actor.ip(), actor.businessPort(), actor.healthPort(), actor.relayPort());
        profile.put("azRole", actor.az());
        profile.put("cellAzRoles", cellZones);
        profile.put("wal", wal());
        var kube = new LinkedHashMap<String, Object>();
        kube.put("apiHost", KUBE_IP);
        kube.put("apiPort", services.kubePort());
        kube.put("caPath", mount.resolve("kube-ca.crt").toString());
        kube.put("tokenPath", mount.resolve("kube-token").toString());
        kube.put("namespace", NAMESPACE);
        profile.put("kube", kube);
        profile.put("privacyPseudonymKey", mount.resolve("privacy.key").toString());
        write(mount, contract(mount, entryHost), profile);
        return mount;
    }

    Path mountGateway(String entryHost) throws Exception {
        var mount = mount("gateway");
        var workload =
                pki.leaf(
                        pki.workload,
                        "gateway-workload",
                        "URI:" + SPIFFE + CELL + "/gateway/" + GATEWAY_ID
                                + ",DNS:gateway.c001.local-composition.test,DNS:c001.signaling.local-composition.test,IP:"
                                + GATEWAY_IP);
        install(workload, mount);
        var profile = profile("GATEWAY", UUID.randomUUID(), GATEWAY_IP, gatewayWssPort, gatewayHealthPort, gatewayRelayPort);
        profile.put("gatewayId", GATEWAY_ID);
        profile.put("gatewayBootId", gatewayBoot.toString());
        profile.put("allowedOrigins", List.of(ORIGIN));
        write(mount, contract(mount, entryHost), profile);
        return mount;
    }

    Path mountControl(String entryHost) throws Exception {
        var mount = mount("control");
        var workload =
                pki.leaf(
                        pki.workload,
                        "control-workload",
                        "URI:" + SPIFFE + CELL + "/control,DNS:bootstrap.signaling.local-composition.test,IP:" + CONTROL_IP);
        install(workload, mount);
        var profile = profile("CONTROL", UUID.randomUUID(), CONTROL_IP, controlPort, controlHealthPort, freePort());
        var transfer = new LinkedHashMap<String, Object>();
        transfer.put("cell", OTHER_CELL);
        transfer.put("storageEpoch", 1);
        transfer.put("database", database(LocalCompositionDatabase.C002, mount));
        profile.put("transferCells", List.of(transfer));
        write(mount, contract(mount, entryHost), profile);
        return mount;
    }

    private Path mount(String name) throws Exception {
        var mount = root.resolve("mounts").resolve(name);
        Files.createDirectories(mount);
        return mount;
    }

    /** Files every plane shares: the workload CA and leaf, source/proof/JWT public keys. */
    private void install(LocalCompositionPki.Leaf workload, Path mount) throws Exception {
        Files.copy(pki.workload.certificate(), mount.resolve("ca.crt"));
        Files.copy(workload.certificate(), mount.resolve("leaf.crt"));
        Files.copy(workload.key(), mount.resolve("leaf.key"));
        LocalCompositionPki.writeKeyPair(sourceKeys, mount.resolve("source.pub"), null);
        LocalCompositionPki.writeKeyPair(proofKeys, mount.resolve("proof.pub"), null);
        LocalCompositionPki.writeKeyPair(jwtKeys, mount.resolve("jwt.pub"), null);
    }

    private void write(Path mount, Map<String, Object> contract, Map<String, Object> profile) throws Exception {
        var snapshot = snapshot();
        Files.write(mount.resolve("directory.json"), snapshot);
        contract.put("directorySnapshotSha256", HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(snapshot)));
        Files.writeString(mount.resolve("provider.json"), JSON.writeValueAsString(contract));
        Files.writeString(mount.resolve("process.json"), JSON.writeValueAsString(profile));
    }

    private byte[] snapshot() throws Exception {
        var document = new LinkedHashMap<String, Object>();
        document.put("schemaVersion", 1);
        document.put("qualification", "LOCAL_TEST_ONLY");
        document.put(
                "buckets",
                List.of(
                        range(0, LocalCompositionDatabase.SPLIT - 1, CELL),
                        range(LocalCompositionDatabase.SPLIT, 16383, OTHER_CELL)));
        var boot = new LinkedHashMap<String, Object>();
        boot.put("cell", CELL);
        boot.put("gatewayId", GATEWAY_ID);
        boot.put("bootId", gatewayBoot.toString());
        document.put("gatewayBoots", List.of(boot));
        return JSON.writeValueAsBytes(document);
    }

    private static Map<String, Object> range(int first, int last, String cell) {
        var range = new LinkedHashMap<String, Object>();
        range.put("first", first);
        range.put("last", last);
        range.put("cell", cell);
        range.put("directoryEpoch", 1);
        range.put("storageEpoch", 1);
        return range;
    }

    private Map<String, Object> contract(Path mount, String entryHost) {
        var document = new LinkedHashMap<String, Object>();
        document.put("schemaVersion", 1);
        document.put("qualification", "LOCAL_TEST_ONLY");
        document.put("cell", CELL);
        document.put("storageEpoch", 1);
        document.put("routingEpoch", 1);
        document.put("environment", ENVIRONMENT);
        var identity = new LinkedHashMap<String, Object>();
        identity.put("issuer", ISSUER);
        identity.put("audience", AUDIENCE);
        identity.put("maximumJwtLifetime", "PT15M");
        identity.put("clockSkew", "PT30S");
        identity.put("revocationPropagationSLO", "PT1S");
        identity.put("hardSafetyBound", "PT5S");
        identity.put("preserveJtiOnRefresh", true);
        identity.put("highWaterSource", "TEST_ONLY_LOCAL_SOURCE");
        document.put("identity", identity);
        document.put("cellDatabase", database(LocalCompositionDatabase.C001, mount));
        document.put("directoryDatabase", database(LocalCompositionDatabase.DIRECTORY, mount));
        var tls = new LinkedHashMap<String, Object>();
        tls.put("ca", file(mount, "ca.crt"));
        tls.put("certificate", file(mount, "leaf.crt"));
        tls.put("privateKey", file(mount, "leaf.key"));
        tls.put("remotingKeyStore", file(mount, "remoting.p12"));
        tls.put("remotingTrustStore", file(mount, "trust.p12"));
        document.put("tls", tls);
        var sourceUrl = "https://" + SOURCE_IP + ":" + services.sourcePort();
        var sources = new LinkedHashMap<String, Object>();
        sources.put("clockUrl", sourceUrl + "/v1/clock-bound");
        sources.put("revocationUrl", sourceUrl + "/v1/revocations");
        sources.put("publicKeys", Map.of(LocalCompositionServices.SOURCE_KEY_ID, file(mount, "source.pub")));
        document.put("sources", sources);
        var proofs = new LinkedHashMap<String, Object>();
        proofs.put("keyId", "local");
        proofs.put("privateKey", file(mount, "proof.key"));
        proofs.put("publicKeys", Map.of(CELL + "/local", file(mount, "proof.pub")));
        document.put("proofs", proofs);
        document.put("jwtPublicKeys", Map.of(JWT_KID, file(mount, "jwt.pub")));
        document.put(
                "cells",
                List.of(
                        cell(CELL, entryHost, entryPort),
                        cell(OTHER_CELL, C002_IP, c002Port)));
        var gateway = new LinkedHashMap<String, Object>();
        gateway.put("cell", CELL);
        gateway.put("gatewayId", GATEWAY_ID);
        gateway.put("host", GATEWAY_IP);
        gateway.put("port", gatewayRelayPort);
        gateway.put("tlsAuthority", "gateway.c001.local-composition.test");
        document.put("gateways", List.of(gateway));
        document.put("directorySnapshot", file(mount, "directory.json"));
        return document;
    }

    private static Map<String, Object> cell(String cell, String host, int port) {
        var target = new LinkedHashMap<String, Object>();
        target.put("cell", cell);
        target.put("host", host);
        target.put("port", port);
        target.put("tlsAuthority", "actor." + cell + ".local-composition.test");
        target.put("storageEpoch", 1);
        return target;
    }

    /** Count-only quotas: the LOCAL_TEST_ONLY resource profiles are derived by the parser. */
    private Map<String, Object> database(String name, Path mount) {
        var database = new LinkedHashMap<String, Object>();
        database.put("jdbcUrl", this.database.jdbcUrl(name, mount.resolve("ca.crt")));
        database.put("username", LocalCompositionDatabase.USER);
        database.put("password", LocalCompositionDatabase.PASSWORD);
        database.put(
                "quotas",
                Map.of(
                        "CRITICAL", 8, "NORMAL", 2, "OUTBOX", 2, "MAINTENANCE", 2,
                        "RENEWAL", 2, "TERMINATION", 2, "RECOVERY", 4));
        database.put("safetyMax", 8);
        database.put("controlMax", 14);
        return database;
    }

    private Map<String, Object> wal() {
        var wal = new LinkedHashMap<String, Object>();
        wal.put("systemIdentifier", database.systemIdentifier);
        wal.put("timeline", database.timeline);
        wal.put("replicas", List.of(LocalCompositionDatabase.STANDBY));
        wal.put("synchronousReplicas", 1);
        wal.put("maximumLagBytes", 16777216);
        wal.put("freshness", "PT1S");
        wal.put("maximumReplyAge", "PT5S");
        return wal;
    }

    private static LinkedHashMap<String, Object> profile(
            String plane, UUID pod, String ip, int business, int health, int relay) {
        var document = new LinkedHashMap<String, Object>();
        document.put("schemaVersion", 1);
        document.put("plane", plane);
        document.put("podUid", pod.toString());
        document.put("podIp", ip);
        document.put("businessPort", business);
        document.put("healthPort", health);
        document.put("relayPort", relay);
        document.put("azRole", null);
        document.put("cellAzRoles", List.of());
        document.put("gatewayId", null);
        document.put("gatewayBootId", null);
        document.put("wal", null);
        var compatibility = new LinkedHashMap<String, Object>();
        compatibility.put("major", 1);
        compatibility.put("minor", 1);
        compatibility.put("executableId", EXECUTABLE);
        compatibility.put("features", FEATURES);
        compatibility.put("lowerMinor", 0);
        compatibility.put("upperMinor", 1);
        document.put("compatibility", compatibility);
        document.put("kube", null);
        document.put("transferCells", List.of());
        document.put("privacyPseudonymKey", null);
        document.put("allowedOrigins", List.of());
        return document;
    }

    private static String file(Path mount, String name) {
        return mount.resolve(name).toString();
    }

    // ---------------------------------------------------------------- environment and processes

    Map<String, String> environment(String plane, Path mount, String pod) {
        var env = new LinkedHashMap<String, String>();
        env.put("SPRING_PROFILES_ACTIVE", plane);
        env.put("SIGNALING_DEPLOYMENT_MODE", "local-minikube");
        env.put("SIGNALING_LOCAL_ACKNOWLEDGEMENT", "LOCAL_TEST_ONLY");
        env.put("SIGNALING_CELL_ID", CELL);
        env.put("SIGNALING_CLUSTER_FINGERPRINT", FINGERPRINT);
        env.put("SIGNALING_IDENTITY_ISSUER", ISSUER);
        env.put("SIGNALING_IDENTITY_AUDIENCE", AUDIENCE);
        env.put("SIGNALING_NATIVE_PROVIDER_CONTRACT", mount.resolve("provider.json").toString());
        env.put("SIGNALING_LOCAL_PROCESS_PROFILE", mount.resolve("process.json").toString());
        env.put("SIGNALING_POD_UID", pod);
        return env;
    }

    Map<String, String> actorEnvironment(Actor actor, Path mount) {
        var env = environment("actor", mount, actor.pod().toString());
        env.put("SIGNALING_POD_IP", actor.ip());
        env.put("SIGNALING_NAMESPACE", NAMESPACE);
        env.put("SIGNALING_ACTOR_SERVICE", "signaling-" + CELL);
        env.put("SIGNALING_DISCOVERY_SELECTOR", "app=webrtc-signaling,plane=actor,cell=" + CELL);
        env.put("SIGNALING_KUBE_API_HOST", KUBE_IP);
        env.put("SIGNALING_KUBE_API_PORT", Integer.toString(services.kubePort()));
        env.put("SIGNALING_REMOTING_KEYSTORE", mount.resolve("remoting.p12").toString());
        env.put("SIGNALING_REMOTING_TRUSTSTORE", mount.resolve("trust.p12").toString());
        env.put("SIGNALING_REMOTING_KEYSTORE_PASSWORD", LocalCompositionPki.STORE_PASSWORD);
        env.put("SIGNALING_REMOTING_KEY_PASSWORD", LocalCompositionPki.STORE_PASSWORD);
        env.put("SIGNALING_REMOTING_TRUSTSTORE_PASSWORD", LocalCompositionPki.STORE_PASSWORD);
        env.put("SIGNALING_FORMATION_PROFILE", "initial-six");
        return env;
    }

    /**
     * Launches {@code SignalingApplication} in its own JVM with exactly {@code environment} (the
     * inherited environment is cleared) and a classpath without any test output.
     */
    Started launch(String name, Map<String, String> environment, String healthHost, int healthPort)
            throws Exception {
        var tmp = root.resolve("tmp").resolve(name);
        Files.createDirectories(tmp);
        var log = logs.resolve(name + ".log");
        var command =
                new ArrayList<>(
                        List.of(
                                Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                                "--add-exports=java.base/jdk.internal.misc=ALL-UNNAMED",
                                "-Xms64m",
                                "-Xmx384m",
                                "-XX:+ExitOnOutOfMemoryError",
                                "-XX:TieredStopAtLevel=1",
                                "-XX:+UseSerialGC",
                                "-Djava.io.tmpdir=" + tmp));
        // Diagnostics only, off by default: the packaged layout drops messages and exceptions.
        if (Boolean.getBoolean(DIAGNOSTICS)) command.add("-Dlogging.config=" + diagnosticLogging());
        command.addAll(List.of("-cp", applicationClasspath(), "io.webrtc.signaling.app.SignalingApplication"));
        var builder = new ProcessBuilder(command);
        builder.environment().clear();
        builder.environment().putAll(environment);
        builder.redirectErrorStream(true).redirectOutput(log.toFile());
        var process = builder.start();
        guard(process);
        var result = new Started(name, process, log, healthHost, healthPort);
        synchronized (started) {
            started.add(result);
        }
        return result;
    }

    /**
     * Children bind fixed ports (25520/8558 on 127.0.0.11-16 and .51-56), so none may outlive the
     * test JVM. A JVM shutdown hook kills every child and its descendants; for a test JVM killed
     * with SIGKILL (no hooks run), a separate watchdog kills the child once the parent pid is gone,
     * and only then. The watchdog lives outside the application under test.
     */
    private static void guard(Process child) throws Exception {
        CHILDREN.add(child);
        if (HOOK_INSTALLED.compareAndSet(false, true))
            Runtime.getRuntime()
                    .addShutdownHook(
                            new Thread(
                                    () ->
                                            CHILDREN.forEach(
                                                    process -> {
                                                        process.descendants().forEach(ProcessHandle::destroyForcibly);
                                                        process.destroyForcibly();
                                                    }),
                                    "TEST_ONLY_local_composition_reaper"));
        long parent = ProcessHandle.current().pid();
        new ProcessBuilder(
                "sh",
                "-c",
                "while kill -0 " + parent + " 2>/dev/null && kill -0 " + child.pid()
                        + " 2>/dev/null; do sleep 1; done; "
                        // The loop also ends when a test stopped the child itself; its pid may then be
                        // reused, so the child is signalled only when the parent is really gone.
                        + "kill -0 " + parent + " 2>/dev/null || kill -9 " + child.pid()
                        + " 2>/dev/null; exit 0")
                .redirectErrorStream(true)
                .redirectOutput(ProcessBuilder.Redirect.DISCARD)
                .start();
    }

    /** Plain-text logging with messages and stack traces, for a developer re-run only. */
    private Path diagnosticLogging() throws Exception {
        var config = root.resolve("TEST_ONLY_diagnostic-logback.xml");
        if (!Files.exists(config))
            Files.writeString(
                    config,
                    "<configuration><appender name=\"OUT\" class=\"ch.qos.logback.core.ConsoleAppender\">"
                            + "<encoder><pattern>%d{HH:mm:ss.SSS} %-5level [%thread] %logger{36} - %msg %mdc%n</pattern></encoder>"
                            + "</appender><root level=\"INFO\"><appender-ref ref=\"OUT\"/></root></configuration>");
        return config;
    }

    /** The test JVM classpath minus test outputs: main classes and third-party jars only. */
    static String applicationClasspath() {
        var entries =
                Arrays.stream(System.getProperty("java.class.path").split(File.pathSeparator))
                        .filter(entry -> !entry.endsWith("test-classes") && !entry.contains("test-classes" + File.separator))
                        .filter(entry -> !entry.endsWith("-tests.jar"))
                        .filter(entry -> !entry.contains("surefire") && !entry.contains("failsafe"))
                        .toList();
        if (entries.stream().noneMatch(entry -> entry.contains("signaling-app")))
            throw new IllegalStateException("TEST_ONLY application classpath lacks signaling-app: " + entries);
        return String.join(File.pathSeparator, entries);
    }

    /** {@code -1} while the private probe does not answer. */
    static int ready(String host, int port) {
        try (var client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(1)).build()) {
            return client.send(
                            HttpRequest.newBuilder(URI.create("http://" + host + ":" + port + "/ready"))
                                    .timeout(Duration.ofSeconds(2))
                                    .GET()
                                    .build(),
                            HttpResponse.BodyHandlers.discarding())
                    .statusCode();
        } catch (Exception unavailable) {
            return -1;
        }
    }

    /** An RS256 token for {@code user}, signed by the fixture key the contract enrolls. */
    String token(String user, Instant now) throws Exception {
        var encoding = Base64.getUrlEncoder().withoutPadding();
        var header = "{\"alg\":\"RS256\",\"kid\":\"" + JWT_KID + "\",\"typ\":\"JWT\"}";
        var claims =
                "{\"iss\":\""
                        + ISSUER
                        + "\",\"aud\":\""
                        + AUDIENCE
                        + "\",\"userId\":\""
                        + user
                        + "\",\"jti\":\"TEST_ONLY_"
                        + UUID.randomUUID()
                        + "\",\"iat\":"
                        + now.getEpochSecond()
                        + ",\"exp\":"
                        + now.plusSeconds(600).getEpochSecond()
                        + "}";
        var input =
                encoding.encodeToString(header.getBytes(StandardCharsets.UTF_8))
                        + "."
                        + encoding.encodeToString(claims.getBytes(StandardCharsets.UTF_8));
        var signature = Signature.getInstance("SHA256withRSA");
        signature.initSign(jwtKeys.getPrivate());
        signature.update(input.getBytes(StandardCharsets.US_ASCII));
        return input + "." + encoding.encodeToString(signature.sign());
    }

    void stop(List<Started> processes) throws Exception {
        for (var process : processes) {
            process.process().descendants().forEach(ProcessHandle::destroyForcibly);
            process.process().destroyForcibly();
        }
        for (var process : processes) process.process().waitFor(20, TimeUnit.SECONDS);
        synchronized (started) {
            started.removeAll(processes);
        }
    }

    static int freePort() throws Exception {
        try (var socket = new ServerSocket(0, 1, InetAddress.getByName("0.0.0.0"))) {
            return socket.getLocalPort();
        }
    }

    static String sha256(String value) {
        try {
            return HexFormat.of()
                    .formatHex(MessageDigest.getInstance("SHA-256").digest(value.getBytes(StandardCharsets.UTF_8)));
        } catch (Exception impossible) {
            throw new IllegalStateException(impossible);
        }
    }

    @Override
    public void close() throws Exception {
        List<Started> remaining;
        synchronized (started) {
            remaining = List.copyOf(started);
        }
        try {
            stop(remaining);
        } finally {
            try {
                services.close();
            } finally {
                database.close();
            }
        }
    }
}
