package io.webrtc.signaling.storage;

import io.webrtc.signaling.protocol.Identity.UserId;
import io.webrtc.signaling.protocol.internal.CompatibilityObservationReply;
import io.webrtc.signaling.rpc.CellRpcClient;
import io.webrtc.signaling.rpc.RpcAdmission;
import io.webrtc.signaling.rpc.RpcTlsContexts;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.io.TempDir;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * One LOCAL_TEST_ONLY cell composed exclusively from mounted files and process environment: six
 * actor JVMs (az-a x3, az-b x3), one gateway and one control, each a separate JVM running the
 * real {@code SignalingApplication} main. Separate processes are required: every actor uses the
 * same ActorSystem name {@code signaling-c001}, the same Artery and management ports and
 * process-wide Pekko and Spring state (Ruling 25).
 *
 * <p>Only the outside services are TEST_ONLY: PostgreSQL with a real synchronous streaming
 * standby, the signed clock/revocation source, the fake Kubernetes API and the PKI. Nothing here is
 * deployment, capacity or HA evidence.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class LocalRuntimeCompositionIT {
    static final Duration ACTOR_READY = Duration.ofSeconds(90);
    @TempDir
    static Path temporary;
    LocalCompositionFixture fixture;
    final List<LocalCompositionFixture.Started> cell = new ArrayList<>();
    LocalCompositionFixture.Started control;
    LocalCompositionFixture.Started gateway;
    List<LocalCompositionFixture.Actor> actors;
    final List<LocalCompositionFixture.Started> actorProcesses = new ArrayList<>();
    java.util.UUID controlPod;
    boolean controlUnreadyWithLiveSourcesBeforeActors;
    /** Transient INVITE replies tolerated before the committed result (same requestId). */
    static final int MAX_TRANSIENT_INVITE_REPLIES = 5;
    /** Lines the packaged startup-failure renderer prints; none may appear in a started plane. */
    static final List<String> STARTUP_FAILURES =
            List.of(
                    "\"errorCode\":\"STARTUP_FAILED\"",
                    "\"errorCode\":\"LOCAL_RUNTIME_REJECTED\"",
                    "\"errorCode\":\"NATIVE_RUNTIME_NOT_INSTALLED\"",
                    "Native runtime not installed");

    @BeforeAll
    void startOutsideServices() throws Exception {
        fixture = new LocalCompositionFixture(temporary, Path.of("target/local-composition"));
    }

    @AfterAll
    void stopEverything() throws Exception {
        if (fixture != null) fixture.close();
    }

    @Test
    @Order(1)
    void oneCellStartsAllPlanesFromMountedFilesOnly() throws Exception {
        actors = fixture.actors(10, List.of("az-a", "az-a", "az-a", "az-b", "az-b", "az-b"));
        var entry = actors.getFirst().ip();
        fixture.services.pods(actors.stream().map(LocalCompositionFixture.Actor::ip).toList());

        // Control first: it must come up but stay unready until real actor observations arrive.
        var controlMount = fixture.mountControl(entry);
        control =
                fixture.launch(
                        "control",
                        fixture.environment("control", controlMount, podUid(controlMount)),
                        LocalCompositionFixture.CONTROL_IP,
                        fixture.controlHealthPort);
        cell.add(control);
        await(control, Duration.ofSeconds(60), status -> status == 503);
        // Both safety sources are live for control, so only compatibility keeps it unready.
        controlPod = java.util.UUID.fromString(podUid(controlMount));
        org.awaitility.Awaitility.await()
                .atMost(Duration.ofSeconds(20))
                .until(
                        () ->
                                fixture.services.clockPods.contains(controlPod)
                                        && fixture.services.revocationPods.contains(controlPod));
        Thread.sleep(3000);
        controlUnreadyWithLiveSourcesBeforeActors =
                LocalCompositionFixture.ready(LocalCompositionFixture.CONTROL_IP, fixture.controlHealthPort) == 503;

        // Six actors at once: the 30 s formation budget starts with each process.
        for (var actor : actors) {
            var mount = fixture.mountActor(actor, entry, List.of("az-a", "az-b"));
            var started =
                    fixture.launch(actor.name(), fixture.actorEnvironment(actor, mount), actor.ip(), actor.healthPort());
            cell.add(started);
            actorProcesses.add(started);
        }
        for (var started : actorProcesses) await(started, ACTOR_READY, status -> status == 200);
        assertThat(control.process().isAlive()).isTrue();

        var gatewayMount = fixture.mountGateway(entry);
        gateway =
                fixture.launch(
                        "gateway",
                        fixture.environment("gateway", gatewayMount, podUid(gatewayMount)),
                        LocalCompositionFixture.GATEWAY_IP,
                        fixture.gatewayHealthPort);
        cell.add(gateway);
        await(gateway, Duration.ofSeconds(60), status -> status == 200);
        await(control, Duration.ofSeconds(30), status -> status == 200);

        var members = observeMembers(entry);
        assertThat(members.getActorsList()).hasSize(6);
        assertThat(members.getActorsList().stream().collect(Collectors.groupingBy(a -> a.getZone(), Collectors.counting())))
                .containsExactlyInAnyOrderEntriesOf(Map.of("az-a", 3L, "az-b", 3L));

        var user = userInCell("bootstrap-user");
        var response = bootstrap(fixture.token(user, Instant.now()));
        assertThat(response.statusCode()).as(response.body()).isEqualTo(200);
        var body = LocalCompositionFixture.JSON.readTree(response.body());
        assertThat(body.path("wssUrl").asText())
                .isEqualTo(LocalCompositionFixture.wssUrl(LocalCompositionFixture.CELL, fixture.gatewayWssPort));
        assertThat(body.path("directoryEpoch").asText()).isEqualTo("1");

        for (var started : cell) {
            assertThat(started.process().isAlive()).as(started.name() + "\n" + started.transcript()).isTrue();
            assertThat(started.transcript())
                    .as(started.name())
                    .doesNotContain(STARTUP_FAILURES.toArray(String[]::new));
        }
        assertThat(fixture.services.failure.get()).isNull();
        assertThat(fixture.services.kubeRequests.get()).isPositive();
    }

    /**
     * Ruling 10: the receiving side enforces TLSv1.3. A TLSv1.2-only client is refused with a
     * protocol_version alert by the actor management endpoint, Artery and the cell RPC ingress; a
     * TLSv1.3 client with a remoting identity completes the same handshakes.
     */
    @Test
    @Order(2)
    void actorListenersRejectTls12() throws Exception {
        requireComposedCell();
        var actor = actors.getFirst();
        var probe = fixture.pki.leaf(fixture.pki.remoting, "tls-probe", "IP:127.0.0.1");
        for (int port : List.of(8558, 25520)) {
            assertThat(handshake(actor.ip(), port, "TLSv1.2", LocalCompositionPki.context(probe, fixture.pki.remoting)))
                    .as("TLSv1.2 to %s:%d", actor.ip(), port)
                    .contains("protocol_version");
            assertThat(handshake(actor.ip(), port, "TLSv1.3", LocalCompositionPki.context(probe, fixture.pki.remoting)))
                    .as("TLSv1.3 to %s:%d", actor.ip(), port)
                    .isEqualTo("TLSv1.3");
        }
        assertThat(handshake(actor.ip(), fixture.entryPort, "TLSv1.2", LocalCompositionPki.context(null, fixture.pki.workload)))
                .contains("protocol_version");
    }

    /**
     * The actor's calling policy (NativeCallingPolicyConfiguration, since the installer passes a
     * null callingPolicy) admits an authenticated INVITE end to end: WSS AUTH at the gateway, cell
     * RPC, the actor and PostgreSQL; exactly one call row results from the retried intent.
     *
     * <p>Control readiness depends on the actors' real observations. Proven in reverse: with only
     * the six actors stopped, while control's signed clock and revocation polls keep succeeding,
     * control falls back to 503. The forward direction (503 with live sources before the actors,
     * 200 after) is consistent with this; on its own it would not exclude a slow source.
     */
    @Test
    @Order(3)
    void controlGateOpensFromActorsAndAnAuthenticatedInviteCommits() throws Exception {
        requireComposedCell();
        assertThat(controlUnreadyWithLiveSourcesBeforeActors).isTrue();
        assertThat(LocalCompositionFixture.ready(LocalCompositionFixture.CONTROL_IP, fixture.controlHealthPort))
                .isEqualTo(200);
        var caller = userInCell("caller");
        var callee = userInCell("callee");
        try (var callerSocket = new Wss(fixture, caller);
             var calleeSocket = new Wss(fixture, callee)) {
            assertThat(callerSocket.next().path("type").asText()).isEqualTo("AUTH_OK");
            assertThat(calleeSocket.next().path("type").asText()).isEqualTo("AUTH_OK");
            // One intent: a retry reuses the requestId, so it is the same command, never a second call.
            var invite =
                    "{\"v\":1,\"type\":\"INVITE\",\"requestId\":\""
                            + java.util.UUID.randomUUID()
                            + "\",\"payload\":{\"targetUserId\":\""
                            + callee
                            + "\"}}";
            com.fasterxml.jackson.databind.JsonNode result = null;
            var transient_ = new ArrayList<String>();
            for (int attempt = 0; attempt < 10; attempt++) {
                callerSocket.send(invite);
                result = callerSocket.next();
                var code = result.path("error").path("code").asText(result.path("type").asText());
                if (!List.of("OVERLOADED", "UNAVAILABLE", "OUTCOME_UNKNOWN").contains(code)) break;
                transient_.add(code);
                Thread.sleep(1000);
            }
            var retries = "transient replies before the result: " + transient_;
            assertThat(transient_).as(retries).hasSizeLessThanOrEqualTo(MAX_TRANSIENT_INVITE_REPLIES);
            assertThat(result.path("type").asText()).as(result + "; " + retries).isEqualTo("ACK_COMMITTED");
            assertThat(result.path("callId").asText()).as(retries).startsWith(LocalCompositionFixture.CELL + ".e1.");
            // call_id is the primary key; the retried intent must have produced one call in total.
            try (var c = fixture.database.admin(LocalCompositionDatabase.C001);
                 var q =
                         c.prepareStatement(
                                 "SELECT count(*),count(*) FILTER (WHERE caller_user=? AND call_id=?) FROM call_state")) {
                q.setString(1, caller);
                q.setString(2, result.path("callId").asText());
                try (var r = q.executeQuery()) {
                    r.next();
                    assertThat(r.getInt(1)).as("all calls; " + retries).isEqualTo(1);
                    assertThat(r.getInt(2)).as("this caller's call; " + retries).isEqualTo(1);
                }
            }
        }

        // Reverse dependence: stop only the actors; the sources keep answering control.
        fixture.stop(actorProcesses);
        await(control, Duration.ofSeconds(30), status -> status == 503);
        long now = System.nanoTime();
        assertThat(now - fixture.services.lastClockNanos.get(controlPod))
                .as("control clock poll age while unready")
                .isLessThan(TimeUnit.SECONDS.toNanos(2));
        assertThat(now - fixture.services.lastRevocationNanos.get(controlPod))
                .as("control revocation poll age while unready")
                .isLessThan(TimeUnit.SECONDS.toNanos(2));
        assertThat(fixture.services.failure.get()).isNull();
        assertThat(control.process().isAlive()).as(control.transcript()).isTrue();
    }

    /**
     * Review Focus 5: six actors that all carry {@code az-a} in a cell enrolled for {@code az-a}
     * and {@code az-b}. Every process starts and stays up, the cell forms, yet no actor becomes
     * ready (compatibility needs two zones), and the packaged log names the missing zone.
     */
    @Test
    @Order(4)
    void readinessStaysFalseWithOneAzRole() throws Exception {
        fixture.stop(List.copyOf(cell));
        cell.clear();
        var single = fixture.actors(50, List.of("az-a", "az-a", "az-a", "az-a", "az-a", "az-a"));
        fixture.services.pods(single.stream().map(LocalCompositionFixture.Actor::ip).toList());
        var started = new ArrayList<LocalCompositionFixture.Started>();
        for (var actor : single) {
            var mount = fixture.mountActor(actor, single.getFirst().ip(), List.of("az-a", "az-b"));
            started.add(fixture.launch(actor.name(), fixture.actorEnvironment(actor, mount), actor.ip(), actor.healthPort()));
        }
        cell.addAll(started);
        for (var actor : started) await(actor, ACTOR_READY, status -> status == 503);
        // The cell formed: the cell's own observation reports six Up members, all in az-a.
        var members = observeMembers(single.getFirst().ip());
        assertThat(members.getActorsList()).hasSize(6).allSatisfy(a -> assertThat(a.getZone()).isEqualTo("az-a"));
        // Formed and serving, but held unready for a sustained period: no flapping to 200.
        long end = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
        while (System.nanoTime() < end) {
            for (var actor : started)
                assertThat(LocalCompositionFixture.ready(actor.healthHost(), actor.healthPort()))
                        .as(actor.name())
                        .isEqualTo(503);
            Thread.sleep(1000);
        }
        for (var actor : started) {
            assertThat(actor.process().isAlive()).as(actor.name() + "\n" + actor.transcript()).isTrue();
            assertThat(actor.transcript()).as(actor.name()).doesNotContain(STARTUP_FAILURES.toArray(String[]::new));
            // The latest zone statement names exactly the missing zone.
            var zoneLines =
                    actor.transcript().lines().filter(line -> line.contains("\"missingZone\"")).toList();
            assertThat(zoneLines).as(actor.name()).isNotEmpty();
            assertThat(LocalCompositionFixture.JSON.readTree(zoneLines.getLast()).path("attributes").path("missingZone").asText())
                    .as(actor.name() + " last zone line " + zoneLines.getLast())
                    .isEqualTo("az-b");
        }
    }

    @Test
    @Order(5)
    void missingMountedContractIsRejectedWithItsRoleAndCategory() throws Exception {
        var mount = Files.createDirectories(temporary.resolve("absent-mount"));
        var env = fixture.environment("actor", mount, "00000000-0000-4000-8000-000000000001");
        var started = fixture.launch("missing-contract", env, "127.0.0.1", 1);
        assertThat(started.process().waitFor(60, TimeUnit.SECONDS)).isTrue();
        assertThat(started.process().exitValue()).isNotZero();
        assertThat(started.transcript())
                .contains("\"errorCode\":\"LOCAL_RUNTIME_REJECTED\",\"role\":\"contract\",\"category\":\"MISSING\"")
                .doesNotContain("STARTUP_FAILED", "NATIVE_RUNTIME_NOT_INSTALLED", mount.toString());
    }

    // ---------------------------------------------------------------- helpers

    /** Ordered tests share test 1's cell; say so instead of failing with a null pointer. */
    private void requireComposedCell() {
        if (actors == null || control == null || gateway == null || actorProcesses.size() != 6)
            org.junit.jupiter.api.Assertions.fail(
                    "oneCellStartsAllPlanesFromMountedFilesOnly did not compose the cell; this test depends on it");
    }

    /** The negotiated protocol, or the handshake failure message. */
    private static String handshake(String host, int port, String protocol, javax.net.ssl.SSLContext context) {
        try (var socket = (javax.net.ssl.SSLSocket) context.getSocketFactory().createSocket()) {
            socket.connect(new java.net.InetSocketAddress(host, port), 2000);
            socket.setSoTimeout(5000);
            socket.setEnabledProtocols(new String[]{protocol});
            socket.startHandshake();
            return socket.getSession().getProtocol();
        } catch (Exception failure) {
            return String.valueOf(failure.getMessage());
        }
    }

    /** One authenticated browser-like WSS session at the gateway. */
    static final class Wss implements AutoCloseable {
        private final HttpClient client;
        private final java.net.http.WebSocket socket;
        private final java.util.concurrent.BlockingQueue<String> frames =
                new java.util.concurrent.LinkedBlockingQueue<>();

        Wss(LocalCompositionFixture fixture, String user) throws Exception {
            client =
                    HttpClient.newBuilder()
                            .sslContext(LocalCompositionPki.context(null, fixture.pki.workload))
                            .connectTimeout(Duration.ofSeconds(3))
                            .build();
            socket =
                    client.newWebSocketBuilder()
                            .header("Origin", LocalCompositionFixture.ORIGIN)
                            .subprotocols("webrtc-signaling.v1")
                            .buildAsync(
                                    URI.create(
                                            "wss://" + LocalCompositionFixture.GATEWAY_IP + ":" + fixture.gatewayWssPort + "/ws"),
                                    new java.net.http.WebSocket.Listener() {
                                        final StringBuilder text = new StringBuilder();

                                        public java.util.concurrent.CompletionStage<?> onText(
                                                java.net.http.WebSocket ws, CharSequence data, boolean last) {
                                            text.append(data);
                                            if (last) {
                                                frames.add(text.toString());
                                                text.setLength(0);
                                            }
                                            ws.request(1);
                                            return null;
                                        }

                                        public java.util.concurrent.CompletionStage<?> onClose(
                                                java.net.http.WebSocket ws, int code, String reason) {
                                            frames.add("{\"type\":\"TEST_ONLY_CLOSED\",\"code\":" + code + "}");
                                            return null;
                                        }

                                        public void onError(java.net.http.WebSocket ws, Throwable error) {
                                            frames.add("{\"type\":\"TEST_ONLY_ERROR\"}");
                                        }
                                    })
                            .get(5, TimeUnit.SECONDS);
            send("{\"v\":1,\"type\":\"AUTH\",\"payload\":{\"token\":\"" + fixture.token(user, Instant.now()) + "\"}}");
        }

        void send(String frame) throws Exception {
            socket.sendText(frame, true).get(3, TimeUnit.SECONDS);
        }

        /** The next non-heartbeat frame. */
        com.fasterxml.jackson.databind.JsonNode next() throws Exception {
            long end = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
            while (System.nanoTime() < end) {
                var frame = frames.poll(1, TimeUnit.SECONDS);
                if (frame == null) continue;
                var node = LocalCompositionFixture.JSON.readTree(frame);
                if (!node.path("type").asText().equals("PING")) return node;
            }
            throw new AssertionError("No WSS frame within 10 s");
        }

        @Override
        public void close() {
            socket.abort();
            client.close();
        }
    }

    private static String podUid(Path mount) throws Exception {
        return LocalCompositionFixture.JSON.readTree(mount.resolve("process.json").toFile()).path("podUid").asText();
    }

    private static void await(
            LocalCompositionFixture.Started started, Duration limit, java.util.function.IntPredicate expected)
            throws Exception {
        long end = System.nanoTime() + limit.toNanos();
        int last = -1;
        while (System.nanoTime() < end) {
            if (!started.process().isAlive())
                throw new AssertionError(started.name() + " exited " + started.process().exitValue() + "\n" + started.transcript());
            last = LocalCompositionFixture.ready(started.healthHost(), started.healthPort());
            if (expected.test(last)) return;
            Thread.sleep(500);
        }
        throw new AssertionError(started.name() + " /ready was " + last + " after " + limit + "\n" + started.transcript());
    }

    /** An external control-identity observer: the cell's own report of its Up members and zones. */
    private CompatibilityObservationReply observeMembers(String entryHost) throws Exception {
        var leaf =
                fixture.pki.leaf(
                        fixture.pki.workload,
                        "observer",
                        "URI:" + LocalCompositionFixture.SPIFFE + LocalCompositionFixture.CELL + "/control/test-observer");
        var client =
                new CellRpcClient(
                        LocalCompositionFixture.ENVIRONMENT,
                        Map.of(
                                LocalCompositionFixture.CELL,
                                new CellRpcClient.Endpoint(
                                        entryHost,
                                        fixture.entryPort,
                                        "actor.c001.local-composition.test")),
                        RpcTlsContexts.controlClients(
                                LocalCompositionFixture.ENVIRONMENT,
                                LocalCompositionFixture.CELL,
                                fixture.pki.workload.certificate().toFile(),
                                leaf.certificate().toFile(),
                                leaf.key().toFile()),
                        new RpcAdmission(8, 1048576, 8, 1048576));
        try {
            CompatibilityObservationReply reply = null;
            long end = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
            while (System.nanoTime() < end) {
                var operation = client.observeCompatibility(LocalCompositionFixture.CELL, Duration.ofSeconds(2));
                try {
                    reply = operation.logical().toCompletableFuture().get(3, TimeUnit.SECONDS);
                    if (reply.getActorsCount() == 6) return reply;
                } catch (Exception unavailable) {
                    // Retry until the deadline.
                }
                operation.physicalCompletion().toCompletableFuture().get(3, TimeUnit.SECONDS);
                Thread.sleep(500);
            }
            throw new AssertionError("Six Up members never observed: " + reply);
        } finally {
            client.drain().toCompletableFuture().get(10, TimeUnit.SECONDS);
        }
    }

    private static String userInCell(String prefix) {
        for (int i = 0; ; i++) {
            var user = prefix + "-" + i;
            if (SessionRegistryService.bucket(new UserId(user)) < LocalCompositionDatabase.SPLIT) return user;
        }
    }

    private HttpResponse<String> bootstrap(String token) throws Exception {
        try (var client =
                     HttpClient.newBuilder()
                             .sslContext(LocalCompositionPki.context(null, fixture.pki.workload))
                             .connectTimeout(Duration.ofSeconds(2))
                             .build()) {
            return client.send(
                    HttpRequest.newBuilder(
                                    URI.create(
                                            "https://"
                                                    + LocalCompositionFixture.CONTROL_IP
                                                    + ":"
                                                    + fixture.controlPort
                                                    + "/v1/signaling/bootstrap"))
                            .timeout(Duration.ofSeconds(5))
                            .header("Authorization", "Bearer " + token)
                            .POST(HttpRequest.BodyPublishers.noBody())
                            .build(),
                    HttpResponse.BodyHandlers.ofString());
        }
    }
}
