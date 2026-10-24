package io.webrtc.signaling.localplatform;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.StreamReadConstraints;
import com.fasterxml.jackson.core.StreamReadFeature;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpsServer;
import io.webrtc.signaling.auth.ClockSafetyMonitor;
import io.webrtc.signaling.storage.worker.RevocationReconciler;
import io.webrtc.signaling.storage.worker.RevocationSourceVerifier;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.PrintStream;
import java.net.ConnectException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyPair;
import java.security.Signature;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocket;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.postgresql.ds.PGSimpleDataSource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * The platform over the wire: real listeners, TEST_ONLY PKI generated at test time, PostgreSQL in a
 * container. LOCAL_TEST_ONLY, NOT_QUALIFIED.
 */
@Testcontainers
class SourceServerIT {

  @Container
  static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:17.6");

  private static final String ISSUER = "https://issuer.test-only.invalid";
  private static final String KEY_ID = "local-source";
  private static final InetAddress LOOPBACK = InetAddress.getLoopbackAddress();
  private static final AtomicInteger DATABASES = new AtomicInteger();
  private static final ObjectMapper JSON = new ObjectMapper().findAndRegisterModules();

  /** Same parser configuration as the actor-side {@code NativeRevocationSource}. */
  private static final ObjectMapper CONSUMER =
      new ObjectMapper(
              JsonFactory.builder()
                  .enable(StreamReadFeature.STRICT_DUPLICATE_DETECTION)
                  .streamReadConstraints(
                      StreamReadConstraints.builder()
                          .maxNestingDepth(8)
                          .maxStringLength(8192)
                          .maxNumberLength(20)
                          .build())
                  .build())
          .findAndRegisterModules()
          .disable(MapperFeature.ALLOW_COERCION_OF_SCALARS)
          .enable(
              DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES,
              DeserializationFeature.FAIL_ON_MISSING_CREATOR_PROPERTIES,
              DeserializationFeature.FAIL_ON_TRAILING_TOKENS);

  @TempDir static Path pki;

  private static TestPki.Authority serverCa;
  private static TestPki.Authority workloadCa;
  private static TestPki.Authority adminCa;
  private static TestPki.Leaf actorA;
  private static TestPki.Leaf gatewayA;
  private static TestPki.Leaf unknownCell;
  private static TestPki.Leaf adminPathOnWorkloadCa;
  private static TestPki.Leaf admin;
  private static TestPki.Leaf workloadPathOnAdminCa;
  private static KeyPair signingKeys;
  private static Path signingKeyFile;
  private static Path sourceKeystore;
  private static Path adminKeystore;
  private static Path keystorePassword;
  private static Path workloadTrust;
  private static Path adminTrust;

  @TempDir Path dir;

  private String database;
  private LocalPlatformApplication.Running running;

  @BeforeAll
  static void generatePki() throws IOException {
    serverCa = TestPki.authority("server-ca");
    workloadCa = TestPki.authority("workload-ca");
    adminCa = TestPki.authority("admin-ca");
    String base = TestPki.BASE;
    actorA =
        workloadCa.issue(
            "actor-cell-a", TestPki.Usage.CLIENT, "uri:" + base + "/cell/cell-a/actor");
    gatewayA =
        workloadCa.issue(
            "gateway-cell-a", TestPki.Usage.CLIENT, "uri:" + base + "/cell/cell-a/gateway/gw-1");
    unknownCell =
        workloadCa.issue(
            "unknown-cell", TestPki.Usage.CLIENT, "uri:" + base + "/cell/cell-z/actor");
    adminPathOnWorkloadCa =
        workloadCa.issue("admin-path", TestPki.Usage.CLIENT, "uri:" + base + "/admin/ops");
    admin = adminCa.issue("admin-ops", TestPki.Usage.CLIENT, "uri:" + base + "/admin/ops");
    workloadPathOnAdminCa =
        adminCa.issue("workload-path", TestPki.Usage.CLIENT, "uri:" + base + "/cell/cell-a/actor");
    sourceKeystore =
        serverCa
            .issue("source", TestPki.Usage.SERVER, "dns:localhost", "ip:127.0.0.1")
            .writePkcs12(pki.resolve("source.p12"));
    adminKeystore =
        serverCa
            .issue("admin", TestPki.Usage.SERVER, "dns:localhost", "ip:127.0.0.1")
            .writePkcs12(pki.resolve("admin.p12"));
    keystorePassword =
        Files.writeString(pki.resolve("keystore.pass"), TestPki.KEYSTORE_PASSWORD + "\n");
    workloadTrust = workloadCa.writePem(pki.resolve("workload-ca.pem"));
    adminTrust = adminCa.writePem(pki.resolve("admin-ca.pem"));
    signingKeys = TestKeys.generate("Ed25519");
    signingKeyFile = TestKeys.writePkcs8Pem(pki, "signing.key", signingKeys);
  }

  @BeforeEach
  void startPlatform() throws Exception {
    database = createDatabase();
    running = LocalPlatformApplication.start(config(database));
  }

  @AfterEach
  void stopPlatform() {
    if (running != null) {
      running.close();
    }
  }

  // --- source ---------------------------------------------------------------------------------

  @Test
  void clockEndpointReturnsSignedReportForEnrolledActor() throws Exception {
    UUID pod = UUID.randomUUID();
    UUID boot = UUID.randomUUID();
    try (HttpClient client = client(TestPki.context(actorA, serverCa))) {
      HttpResponse<byte[]> response =
          client.send(
              HttpRequest.newBuilder(sourceUri("/v1/clock-bound"))
                  .header("X-Signaling-Pod-Uid", pod.toString())
                  .header("X-Signaling-Process-Boot", boot.toString())
                  .GET()
                  .build(),
              HttpResponse.BodyHandlers.ofByteArray());

      assertThat(response.statusCode()).isEqualTo(200);
      assertThat(response.headers().firstValue("content-type")).hasValue("application/json");
      JsonNode body = JSON.readTree(response.body());
      ClockSafetyMonitor.Report report =
          JSON.treeToValue(body.get("report"), ClockSafetyMonitor.Report.class);
      String signature = body.get("signature").asText();
      assertThat(signature).hasSize(86);
      Signature verifier = Signature.getInstance("Ed25519");
      verifier.initVerify(signingKeys.getPublic());
      verifier.update(ClockSafetyMonitor.signingBytes(report));
      assertThat(verifier.verify(Base64.getUrlDecoder().decode(signature))).isTrue();
      assertThat(report.keyId()).isEqualTo(KEY_ID);
      assertThat(report.cell()).isEqualTo("cell-a");
      assertThat(report.storageEpoch()).isEqualTo(3);
      assertThat(report.podUid()).isEqualTo(pod);
      assertThat(report.processBoot()).isEqualTo(boot);
      assertThat(Duration.between(report.observedAt(), report.validUntil()))
          .isEqualTo(Duration.ofSeconds(2));
    }
  }

  @Test
  void revocationEndpointReturnsEmptyCaughtUpPage() throws Exception {
    Reply reply = source(actorA, get("/v1/revocations", ids(UUID.randomUUID()), offset("0")));

    assertThat(reply.status()).isEqualTo(200);
    RevocationReconciler.Batch batch =
        CONSUMER.readValue(reply.body(), RevocationReconciler.Batch.class);
    assertThat(batch.fromOffset()).isZero();
    assertThat(batch.highWater()).isZero();
    assertThat(batch.currentSourceHighWater()).isZero();
    assertThat(batch.events()).isEmpty();
    assertThat(verifier("cell-a").test(batch)).isTrue();
    assertThat(verifier("cell-b").test(batch)).isFalse();
  }

  @Test
  void adminAppendThenSourceReturnsEvent() throws Exception {
    try (HttpClient client = client(TestPki.context(admin, serverCa))) {
      HttpResponse<String> first = adminPost(client, "{\"userId\":\"alice\"}");
      HttpResponse<String> second =
          adminPost(client, "{\"userId\":\"alice\",\"jti\":\"jti-1\",\"epoch\":5}");
      HttpResponse<String> conflict = adminPost(client, "{\"userId\":\"alice\",\"epoch\":0}");

      assertThat(first.statusCode()).isEqualTo(201);
      assertThat(JSON.readTree(first.body()))
          .isEqualTo(JSON.readTree("{\"offset\":1,\"epoch\":0}"));
      assertThat(first.headers().firstValue("content-type")).hasValue("application/json");
      assertThat(second.statusCode()).isEqualTo(201);
      assertThat(JSON.readTree(second.body()))
          .isEqualTo(JSON.readTree("{\"offset\":2,\"epoch\":5}"));
      assertThat(conflict.statusCode()).isEqualTo(409);
      assertThat(conflict.body()).isEmpty();
    }

    UUID pod = UUID.randomUUID();
    Reply page = source(actorA, get("/v1/revocations", ids(pod), offset("0")));
    Reply caughtUp = source(actorA, get("/v1/revocations", ids(pod), offset("2")));

    assertThat(page.status()).isEqualTo(200);
    RevocationReconciler.Batch batch =
        CONSUMER.readValue(page.body(), RevocationReconciler.Batch.class);
    assertThat(verifier("cell-a").test(batch)).isTrue();
    assertThat(batch.highWater()).isEqualTo(2);
    assertThat(batch.events()).hasSize(2);
    assertThat(batch.events().get(0).userId().value()).isEqualTo("alice");
    assertThat(batch.events().get(0).jti()).isNull();
    assertThat(batch.events().get(0).epoch()).isZero();
    assertThat(batch.events().get(0).issuer()).isEqualTo(ISSUER);
    assertThat(batch.events().get(1).jti()).isEqualTo("jti-1");
    assertThat(batch.events().get(1).epoch()).isEqualTo(5);
    RevocationReconciler.Batch end =
        CONSUMER.readValue(caughtUp.body(), RevocationReconciler.Batch.class);
    assertThat(end.fromOffset()).isEqualTo(2);
    assertThat(end.events()).isEmpty();
  }

  // --- TLS and roles --------------------------------------------------------------------------

  @Test
  void clientWithoutCertificateIsRejected() {
    SSLContext anonymous = TestPki.context(null, serverCa);

    assertThatThrownBy(() -> tls(running.sourcePort(), anonymous, clockRequest()))
        .isInstanceOf(IOException.class);
    assertThatThrownBy(() -> tls(running.adminPort(), anonymous, adminRequest("{}")))
        .isInstanceOf(IOException.class);
    try (HttpClient client = client(anonymous)) {
      assertThatThrownBy(
              () ->
                  client.send(
                      HttpRequest.newBuilder(sourceUri("/v1/clock-bound")).GET().build(),
                      HttpResponse.BodyHandlers.ofByteArray()))
          .isInstanceOf(IOException.class);
    }
  }

  @Test
  void tls12ClientIsRejected() throws Exception {
    SSLContext context = TestPki.context(actorA, serverCa);
    for (int port : new int[] {running.sourcePort(), running.adminPort()}) {
      try (SSLSocket socket = (SSLSocket) context.getSocketFactory().createSocket(LOOPBACK, port)) {
        socket.setEnabledProtocols(new String[] {"TLSv1.2"});
        socket.setSoTimeout(10_000);
        assertThatThrownBy(socket::startHandshake).isInstanceOf(IOException.class);
      }
    }

    // A failed handshake does not take the listener down.
    assertThat(source(actorA, clockRequest()).status()).isEqualTo(200);
  }

  @Test
  void adminCertificateOnSourcePortIsRefused() throws Exception {
    assertThatThrownBy(() -> source(admin, clockRequest())).isInstanceOf(IOException.class);

    // Admin URI on a certificate the source does trust: TLS passes, the identity does not.
    Reply reply = source(adminPathOnWorkloadCa, clockRequest());
    assertThat(reply.status()).isEqualTo(403);
    assertThat(reply.body()).isEmpty();
  }

  @Test
  void workloadCertificateOnAdminPortIsRefused() throws Exception {
    assertThatThrownBy(() -> adminCall(actorA, adminRequest("{\"userId\":\"alice\"}")))
        .isInstanceOf(IOException.class);

    Reply reply = adminCall(workloadPathOnAdminCa, adminRequest("{\"userId\":\"alice\"}"));
    assertThat(reply.status()).isEqualTo(403);
    assertThat(reply.body()).isEmpty();
    assertThat(highWater()).isZero();
  }

  @Test
  void podOfAnotherIdentityIsForbidden() throws Exception {
    UUID pod = UUID.randomUUID();
    UUID boot = UUID.randomUUID();
    UUID newerBoot = UUID.randomUUID();

    assertThat(source(actorA, get("/v1/clock-bound", ids(pod, boot))).status()).isEqualTo(200);

    Reply stolenClock = source(gatewayA, get("/v1/clock-bound", ids(pod, boot)));
    Reply stolenPage = source(gatewayA, get("/v1/revocations", ids(pod, boot), offset("0")));
    assertThat(stolenClock.status()).isEqualTo(403);
    assertThat(stolenClock.body()).isEmpty();
    assertThat(stolenPage.status()).isEqualTo(403);
    assertThat(stolenPage.body()).isEmpty();

    assertThat(source(actorA, get("/v1/clock-bound", ids(pod, newerBoot))).status()).isEqualTo(200);
    Reply staleBoot = source(actorA, get("/v1/clock-bound", ids(pod, boot)));
    assertThat(staleBoot.status()).isEqualTo(403);
    assertThat(staleBoot.body()).isEmpty();

    Reply foreignCell = source(unknownCell, clockRequest());
    assertThat(foreignCell.status()).isEqualTo(403);
    assertThat(foreignCell.body()).isEmpty();
  }

  @Test
  void malformedRequestsGet4xxWithEmptyBody() throws Exception {
    UUID pod = UUID.randomUUID();
    UUID boot = UUID.randomUUID();
    String pods = "X-Signaling-Pod-Uid: " + pod;
    String boots = "X-Signaling-Process-Boot: " + boot;
    Map<String, Integer> cases = new LinkedHashMap<>();
    cases.put(get("/v1/clock-bound", "X-Signaling-Pod-Uid: not-a-uuid", boots), 400);
    cases.put(get("/v1/clock-bound", "X-Signaling-Pod-Uid: 1-1-1-1-1", boots), 400);
    cases.put(get("/v1/clock-bound", pods, pods, boots), 400);
    cases.put(
        get("/v1/clock-bound", pods, "X-Signaling-Pod-Uid: " + UUID.randomUUID(), boots), 400);
    cases.put(get("/v1/clock-bound", pods), 400);
    cases.put(get("/v1/clock-bound", boots), 400);
    cases.put(get("/v1/clock-bound", pods, boots, boots), 400);
    cases.put(get("/v1/revocations", pods, boots), 400);
    cases.put(get("/v1/revocations", pods, boots, offset("-1")), 400);
    cases.put(get("/v1/revocations", pods, boots, offset("99999999999999999999")), 400);
    cases.put(get("/v1/revocations", pods, boots, offset("9223372036854775808")), 400);
    cases.put(get("/v1/revocations", pods, boots, offset("01")), 400);
    cases.put(get("/v1/revocations", pods, boots, offset("+1")), 400);
    cases.put(get("/v1/revocations", pods, boots, offset("0"), offset("0")), 400);
    cases.put(get("/v1/revocations?from=0", pods, boots, offset("0")), 400);
    cases.put(
        "GET /v1/revocations HTTP/1.1\r\nHost: 127.0.0.1\r\n"
            + pods
            + "\r\n"
            + boots
            + "\r\n"
            + offset("0")
            + "\r\nContent-Length: 2\r\n\r\n{}",
        400);
    cases.put(
        "POST /v1/clock-bound HTTP/1.1\r\nHost: 127.0.0.1\r\n"
            + pods
            + "\r\n"
            + boots
            + "\r\nContent-Length: 0\r\n\r\n",
        405);
    cases.put(get("/v1/unknown", pods, boots), 404);
    cases.put(get("/v1/clock-bound/", pods, boots), 404);
    cases.put(get("/", pods, boots), 404);
    cases.put(get("/v1/revocations", pods, boots, offset("5")), 409);

    for (Map.Entry<String, Integer> request : cases.entrySet()) {
      Reply reply = source(actorA, request.getKey());
      assertThat(reply.status()).as(request.getKey()).isEqualTo(request.getValue());
      assertThat(reply.body()).as(request.getKey()).isEmpty();
      assertThat(reply.header("content-length")).as(request.getKey()).isEqualTo("0");
    }
    // Nothing malformed registered the pod to another boot: the original pair still works.
    assertThat(source(actorA, get("/v1/clock-bound", pods, boots)).status()).isEqualTo(200);
  }

  @Test
  void responsesAreFramedWithContentLengthAndClose() throws Exception {
    List<Reply> replies = new ArrayList<>();
    replies.add(source(actorA, clockRequest()));
    replies.add(source(actorA, get("/v1/revocations", ids(UUID.randomUUID()), offset("0"))));
    replies.add(source(actorA, get("/v1/unknown", ids(UUID.randomUUID()))));
    replies.add(adminCall(admin, adminRequest("{\"userId\":\"alice\"}")));
    replies.add(adminCall(admin, adminRequest("{\"userId\":\"alice\",\"epoch\":0}")));
    replies.add(plain(running.healthPort(), "GET /ready HTTP/1.1\r\nHost: 127.0.0.1\r\n\r\n"));

    for (Reply reply : replies) {
      // tls()/plain() read until the server closed the connection, so the body is exactly what
      // was sent after the head.
      assertThat(reply.header("content-length")).isEqualTo(String.valueOf(reply.body().length));
      assertThat(reply.header("connection")).isEqualToIgnoringCase("close");
      assertThat(reply.headers()).doesNotContainKeys("transfer-encoding", "content-encoding");
      assertThat(reply.headers()).doesNotContainKey("location");
      assertThat(reply.body().length).isLessThanOrEqualTo(32768);
      if (reply.body().length > 0) {
        assertThat(reply.header("content-type")).isEqualTo("application/json");
      }
    }
    assertThat(replies.get(0).status()).isEqualTo(200);
    assertThat(replies.get(0).body()).isNotEmpty();
    assertThat(replies.get(2).status()).isEqualTo(404);
    assertThat(replies.get(3).status()).isEqualTo(201);
    assertThat(replies.get(4).status()).isEqualTo(409);
  }

  // --- readiness and lifecycle ----------------------------------------------------------------

  @Test
  void readyIs503UntilObserverWarmsThen200() throws Exception {
    String ready = "GET /ready HTTP/1.1\r\nHost: 127.0.0.1\r\n\r\n";

    // The observer needs a two-second window; right after start it is not continuous.
    Reply cold = plain(running.healthPort(), ready);
    assertThat(cold.status()).isEqualTo(503);
    assertThat(cold.body()).isEmpty();

    long deadline = System.nanoTime() + Duration.ofSeconds(15).toNanos();
    int status = cold.status();
    while (status != 200 && System.nanoTime() < deadline) {
      Thread.sleep(200);
      status = plain(running.healthPort(), ready).status();
    }
    assertThat(status).isEqualTo(200);

    assertThat(plain(running.healthPort(), "GET /other HTTP/1.1\r\nHost: x\r\n\r\n").status())
        .isEqualTo(404);
    assertThat(
            plain(
                    running.healthPort(),
                    "POST /ready HTTP/1.1\r\nHost: x\r\nContent-Length: 0\r\n\r\n")
                .status())
        .isEqualTo(405);
  }

  @Test
  void unreachableDatabaseYields503WithEmptyBody() throws Exception {
    String ready = "GET /ready HTTP/1.1\r\nHost: 127.0.0.1\r\n\r\n";
    awaitReady(ready);

    dropDatabase(database);

    Reply page = source(actorA, get("/v1/revocations", ids(UUID.randomUUID()), offset("0")));
    Reply append = adminCall(admin, adminRequest("{\"userId\":\"alice\"}"));
    Reply notReady = plain(running.healthPort(), ready);
    assertThat(page.status()).isEqualTo(503);
    assertThat(page.body()).isEmpty();
    assertThat(append.status()).isEqualTo(503);
    assertThat(append.body()).isEmpty();
    assertThat(notReady.status()).isEqualTo(503);
    // The clock report does not depend on the database.
    assertThat(source(actorA, clockRequest()).status()).isEqualTo(200);
  }

  @Test
  void successfulStartPrintsNothingToStdoutOrStderr() throws Exception {
    LocalPlatformConfig fresh = config(createDatabase());
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    ByteArrayOutputStream err = new ByteArrayOutputStream();

    PrintStream originalOut = System.out;
    PrintStream originalErr = System.err;
    System.setOut(new PrintStream(out, true, StandardCharsets.UTF_8));
    System.setErr(new PrintStream(err, true, StandardCharsets.UTF_8));
    try {
      LocalPlatformApplication.start(fresh).close();
    } finally {
      System.setOut(originalOut);
      System.setErr(originalErr);
    }

    assertNoMigrationOutput(out.toString(StandardCharsets.UTF_8));
    assertNoMigrationOutput(err.toString(StandardCharsets.UTF_8));
  }

  @Test
  void failedMigrationPrintsNothingToStdoutOrStderr() throws Exception {
    int closedPort;
    try (java.net.ServerSocket probe = new java.net.ServerSocket(0, 1, LOOPBACK)) {
      closedPort = probe.getLocalPort();
    }
    LocalPlatformConfig unreachable =
        LocalPlatformConfig.load(
                env(
                    writeConfigWithUrl(
                        "jdbc:postgresql://127.0.0.1:" + closedPort + "/" + createDatabase())))
            .withPorts(0, 0, 0);
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    ByteArrayOutputStream err = new ByteArrayOutputStream();

    PrintStream originalOut = System.out;
    PrintStream originalErr = System.err;
    System.setOut(new PrintStream(out, true, StandardCharsets.UTF_8));
    System.setErr(new PrintStream(err, true, StandardCharsets.UTF_8));
    try {
      assertThatThrownBy(() -> LocalPlatformApplication.start(unreachable))
          .isInstanceOf(LocalPlatformFailure.class)
          .extracting(failure -> ((LocalPlatformFailure) failure).category())
          .isEqualTo(LocalPlatformFailure.Category.DATABASE);
    } finally {
      System.setOut(originalOut);
      System.setErr(originalErr);
    }

    assertNoMigrationOutput(out.toString(StandardCharsets.UTF_8));
    assertNoMigrationOutput(err.toString(StandardCharsets.UTF_8));
  }

  private static void assertNoMigrationOutput(String printed) {
    assertThat(printed).isEmpty();
    assertThat(printed.toLowerCase(Locale.ROOT))
        .doesNotContain("jdbc:postgresql")
        .doesNotContain("migrating")
        .doesNotContain("flyway");
  }

  @Test
  void runningCloseIsIdempotentAndStopsEveryListener() {
    int[] ports = {running.sourcePort(), running.adminPort(), running.healthPort()};

    running.close();
    running.close();

    for (int port : ports) {
      assertThatThrownBy(() -> new Socket(LOOPBACK, port).close())
          .isInstanceOf(ConnectException.class);
    }
    running = null;
  }

  @Test
  void startupRefusesWithoutAcknowledgement() throws Exception {
    Map<String, String> env = env(writeConfig(createDatabase()));
    env.remove("SIGNALING_LOCAL_ACKNOWLEDGEMENT");
    ByteArrayOutputStream err = new ByteArrayOutputStream();
    AtomicBoolean started = new AtomicBoolean();

    int code =
        LocalPlatformApplication.run(
            env,
            new PrintStream(err, true, StandardCharsets.UTF_8),
            config -> {
              started.set(true);
              return LocalPlatformApplication.start(config);
            },
            platform -> {});

    assertThat(code).isEqualTo(1);
    assertThat(started).isFalse();
    assertThat(err.toString(StandardCharsets.UTF_8))
        .isEqualTo("LOCAL_PLATFORM_REJECTED ACKNOWLEDGEMENT" + System.lineSeparator());
  }

  // --- admin ----------------------------------------------------------------------------------

  @Test
  void adminRefusesWrongContentTypeAndNonStrictBodies() throws Exception {
    Map<String, Integer> cases = new LinkedHashMap<>();
    cases.put(adminRequest("text/plain", "{\"userId\":\"alice\"}"), 415);
    cases.put(adminRequest("application/json-patch+json", "{\"userId\":\"alice\"}"), 415);
    cases.put(adminRequest(null, "{\"userId\":\"alice\"}"), 415);
    cases.put(adminRequest("{\"userId\":\"alice\",\"extra\":1}"), 400);
    cases.put(adminRequest("{\"userId\":\"alice\",\"userId\":\"bob\"}"), 400);
    cases.put(adminRequest("{\"userId\":\"alice\"} {}"), 400);
    cases.put(adminRequest("{\"userId\":\"alice\"} x"), 400);
    cases.put(adminRequest("[\"alice\"]"), 400);
    cases.put(adminRequest(""), 400);
    cases.put(adminRequest("{}"), 400);
    cases.put(adminRequest("{\"userId\":1}"), 400);
    cases.put(adminRequest("{\"userId\":\"\"}"), 400);
    cases.put(adminRequest("{\"userId\":\" alice\"}"), 400);
    cases.put(adminRequest("{\"userId\":\"alice\",\"jti\":null}"), 400);
    cases.put(adminRequest("{\"userId\":\"alice\",\"jti\":\"\"}"), 400);
    cases.put(adminRequest("{\"userId\":\"alice\",\"epoch\":1.5}"), 400);
    cases.put(adminRequest("{\"userId\":\"alice\",\"epoch\":\"1\"}"), 400);
    cases.put(adminRequest("{\"userId\":\"alice\",\"epoch\":-1}"), 400);
    cases.put(adminRequest("{\"userId\":\"alice\",\"epoch\":9223372036854775808}"), 400);
    cases.put(adminRequest("{\"userId\":\"" + "a".repeat(5000) + "\"}"), 413);
    cases.put("GET /v1/admin/revocations HTTP/1.1\r\nHost: 127.0.0.1\r\n\r\n", 405);
    cases.put(
        "POST /v1/admin/other HTTP/1.1\r\nHost: 127.0.0.1\r\nContent-Type: application/json\r\n"
            + "Content-Length: 2\r\n\r\n{}",
        404);
    cases.put(
        "POST /v1/admin/revocations HTTP/1.1\r\nHost: 127.0.0.1\r\nContent-Type: application/json\r\n"
            + adminRequest("application/json", "{\"userId\":\"alice\"}")
                .substring("POST /v1/admin/revocations HTTP/1.1\r\nHost: 127.0.0.1\r\n".length()),
        400);

    for (Map.Entry<String, Integer> request : cases.entrySet()) {
      Reply reply = adminCall(admin, request.getKey());
      String label = request.getKey().substring(0, Math.min(160, request.getKey().length()));
      assertThat(reply.status()).as(label).isEqualTo(request.getValue());
      assertThat(reply.body()).as(label).isEmpty();
    }
    assertThat(highWater()).isZero();

    // Invalid UTF-8 is refused, not replaced.
    byte[] head =
        ("POST /v1/admin/revocations HTTP/1.1\r\nHost: 127.0.0.1\r\n"
                + "Content-Type: application/json\r\nContent-Length: 14\r\n\r\n")
            .getBytes(StandardCharsets.US_ASCII);
    byte[] body = "{\"userId\":\"a\"}".getBytes(StandardCharsets.US_ASCII);
    body[11] = (byte) 0xC3; // a lone UTF-8 lead byte in place of 'a'
    byte[] raw = Arrays.copyOf(head, head.length + body.length);
    System.arraycopy(body, 0, raw, head.length, body.length);
    assertThat(adminCall(admin, raw).status()).isEqualTo(400);

    // NFC-equivalent input is stored as the NFC form, with charset parameter accepted.
    Reply decomposed =
        adminCall(admin, adminRequest("application/json; charset=utf-8", "{\"userId\":\"Alé\"}"));
    assertThat(decomposed.status()).isEqualTo(201);
  }

  // --- handler-level guards (fake services behind a real TLS listener) -------------------------

  @Test
  void oversizedBodyIsNeverSent() throws Exception {
    String oversized = "\"" + "x".repeat(40_000) + "\"";
    String exact = "\"" + "y".repeat(32_766) + "\"";
    SourceHandler handler =
        new SourceHandler(
            Set.of("cell-a"), new BootRegistry(), registration -> oversized, (cell, from) -> exact);

    try (Served served = serve(handler)) {
      Reply tooBig = tls(served.port(), TestPki.context(actorA, serverCa), clockRequest());
      Reply fits =
          tls(
              served.port(),
              TestPki.context(actorA, serverCa),
              get("/v1/revocations", ids(UUID.randomUUID()), offset("0")));

      assertThat(tooBig.status()).isEqualTo(503);
      assertThat(tooBig.body()).isEmpty();
      assertThat(tooBig.header("content-length")).isEqualTo("0");
      assertThat(tooBig.rawLength()).isLessThan(1024);
      assertThat(fits.status()).isEqualTo(200);
      assertThat(fits.body()).hasSize(32_768);
    }
  }

  @Test
  void unexpectedFailuresYield503WithEmptyBody() throws Exception {
    RevocationLog broken =
        new RevocationLog() {
          @Override
          public Appended append(String userId, String jtiOrNull, java.util.OptionalLong epoch) {
            throw new IllegalStateException("TEST_ONLY secret detail jdbc:postgresql://x");
          }

          @Override
          public Snapshot read(long from, int limit) {
            throw new IllegalStateException("TEST_ONLY secret detail jdbc:postgresql://x");
          }
        };
    RevocationPageService pages =
        new RevocationPageService(
            SourceSigner.of(signingKeys, KEY_ID), broken, ISSUER, java.time.Clock.systemUTC());
    SourceHandler source =
        new SourceHandler(
            Set.of("cell-a"),
            new BootRegistry(),
            registration -> {
              throw new IllegalStateException("too many pods");
            },
            pages::pageBody);

    try (Served served = serve(source)) {
      SSLContext context = TestPki.context(actorA, serverCa);
      Reply clock = tls(served.port(), context, clockRequest());
      Reply page =
          tls(served.port(), context, get("/v1/revocations", ids(UUID.randomUUID()), offset("0")));
      assertThat(clock.status()).isEqualTo(503);
      assertThat(clock.body()).isEmpty();
      assertThat(page.status()).isEqualTo(503);
      assertThat(page.body()).isEmpty();
    }
    try (Served served = serveAdmin(new AdminHandler(broken))) {
      Reply reply =
          tls(
              served.port(),
              TestPki.context(admin, serverCa),
              adminRequest("{\"userId\":\"alice\"}"));
      assertThat(reply.status()).isEqualTo(503);
      assertThat(reply.body()).isEmpty();
    }
  }

  @Test
  void serviceBugsAreServerFaultsButTheLogsOwnInputValidationIsStill400() throws Exception {
    SourceHandler source =
        new SourceHandler(
            Set.of("cell-a"),
            new BootRegistry(),
            registration -> "{}",
            (cell, from) -> {
              throw new IllegalArgumentException("TEST_ONLY bug in the page service");
            });
    try (Served served = serve(source)) {
      Reply page =
          tls(
              served.port(),
              TestPki.context(actorA, serverCa),
              get("/v1/revocations", ids(UUID.randomUUID()), offset("0")));
      assertThat(page.status()).isEqualTo(503);
      assertThat(page.body()).isEmpty();
    }

    RevocationLog nullPointer =
        new RevocationLog() {
          @Override
          public Appended append(String userId, String jtiOrNull, java.util.OptionalLong epoch) {
            throw new NullPointerException("TEST_ONLY bug in the log");
          }

          @Override
          public Snapshot read(long from, int limit) {
            throw new IllegalStateException();
          }
        };
    RevocationLog invalidInput =
        new RevocationLog() {
          @Override
          public Appended append(String userId, String jtiOrNull, java.util.OptionalLong epoch) {
            throw new IllegalArgumentException();
          }

          @Override
          public Snapshot read(long from, int limit) {
            throw new IllegalStateException();
          }
        };
    try (Served served = serveAdmin(new AdminHandler(nullPointer))) {
      Reply reply =
          tls(
              served.port(),
              TestPki.context(admin, serverCa),
              adminRequest("{\"userId\":\"alice\"}"));
      assertThat(reply.status()).isEqualTo(503);
      assertThat(reply.body()).isEmpty();
    }
    try (Served served = serveAdmin(new AdminHandler(invalidInput))) {
      Reply reply =
          tls(
              served.port(),
              TestPki.context(admin, serverCa),
              adminRequest("{\"userId\":\"alice\"}"));
      assertThat(reply.status()).isEqualTo(400);
      assertThat(reply.body()).isEmpty();
    }
  }

  // --- helpers --------------------------------------------------------------------------------

  private record Reply(int status, Map<String, List<String>> headers, byte[] body, int rawLength) {
    String header(String name) {
      List<String> values = headers.get(name.toLowerCase(Locale.ROOT));
      if (values == null) {
        return null;
      }
      assertThat(values).as("single header " + name).hasSize(1);
      return values.get(0);
    }
  }

  private record Served(HttpsServer server, ExecutorService executor) implements AutoCloseable {
    int port() {
      return server.getAddress().getPort();
    }

    @Override
    public void close() {
      server.stop(0);
      executor.shutdownNow();
    }
  }

  private static Served serve(HttpHandler handler) throws IOException {
    return serve(
        handler,
        new LocalPlatformConfig.Listener(
            LOOPBACK, 0, sourceKeystore, keystorePassword, workloadTrust));
  }

  private static Served serveAdmin(HttpHandler handler) throws IOException {
    return serve(
        handler,
        new LocalPlatformConfig.Listener(LOOPBACK, 0, adminKeystore, keystorePassword, adminTrust));
  }

  private static Served serve(HttpHandler handler, LocalPlatformConfig.Listener listener)
      throws IOException {
    HttpsServer server = HttpsServer.create(new InetSocketAddress(LOOPBACK, 0), 8);
    server.setHttpsConfigurator(TlsContexts.configurator(TlsContexts.server(listener)));
    server.createContext("/", handler);
    ExecutorService executor = Executors.newFixedThreadPool(2);
    server.setExecutor(executor);
    server.start();
    return new Served(server, executor);
  }

  private Reply source(TestPki.Leaf identity, String request) throws IOException {
    return tls(running.sourcePort(), TestPki.context(identity, serverCa), request);
  }

  private Reply adminCall(TestPki.Leaf identity, String request) throws IOException {
    return adminCall(identity, request.getBytes(StandardCharsets.UTF_8));
  }

  private Reply adminCall(TestPki.Leaf identity, byte[] request) throws IOException {
    SSLContext context = TestPki.context(identity, serverCa);
    try (SSLSocket socket =
        (SSLSocket) context.getSocketFactory().createSocket(LOOPBACK, running.adminPort())) {
      socket.setEnabledProtocols(new String[] {"TLSv1.3"});
      socket.setSoTimeout(10_000);
      socket.startHandshake();
      return exchange(socket, request);
    }
  }

  private static Reply tls(int port, SSLContext context, String request) throws IOException {
    try (SSLSocket socket = (SSLSocket) context.getSocketFactory().createSocket(LOOPBACK, port)) {
      socket.setEnabledProtocols(new String[] {"TLSv1.3"});
      socket.setSoTimeout(10_000);
      socket.startHandshake();
      return exchange(socket, request.getBytes(StandardCharsets.UTF_8));
    }
  }

  private static Reply plain(int port, String request) throws IOException {
    try (Socket socket = new Socket(LOOPBACK, port)) {
      socket.setSoTimeout(10_000);
      return exchange(socket, request.getBytes(StandardCharsets.US_ASCII));
    }
  }

  /** Sends the request and reads until the server closes the connection. */
  private static Reply exchange(Socket socket, byte[] request) throws IOException {
    socket.getOutputStream().write(request);
    socket.getOutputStream().flush();
    byte[] all = socket.getInputStream().readAllBytes();
    if (all.length == 0) {
      throw new EOFException("connection closed without a response");
    }
    String text = new String(all, StandardCharsets.ISO_8859_1);
    int split = text.indexOf("\r\n\r\n");
    if (split < 0) {
      throw new EOFException("incomplete response head");
    }
    String[] lines = text.substring(0, split).split("\r\n");
    int status = Integer.parseInt(lines[0].split(" ")[1]);
    Map<String, List<String>> headers = new HashMap<>();
    for (int i = 1; i < lines.length; i++) {
      int colon = lines[i].indexOf(':');
      headers
          .computeIfAbsent(
              lines[i].substring(0, colon).toLowerCase(Locale.ROOT), k -> new ArrayList<>())
          .add(lines[i].substring(colon + 1).trim());
    }
    return new Reply(status, headers, Arrays.copyOfRange(all, split + 4, all.length), all.length);
  }

  private static String get(String path, String... headerLines) {
    StringBuilder request =
        new StringBuilder("GET ").append(path).append(" HTTP/1.1\r\nHost: 127.0.0.1\r\n");
    for (String line : headerLines) {
      request.append(line).append("\r\n");
    }
    return request.append("\r\n").toString();
  }

  private static String ids(UUID pod) {
    return ids(pod, UUID.randomUUID());
  }

  private static String ids(UUID pod, UUID boot) {
    return "X-Signaling-Pod-Uid: " + pod + "\r\nX-Signaling-Process-Boot: " + boot;
  }

  private static String offset(String value) {
    return "X-Signaling-Source-Offset: " + value;
  }

  private static String clockRequest() {
    return get("/v1/clock-bound", ids(UUID.randomUUID()));
  }

  private static String adminRequest(String body) {
    return adminRequest("application/json", body);
  }

  private static String adminRequest(String contentType, String body) {
    byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
    return "POST /v1/admin/revocations HTTP/1.1\r\nHost: 127.0.0.1\r\n"
        + (contentType == null ? "" : "Content-Type: " + contentType + "\r\n")
        + "Content-Length: "
        + bytes.length
        + "\r\n\r\n"
        + body;
  }

  private HttpResponse<String> adminPost(HttpClient client, String body) throws Exception {
    return client.send(
        HttpRequest.newBuilder(
                URI.create("https://127.0.0.1:" + running.adminPort() + "/v1/admin/revocations"))
            .header("Content-Type", "application/json")
            .POST(HttpRequest.BodyPublishers.ofString(body))
            .build(),
        HttpResponse.BodyHandlers.ofString());
  }

  private URI sourceUri(String path) {
    return URI.create("https://127.0.0.1:" + running.sourcePort() + path);
  }

  private static HttpClient client(SSLContext context) {
    return HttpClient.newBuilder()
        .sslContext(context)
        .version(HttpClient.Version.HTTP_1_1)
        .connectTimeout(Duration.ofSeconds(10))
        .build();
  }

  private static RevocationSourceVerifier verifier(String cell) {
    return new RevocationSourceVerifier(cell, ISSUER, Map.of(KEY_ID, signingKeys.getPublic()));
  }

  private void awaitReady(String request) throws Exception {
    long deadline = System.nanoTime() + Duration.ofSeconds(15).toNanos();
    while (plain(running.healthPort(), request).status() != 200) {
      assertThat(System.nanoTime()).isLessThan(deadline);
      Thread.sleep(200);
    }
  }

  private long highWater() throws Exception {
    Reply reply = source(actorA, get("/v1/revocations", ids(UUID.randomUUID()), offset("0")));
    return CONSUMER
        .readValue(reply.body(), RevocationReconciler.Batch.class)
        .currentSourceHighWater();
  }

  private LocalPlatformConfig config(String name) throws IOException {
    return LocalPlatformConfig.load(env(writeConfig(name))).withPorts(0, 0, 0);
  }

  private Path writeConfig(String name) throws IOException {
    return writeConfigWithUrl(
        "jdbc:postgresql://"
            + POSTGRES.getHost()
            + ":"
            + POSTGRES.getFirstMappedPort()
            + "/"
            + name);
  }

  private Path writeConfigWithUrl(String jdbcUrl) throws IOException {
    Path dbPassword = Files.writeString(dir.resolve("db.pass"), POSTGRES.getPassword() + "\n");
    ObjectNode root = JSON.createObjectNode();
    root.putObject("cells").put("cell-a", 3).put("cell-b", 7);
    var homes = root.putObject("homeBuckets");
    homes.putArray("cell-a").add(0).add(8191);
    homes.putArray("cell-b").add(8192).add(16383);
    root.put("keyId", KEY_ID);
    root.put("signingKey", signingKeyFile.toString());
    listener(root.putObject("source"), sourceKeystore, workloadTrust);
    listener(root.putObject("admin"), adminKeystore, adminTrust);
    root.putObject("health").put("bind", "127.0.0.1").put("port", 1);
    root.putObject("database")
        .put("jdbcUrl", jdbcUrl)
        .put("user", POSTGRES.getUsername())
        .put("passwordFile", dbPassword.toString());
    return Files.writeString(dir.resolve("platform.json"), JSON.writeValueAsString(root));
  }

  private static void listener(ObjectNode node, Path keystore, Path trust) {
    node.put("bind", "127.0.0.1")
        .put("port", 1)
        .put("keystore", keystore.toString())
        .put("keystorePasswordFile", keystorePassword.toString())
        .put("clientTrust", trust.toString());
  }

  private static Map<String, String> env(Path config) {
    Map<String, String> env = new HashMap<>();
    env.put("SIGNALING_PLATFORM_CONFIG", config.toString());
    env.put("SIGNALING_LOCAL_ACKNOWLEDGEMENT", "LOCAL_TEST_ONLY");
    env.put("SIGNALING_IDENTITY_ISSUER", ISSUER);
    return env;
  }

  private static String createDatabase() throws SQLException {
    String name = "platform_" + DATABASES.incrementAndGet();
    try (Connection connection = adminSource().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE DATABASE " + name);
    }
    return name;
  }

  private static void dropDatabase(String name) throws SQLException {
    try (Connection connection = adminSource().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("DROP DATABASE " + name + " WITH (FORCE)");
    }
  }

  private static PGSimpleDataSource adminSource() {
    PGSimpleDataSource source = new PGSimpleDataSource();
    source.setServerNames(new String[] {POSTGRES.getHost()});
    source.setPortNumbers(new int[] {POSTGRES.getFirstMappedPort()});
    source.setDatabaseName(POSTGRES.getDatabaseName());
    source.setUser(POSTGRES.getUsername());
    source.setPassword(POSTGRES.getPassword());
    return source;
  }
}
