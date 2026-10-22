package io.webrtc.signaling.storage;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import io.webrtc.signaling.auth.ClockSafetyMonitor;
import io.webrtc.signaling.localplatform.LocalPlatformApplication;
import io.webrtc.signaling.localplatform.LocalPlatformConfig;
import io.webrtc.signaling.protocol.Identity.UserId;
import io.webrtc.signaling.protocol.internal.CompatibilityObservationReply;
import io.webrtc.signaling.rpc.CellRpcClient;
import io.webrtc.signaling.rpc.RpcAdmission;
import io.webrtc.signaling.rpc.RpcTlsContexts;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.WebSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.Signature;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.IntPredicate;
import java.util.stream.Collectors;
import javax.net.ssl.SSLContext;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.io.TempDir;

/**
 * The SP1 one-cell composition (six actor JVMs in two zones, one gateway, one control, PostgreSQL
 * 17.6 with a synchronous streaming standby, Pekko over mTLS) served by the real LOCAL_TEST_ONLY
 * platform instead of the fixture's hand-written source. The platform runs in this JVM through
 * {@link LocalPlatformApplication#start(LocalPlatformConfig, java.util.function.LongSupplier,
 * java.util.function.Supplier)}: the source port, the harness source key (same key id and public
 * key the contracts already publish), the workload CA the planes trust and a {@code source}
 * database with its own owner on the cell's PostgreSQL primary. The contracts publish the
 * platform's URLs exactly where the fixture's source was published ({@link
 * LocalCompositionFixture#useExternalSource}); the planes are unchanged separate JVMs.
 *
 * <p>Clock figures: the platform measures only the drift between this process's wall and monotonic
 * clocks. It cannot see an error shared by both clocks or any error against an external reference,
 * so nothing here is clock-quality evidence. The clocks handed to the platform are a TEST_ONLY
 * {@link SteppableClock} injected through the {@code start(config, mono, wall)} seam, which is a
 * test seam and not a production hook: production calls {@code start(config)} with {@code
 * System::nanoTime} and {@code Instant::now}.
 *
 * <p>Status: LOCAL_TEST_ONLY, NOT_QUALIFIED. Nothing here is deployment, capacity or HA evidence.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class LocalPlatformCompositionIT {
  static final Duration ACTOR_READY = Duration.ofSeconds(90);
  static final String PLATFORM_IP = LocalCompositionFixture.SOURCE_IP;
  static final String SOURCE_DATABASE = "source";
  static final String SOURCE_OWNER = "source_owner";
  static final String SOURCE_PASSWORD = "TEST_ONLY_LOCAL_PLATFORM_SOURCE_DB";
  static final String ADMIN_SPIFFE = "spiffe://signaling/local-test-only/admin/test-only-admin";

  /** How long the cold clock is held after every plane answers, to show the 503 is sustained. */
  static final Duration COLD_HOLD = Duration.ofSeconds(5);

  /** The measurement window must span this much monotonic time before it can be continuous. */
  static final Duration WARM_UP = Duration.ofSeconds(2);

  /** Revocation must reach the running planes within this bound (task requirement). */
  static final Duration REVOCATION_BOUND = Duration.ofSeconds(10);

  /** A stepped platform clock must make control unsafe within this bound (task requirement). */
  static final Duration STEP_BOUND = Duration.ofSeconds(5);

  /**
   * A jump stays in the platform's five-second window; allowing for the 100 ms sample spacing, it
   * cannot report continuous again sooner than this after the step.
   */
  static final Duration STEP_MINIMUM_UNSAFE = Duration.ofMillis(4500);

  static final Duration POLL = Duration.ofMillis(100);
  static final int MAX_TRANSIENT_INVITE_REPLIES = 5;

  @TempDir static Path temporary;

  LocalCompositionFixture fixture;
  LocalCompositionPki adminPki;
  LocalCompositionPki.Leaf adminClient;
  final SteppableClock clock = new SteppableClock();
  LocalPlatformApplication.Running platform;
  final List<LocalCompositionFixture.Started> cell = new ArrayList<>();
  final List<LocalCompositionFixture.Started> actorProcesses = new ArrayList<>();
  LocalCompositionFixture.Started control;
  LocalCompositionFixture.Started gateway;
  List<LocalCompositionFixture.Actor> actors;
  boolean composed;

  @BeforeAll
  void startPlatformAndOutsideServices() throws Exception {
    fixture = new LocalCompositionFixture(temporary, Path.of("target/local-platform-composition"));
    createSourceDatabase();
    platform = LocalPlatformApplication.start(platformConfig(), clock::mono, clock::wall);
    fixture.useExternalSource("https://" + PLATFORM_IP + ":" + platform.sourcePort());
  }

  @AfterAll
  void stopEverything() throws Exception {
    try {
      if (platform != null) platform.close();
    } finally {
      if (fixture != null) fixture.close();
    }
  }

  /**
   * The platform starts with its monotonic clock held, so its measurement window never spans the
   * two seconds a continuous report needs: it signs {@code continuous=false} reports and its own
   * {@code /ready} is 503. The whole cell is composed against it and every plane comes up but stays
   * unready. Releasing the monotonic clock is the only change; control then turns ready, never
   * before the platform's own {@code /ready} does, and the platform only after a full warm-up.
   */
  @Test
  @Order(1)
  void controlBecomesReadyOnlyAfterMeasuredClockWarmsUp() throws Exception {
    assertThat(platformReady()).as("platform /ready with the clock held").isEqualTo(503);
    actors = fixture.actors(10, List.of("az-a", "az-a", "az-a", "az-b", "az-b", "az-b"));
    var entry = actors.getFirst().ip();
    fixture.services.pods(actors.stream().map(LocalCompositionFixture.Actor::ip).toList());

    var controlMount = fixture.mountControl(entry);
    assertThat(published(controlMount, "clockUrl"))
        .isEqualTo("https://" + PLATFORM_IP + ":" + platform.sourcePort() + "/v1/clock-bound");
    assertThat(published(controlMount, "revocationUrl"))
        .isEqualTo("https://" + PLATFORM_IP + ":" + platform.sourcePort() + "/v1/revocations");
    control =
        fixture.launch(
            "control",
            fixture.environment("control", controlMount, podUid(controlMount)),
            LocalCompositionFixture.CONTROL_IP,
            fixture.controlHealthPort);
    cell.add(control);
    // Staged like SP1: control's startup database budget is short, so it starts on a quiet host.
    await(control, Duration.ofSeconds(60), status -> status == 503);
    for (var actor : actors) {
      var mount = fixture.mountActor(actor, entry, List.of("az-a", "az-b"));
      var started =
          fixture.launch(
              actor.name(), fixture.actorEnvironment(actor, mount), actor.ip(), actor.healthPort());
      cell.add(started);
      actorProcesses.add(started);
    }
    for (var started : actorProcesses) await(started, ACTOR_READY, status -> status == 503);
    var gatewayMount = fixture.mountGateway(entry);
    gateway =
        fixture.launch(
            "gateway",
            fixture.environment("gateway", gatewayMount, podUid(gatewayMount)),
            LocalCompositionFixture.GATEWAY_IP,
            fixture.gatewayHealthPort);
    cell.add(gateway);
    // Every plane is up and answering its probe, unready.
    await(gateway, Duration.ofSeconds(60), status -> status == 503);

    // Sustained: while the platform's clock is cold, no plane turns ready.
    var cold = clockReport();
    assertThat(cold.continuous()).as("served report while cold").isFalse();
    assertThat(cold.pairUncertaintyMicros()).isGreaterThanOrEqualTo(1_000_000);
    long holdEnd = System.nanoTime() + COLD_HOLD.toNanos();
    while (System.nanoTime() < holdEnd) {
      assertThat(platformReady()).as("platform /ready while cold").isEqualTo(503);
      for (var started : cell) {
        assertThat(started.process().isAlive()).as(started.name()).isTrue();
        assertThat(ready(started)).as(started.name() + " /ready while cold").isEqualTo(503);
      }
      Thread.sleep(500);
    }

    long released = System.nanoTime();
    clock.release();
    long platformWarm = -1;
    long deadline = released + Duration.ofSeconds(120).toNanos();
    while (true) {
      if (!control.process().isAlive()) Assertions.fail(control.transcript());
      int controlStatus = ready(control);
      int platformStatus = platformReady(); // read after control: it cannot have gone cold since
      if (platformStatus == 200 && platformWarm < 0) platformWarm = System.nanoTime();
      if (controlStatus == 200) {
        assertThat(platformStatus).as("platform /ready once control is ready").isEqualTo(200);
        break;
      }
      assertThat(System.nanoTime()).as("control ready after release").isLessThan(deadline);
      Thread.sleep(POLL.toMillis());
    }
    assertThat(Duration.ofNanos(platformWarm - released))
        .as("platform warm-up after the clock was released")
        .isGreaterThanOrEqualTo(WARM_UP);
    assertThat(clockReport().continuous()).as("served report once warm").isTrue();

    for (var started : actorProcesses) await(started, ACTOR_READY, status -> status == 200);
    await(gateway, Duration.ofSeconds(60), status -> status == 200);
    await(control, Duration.ofSeconds(30), status -> status == 200);
    var members = observeMembers(entry);
    assertThat(members.getActorsList()).hasSize(6);
    assertThat(
            members.getActorsList().stream()
                .collect(Collectors.groupingBy(a -> a.getZone(), Collectors.counting())))
        .containsExactlyInAnyOrderEntriesOf(Map.of("az-a", 3L, "az-b", 3L));
    var bootstrap = bootstrap(userInCell("bootstrap-user"));
    assertThat(bootstrap.statusCode()).as(bootstrap.body()).isEqualTo(200);
    for (var started : cell) {
      assertThat(started.process().isAlive())
          .as(started.name() + "\n" + started.transcript())
          .isTrue();
      assertThat(started.transcript())
          .as(started.name())
          .doesNotContain(LocalRuntimeCompositionIT.STARTUP_FAILURES.toArray(String[]::new));
    }
    composed = true;
  }

  /** The SP1 INVITE flow, now with the platform as the only clock and revocation source. */
  @Test
  @Order(2)
  void authenticatedInviteStillCommits() throws Exception {
    requireComposedCell();
    var caller = userInCell("caller");
    var callee = userInCell("callee");
    try (var callerSocket = new LocalRuntimeCompositionIT.Wss(fixture, caller);
        var calleeSocket = new LocalRuntimeCompositionIT.Wss(fixture, callee)) {
      assertThat(callerSocket.next().path("type").asText()).isEqualTo("AUTH_OK");
      assertThat(calleeSocket.next().path("type").asText()).isEqualTo("AUTH_OK");
      // One intent: a retry reuses the requestId, so it is the same command, never a second call.
      var invite =
          "{\"v\":1,\"type\":\"INVITE\",\"requestId\":\""
              + UUID.randomUUID()
              + "\",\"payload\":{\"targetUserId\":\""
              + callee
              + "\"}}";
      JsonNode result = null;
      var transientReplies = new ArrayList<String>();
      for (int attempt = 0; attempt < 10; attempt++) {
        callerSocket.send(invite);
        result = callerSocket.next();
        var code = result.path("error").path("code").asText(result.path("type").asText());
        if (!List.of("OVERLOADED", "UNAVAILABLE", "OUTCOME_UNKNOWN").contains(code)) break;
        transientReplies.add(code);
        Thread.sleep(1000);
      }
      var retries = "transient replies before the result: " + transientReplies;
      assertThat(transientReplies)
          .as(retries)
          .hasSizeLessThanOrEqualTo(MAX_TRANSIENT_INVITE_REPLIES);
      assertThat(result.path("type").asText())
          .as(result + "; " + retries)
          .isEqualTo("ACK_COMMITTED");
      var callId = result.path("callId").asText();
      assertThat(callId).as(retries).startsWith(LocalCompositionFixture.CELL + ".e1.");
      try (var c = fixture.database.admin(LocalCompositionDatabase.C001);
          var q =
              c.prepareStatement(
                  "SELECT count(*),count(*) FILTER (WHERE caller_user=? AND call_id=?) FROM call_state")) {
        q.setString(1, caller);
        q.setString(2, callId);
        try (var r = q.executeQuery()) {
          r.next();
          assertThat(r.getInt(1)).as("all calls; " + retries).isEqualTo(1);
          assertThat(r.getInt(2)).as("this caller's call; " + retries).isEqualTo(1);
        }
      }
    }
  }

  /**
   * An admin revoke with the epoch omitted (default 0 for a user without events) reaches the
   * running planes: within ten seconds the user's next bootstrap is 401 and the user's next WSS
   * AUTH is closed with {@code AUTH_REVOKED}. Tokens here carry no {@code securityEpoch} claim, so
   * their epoch is 0. A user with no event at all in the log keeps bootstrapping and authenticating
   * (Review Focus 1, end to end).
   */
  @Test
  @Order(3)
  void adminRevocationDeniesTheUserOnTheRunningPlanes() throws Exception {
    requireComposedCell();
    var revoked = userInCell("revoked");
    var bystander = userInCell("bystander");
    assertThat(eventsFor(revoked)).isZero();
    assertThat(bootstrap(revoked).statusCode()).as("before the revoke").isEqualTo(200);
    assertThat(authenticate(revoked)).as("before the revoke").isEqualTo("AUTH_OK");

    var appended = adminRevoke("{\"userId\":\"" + revoked + "\"}");
    long revokedAt = System.nanoTime();
    assertThat(appended.statusCode()).as(appended.body()).isEqualTo(201);
    var body = LocalCompositionFixture.JSON.readTree(appended.body());
    assertThat(body.path("epoch").asLong(-1)).as("default epoch of a first event").isZero();
    assertThat(eventsFor(revoked)).isEqualTo(1);

    long deadline = revokedAt + REVOCATION_BOUND.toNanos();
    int status;
    while ((status = bootstrap(revoked).statusCode()) != 401) {
      assertThat(System.nanoTime()).as("bootstrap still " + status).isLessThan(deadline);
      Thread.sleep(200);
    }
    String outcome;
    while (!(outcome = authenticate(revoked)).equals("CLOSED 1008 AUTH_REVOKED")) {
      assertThat(System.nanoTime()).as("WSS AUTH still " + outcome).isLessThan(deadline);
      Thread.sleep(200);
    }
    assertThat(System.nanoTime()).as("both planes deny within the bound").isLessThan(deadline);

    // A user with no event in the log is not denied by the default epoch 0.
    assertThat(eventsFor(bystander)).isZero();
    var other = bootstrap(bystander);
    assertThat(other.statusCode()).as(other.body()).isEqualTo(200);
    assertThat(authenticate(bystander)).isEqualTo("AUTH_OK");
    for (var started : cell)
      assertThat(started.process().isAlive())
          .as(started.name() + "\n" + started.transcript())
          .isTrue();
  }

  /**
   * Steps the platform's wall clock back by 300 ms. The step lands in the platform's measurement
   * window as an offset jump far above its 50 ms limit, so its reports turn {@code
   * continuous=false} and control must turn unready within five seconds. Once the jump has left the
   * five-second window the reports are continuous again and control recovers.
   *
   * <p>The step is backwards on purpose: a forward step would also date every later report in the
   * planes' future, which their own monitor rejects for as long as the offset lasts. Control would
   * then turn unready for that reason too and could never recover, so the test would no longer
   * isolate the platform's measurement.
   */
  @Test
  @Order(4)
  void clockStepMakesThePlanesUnsafeAgain() throws Exception {
    requireComposedCell();
    assertThat(ready(control)).isEqualTo(200);
    assertThat(platformReady()).isEqualTo(200);

    long stepped = System.nanoTime();
    clock.step(Duration.ofMillis(-300));
    long deadline = stepped + STEP_BOUND.toNanos();
    while (ready(control) != 503) {
      assertThat(System.nanoTime()).as("control still ready after the step").isLessThan(deadline);
      Thread.sleep(POLL.toMillis());
    }
    assertThat(platformReady()).as("platform /ready while control is unsafe").isEqualTo(503);
    assertThat(clockReport().continuous()).as("served report after the step").isFalse();

    long platformRecovered = -1;
    long recoveryDeadline = stepped + Duration.ofSeconds(60).toNanos();
    while (true) {
      int controlStatus = ready(control);
      int platformStatus = platformReady();
      if (platformStatus == 200 && platformRecovered < 0) platformRecovered = System.nanoTime();
      if (controlStatus == 200) {
        assertThat(platformStatus).as("platform /ready once control recovered").isEqualTo(200);
        break;
      }
      assertThat(System.nanoTime()).as("control recovery").isLessThan(recoveryDeadline);
      Thread.sleep(POLL.toMillis());
    }
    assertThat(Duration.ofNanos(platformRecovered - stepped))
        .as("platform unsafe period after the step")
        .isGreaterThanOrEqualTo(STEP_MINIMUM_UNSAFE);
    for (var started : cell)
      assertThat(started.process().isAlive())
          .as(started.name() + "\n" + started.transcript())
          .isTrue();
  }

  // ---------------------------------------------------------------- platform wiring

  /**
   * TEST_ONLY clocks for the platform's {@code start(config, mono, wall)} seam. The monotonic clock
   * is held at its start value until {@link #release()}, then reads {@link System#nanoTime()}
   * (never decreasing). The wall clock is {@link Instant#now()} plus a step that {@link #step} can
   * change.
   */
  static final class SteppableClock {
    private final long heldAt = System.nanoTime();
    private volatile boolean held = true;
    private final AtomicLong wallStepNanos = new AtomicLong();

    long mono() {
      return held ? heldAt : System.nanoTime();
    }

    Instant wall() {
      return Instant.now().plusNanos(wallStepNanos.get());
    }

    void release() {
      held = false;
    }

    void step(Duration step) {
      wallStepNanos.addAndGet(step.toNanos());
    }
  }

  /** A {@code source} database owned by its own TEST_ONLY role on the cell's primary. */
  private void createSourceDatabase() throws Exception {
    try (var c = fixture.database.admin("postgres");
        var q = c.createStatement()) {
      q.execute("CREATE ROLE " + SOURCE_OWNER + " LOGIN PASSWORD '" + SOURCE_PASSWORD + "'");
      q.execute("CREATE DATABASE " + SOURCE_DATABASE + " OWNER " + SOURCE_OWNER);
    }
  }

  /**
   * The platform's mounted configuration: source key = the harness key under the harness key id;
   * source server leaf and client trust = the workload CA the planes use; a separate throwaway
   * admin CA for the admin listener and its client; the {@code source} database over verified TLS.
   * Ports are ephemeral through {@link LocalPlatformConfig#withPorts}.
   */
  private LocalPlatformConfig platformConfig() throws Exception {
    var dir = Files.createDirectories(temporary.resolve("platform"));
    var pki = fixture.pki;
    adminPki = LocalCompositionPki.create(dir.resolve("admin-pki"));
    var adminCa = adminPki.workload; // TEST_ONLY: this throwaway PKI's first CA is the admin CA
    adminClient = adminPki.leaf(adminCa, "test-only-admin", "URI:" + ADMIN_SPIFFE);
    var sourceLeaf =
        pki.leaf(
            pki.workload, "platform-source", "DNS:source.local-composition.test,IP:" + PLATFORM_IP);
    var adminLeaf = adminPki.leaf(adminCa, "platform-admin", "IP:" + PLATFORM_IP);
    var sourceStore = dir.resolve("source.p12");
    var adminStore = dir.resolve("admin.p12");
    pki.keyStore(sourceLeaf, pki.workload, sourceStore);
    adminPki.keyStore(adminLeaf, adminCa, adminStore);
    var storePassword =
        Files.writeString(dir.resolve("keystore.pass"), LocalCompositionPki.STORE_PASSWORD + "\n");
    var databasePassword = Files.writeString(dir.resolve("database.pass"), SOURCE_PASSWORD + "\n");
    var signingKey = dir.resolve("source-signing.key");
    LocalCompositionPki.writeKeyPair(fixture.sourceKeys, null, signingKey);

    var root = new LinkedHashMap<String, Object>();
    root.put("cells", Map.of(LocalCompositionFixture.CELL, 1));
    root.put("keyId", LocalCompositionServices.SOURCE_KEY_ID);
    root.put("signingKey", signingKey.toString());
    root.put("source", listener(sourceStore, storePassword, pki.workload.certificate()));
    root.put("admin", listener(adminStore, storePassword, adminCa.certificate()));
    root.put("health", Map.of("bind", PLATFORM_IP, "port", 1));
    root.put(
        "database",
        Map.of(
            "jdbcUrl",
            fixture.database.jdbcUrl(SOURCE_DATABASE, pki.workload.certificate()),
            "user",
            SOURCE_OWNER,
            "passwordFile",
            databasePassword.toString()));
    var config = dir.resolve("platform.json");
    Files.writeString(config, LocalCompositionFixture.JSON.writeValueAsString(root));
    return LocalPlatformConfig.load(
            Map.of(
                "SIGNALING_PLATFORM_CONFIG",
                config.toString(),
                "SIGNALING_LOCAL_ACKNOWLEDGEMENT",
                "LOCAL_TEST_ONLY",
                "SIGNALING_IDENTITY_ISSUER",
                LocalCompositionFixture.ISSUER))
        .withPorts(0, 0, 0);
  }

  private static Map<String, Object> listener(Path keystore, Path password, Path clientTrust) {
    var listener = new LinkedHashMap<String, Object>();
    listener.put("bind", PLATFORM_IP);
    listener.put("port", 1);
    listener.put("keystore", keystore.toString());
    listener.put("keystorePasswordFile", password.toString());
    listener.put("clientTrust", clientTrust.toString());
    return listener;
  }

  private int platformReady() {
    return LocalCompositionFixture.ready(PLATFORM_IP, platform.healthPort());
  }

  /** One signed report fetched as a workload identity, verified with the published key. */
  private ClockSafetyMonitor.Report clockReport() throws Exception {
    var probe =
        fixture.pki.leaf(
            fixture.pki.workload,
            "clock-probe-" + UUID.randomUUID(),
            "URI:"
                + LocalCompositionFixture.SPIFFE
                + LocalCompositionFixture.CELL
                + "/control/test-probe");
    try (var client = client(LocalCompositionPki.context(probe, fixture.pki.workload))) {
      var response =
          client.send(
              HttpRequest.newBuilder(
                      URI.create(
                          "https://"
                              + PLATFORM_IP
                              + ":"
                              + platform.sourcePort()
                              + "/v1/clock-bound"))
                  .timeout(Duration.ofSeconds(5))
                  .header("X-Signaling-Pod-Uid", UUID.randomUUID().toString())
                  .header("X-Signaling-Process-Boot", UUID.randomUUID().toString())
                  .GET()
                  .build(),
              HttpResponse.BodyHandlers.ofString());
      assertThat(response.statusCode()).as(response.body()).isEqualTo(200);
      var body = LocalCompositionServices.JSON.readTree(response.body());
      var report =
          LocalCompositionServices.JSON.treeToValue(
              body.get("report"), ClockSafetyMonitor.Report.class);
      var verifier = Signature.getInstance("Ed25519");
      verifier.initVerify(fixture.sourceKeys.getPublic());
      verifier.update(ClockSafetyMonitor.signingBytes(report));
      assertThat(verifier.verify(Base64.getUrlDecoder().decode(body.path("signature").asText())))
          .as("report verifies with the published source key")
          .isTrue();
      assertThat(report.keyId()).isEqualTo(LocalCompositionServices.SOURCE_KEY_ID);
      assertThat(report.cell()).isEqualTo(LocalCompositionFixture.CELL);
      return report;
    }
  }

  private HttpResponse<String> adminRevoke(String json) throws Exception {
    try (var client = client(LocalCompositionPki.context(adminClient, adminPki.workload))) {
      return client.send(
          HttpRequest.newBuilder(
                  URI.create(
                      "https://"
                          + PLATFORM_IP
                          + ":"
                          + platform.adminPort()
                          + "/v1/admin/revocations"))
              .timeout(Duration.ofSeconds(5))
              .header("Content-Type", "application/json")
              .POST(HttpRequest.BodyPublishers.ofString(json))
              .build(),
          HttpResponse.BodyHandlers.ofString());
    }
  }

  /** Events the platform's append-only log holds for {@code user}. */
  private int eventsFor(String user) throws Exception {
    try (var c = fixture.database.admin(SOURCE_DATABASE);
        var q = c.prepareStatement("SELECT count(*) FROM revocation_log WHERE user_id=?")) {
      q.setString(1, user);
      try (var r = q.executeQuery()) {
        r.next();
        return r.getInt(1);
      }
    }
  }

  private static HttpClient client(SSLContext context) {
    return HttpClient.newBuilder()
        .sslContext(context)
        .version(HttpClient.Version.HTTP_1_1)
        .connectTimeout(Duration.ofSeconds(3))
        .build();
  }

  private static String published(Path mount, String source) throws Exception {
    return LocalCompositionFixture.JSON
        .readTree(mount.resolve("provider.json").toFile())
        .path("sources")
        .path(source)
        .asText();
  }

  // ---------------------------------------------------------------- planes

  private void requireComposedCell() {
    if (!composed)
      Assertions.fail(
          "controlBecomesReadyOnlyAfterMeasuredClockWarmsUp did not compose the cell; this test depends on it");
  }

  private static int ready(LocalCompositionFixture.Started started) {
    return LocalCompositionFixture.ready(started.healthHost(), started.healthPort());
  }

  private static void await(
      LocalCompositionFixture.Started started, Duration limit, IntPredicate expected)
      throws Exception {
    long end = System.nanoTime() + limit.toNanos();
    int last = -1;
    while (System.nanoTime() < end) {
      if (!started.process().isAlive())
        throw new AssertionError(
            started.name()
                + " exited "
                + started.process().exitValue()
                + "\n"
                + started.transcript());
      last = ready(started);
      if (expected.test(last)) return;
      Thread.sleep(500);
    }
    throw new AssertionError(
        started.name() + " /ready was " + last + " after " + limit + "\n" + started.transcript());
  }

  private static String podUid(Path mount) throws Exception {
    return LocalCompositionFixture.JSON
        .readTree(mount.resolve("process.json").toFile())
        .path("podUid")
        .asText();
  }

  private static String userInCell(String prefix) {
    for (int i = 0; ; i++) {
      var user = prefix + "-" + i;
      if (SessionRegistryService.bucket(new UserId(user)) < LocalCompositionDatabase.SPLIT)
        return user;
    }
  }

  private HttpResponse<String> bootstrap(String user) throws Exception {
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
              .header("Authorization", "Bearer " + fixture.token(user, Instant.now()))
              .POST(HttpRequest.BodyPublishers.noBody())
              .build(),
          HttpResponse.BodyHandlers.ofString());
    }
  }

  /**
   * One WSS AUTH with a fresh token: {@code AUTH_OK}, or {@code CLOSED <code> <reason>} when the
   * gateway closes the socket instead.
   */
  private String authenticate(String user) throws Exception {
    BlockingQueue<String> outcomes = new LinkedBlockingQueue<>();
    try (var client =
        HttpClient.newBuilder()
            .sslContext(LocalCompositionPki.context(null, fixture.pki.workload))
            .connectTimeout(Duration.ofSeconds(3))
            .build()) {
      var socket =
          client
              .newWebSocketBuilder()
              .header("Origin", LocalCompositionFixture.ORIGIN)
              .subprotocols("webrtc-signaling.v1")
              .buildAsync(
                  URI.create(
                      "wss://"
                          + LocalCompositionFixture.GATEWAY_IP
                          + ":"
                          + fixture.gatewayWssPort
                          + "/ws"),
                  new WebSocket.Listener() {
                    final StringBuilder text = new StringBuilder();

                    @Override
                    public CompletionStage<?> onText(
                        WebSocket ws, CharSequence data, boolean last) {
                      text.append(data);
                      if (last) {
                        var type = type(text.toString());
                        if (!type.equals("PING")) outcomes.add(type);
                        text.setLength(0);
                      }
                      ws.request(1);
                      return null;
                    }

                    @Override
                    public CompletionStage<?> onClose(WebSocket ws, int code, String reason) {
                      outcomes.add("CLOSED " + code + " " + reason);
                      return null;
                    }

                    @Override
                    public void onError(WebSocket ws, Throwable error) {
                      outcomes.add("ERROR");
                    }
                  })
              .get(5, TimeUnit.SECONDS);
      try {
        socket
            .sendText(
                "{\"v\":1,\"type\":\"AUTH\",\"payload\":{\"token\":\""
                    + fixture.token(user, Instant.now())
                    + "\"}}",
                true)
            .get(3, TimeUnit.SECONDS);
        var outcome = outcomes.poll(10, TimeUnit.SECONDS);
        return outcome == null ? "NO_REPLY" : outcome;
      } finally {
        socket.abort();
      }
    }
  }

  private static String type(String frame) {
    try {
      return LocalCompositionFixture.JSON.readTree(frame).path("type").asText();
    } catch (Exception unreadable) {
      return "UNREADABLE";
    }
  }

  /** An external control-identity observer: the cell's own report of its Up members and zones. */
  private CompatibilityObservationReply observeMembers(String entryHost) throws Exception {
    var leaf =
        fixture.pki.leaf(
            fixture.pki.workload,
            "observer",
            "URI:"
                + LocalCompositionFixture.SPIFFE
                + LocalCompositionFixture.CELL
                + "/control/test-observer");
    var client =
        new CellRpcClient(
            LocalCompositionFixture.ENVIRONMENT,
            Map.of(
                LocalCompositionFixture.CELL,
                new CellRpcClient.Endpoint(
                    entryHost, fixture.entryPort, "actor.c001.local-composition.test")),
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
        var operation =
            client.observeCompatibility(LocalCompositionFixture.CELL, Duration.ofSeconds(2));
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
}
