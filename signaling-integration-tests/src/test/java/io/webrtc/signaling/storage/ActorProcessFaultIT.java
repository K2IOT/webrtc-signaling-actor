package io.webrtc.signaling.storage;

import static org.assertj.core.api.Assertions.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.typesafe.config.ConfigFactory;
import io.webrtc.signaling.actors.call.*;
import io.webrtc.signaling.actors.lease.PostgresShardLease;
import io.webrtc.signaling.protocol.Identity.*;
import java.io.*;
import java.nio.file.*;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import org.apache.pekko.actor.testkit.typed.javadsl.ActorTestKit;
import org.apache.pekko.actor.typed.javadsl.AskPattern;
import org.apache.pekko.coordination.lease.LeaseSettings;
import org.junit.jupiter.api.Test;

/** Real child actor JVM signals and native PostgreSQL TTL; no artificially advanced DB clock. */
class ActorProcessFaultIT {
  static final String OWNER = "TEST_ONLY_ACTOR_JVM";

  static final class ChildProcess implements AutoCloseable {
    final Process process;
    final Path directory;
    final Path log;
    boolean paused;

    ChildProcess(LocalInviteAtomicIT.Fixture f, CallId call) throws Exception {
      assertThat(System.getProperty("os.name")).startsWith("Linux");
      directory = Files.createTempDirectory(Path.of("target"), "fault-child-");
      log = directory.resolve("child.log");
      var config = new Properties();
      config.setProperty("jdbc", f.url);
      config.setProperty("username", f.username);
      config.setProperty("password", f.password);
      config.setProperty("call", call.value());
      Path input = directory.resolve("TEST_ONLY.properties");
      try (var writer = Files.newBufferedWriter(input)) {
        config.store(writer, "Task 22 disposable local fixture");
      }
      try {
        process =
            new ProcessBuilder(
                    Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                    "--add-exports=java.base/jdk.internal.misc=ALL-UNNAMED",
                    "-cp",
                    System.getProperty(
                        "surefire.test.class.path", System.getProperty("java.class.path")),
                    Child.class.getName(),
                    input.toAbsolutePath().toString())
                .redirectErrorStream(true)
                .redirectOutput(log.toFile())
                .start();
      } catch (Exception failure) {
        Files.deleteIfExists(input);
        throw failure;
      }
    }

    void marker(String value) {
      org.awaitility.Awaitility.await()
          .alias("child marker " + value + ": " + log)
          .atMost(Duration.ofSeconds(15))
          .until(
              () -> {
                if (Files.size(log) > 1024 * 1024)
                  throw new AssertionError("Child output exceeded bound");
                return Files.readString(log).contains("TASK22:" + value);
              });
    }

    void pause() throws Exception {
      signal("STOP");
      paused = true;
      org.awaitility.Awaitility.await()
          .atMost(Duration.ofSeconds(2))
          .until(
              () ->
                  Files.readString(Path.of("/proc/" + process.pid() + "/status"))
                      .contains("State:\tT"));
    }

    void resume() throws Exception {
      if (paused && process.isAlive()) {
        signal("CONT");
        paused = false;
      }
    }

    void signal(String signal) throws Exception {
      var sent = new ProcessBuilder("kill", "-" + signal, Long.toString(process.pid())).start();
      assertThat(sent.waitFor(2, TimeUnit.SECONDS)).isTrue();
      assertThat(sent.exitValue()).isZero();
    }

    @Override
    public void close() throws Exception {
      try {
        resume();
        if (process.isAlive()) {
          process.destroyForcibly();
          assertThat(process.waitFor(10, TimeUnit.SECONDS)).isTrue();
        }
      } finally {
        Files.deleteIfExists(directory.resolve("TEST_ONLY.properties"));
      }
    }
  }

  @Test
  void pausedActorJvmCannotMutateOrReleaseAfterFifteenSecondNativeTakeover() throws Exception {
    scenario(true);
  }

  @Test
  void killedActorJvmRecoversSilentCommittedCallWithOriginalDeadline() throws Exception {
    scenario(false);
  }

  static void scenario(boolean pause) throws Exception {
    try (var f = new LocalInviteAtomicIT.Fixture()) {
      var caller = f.sender("process-caller");
      var callee = f.sender("process-callee");
      var call =
          f.service()
              .executeCallCommand(f.invite(caller, callee.userId()))
              .toCompletableFuture()
              .join()
              .callId();
      var original = f.service().loadCallSnapshot(caller, call).toCompletableFuture().join();
      int group = HomeParticipationService.group(call);
      assertThat(CoordinatorGrantIT.done(f.groups.releaseTracked(f.token(call)))).isTrue();
      try (var child = new ChildProcess(f, call)) {
        child.marker("READY");
        UUID oldIncarnation;
        try (var c = f.connection();
            var q =
                c.prepareStatement("SELECT owner_incarnation FROM group_owner WHERE group_id=?")) {
          q.setInt(1, group);
          try (var r = q.executeQuery()) {
            assertThat(r.next()).isTrue();
            oldIncarnation = r.getObject(1, UUID.class);
          }
        }
        var old =
            f.groups
                .reconcile(group, OWNER, oldIncarnation)
                .toCompletableFuture()
                .join()
                .orElseThrow();
        long faultAt = System.nanoTime();
        if (pause) child.pause();
        else {
          child.process.destroyForcibly();
          assertThat(child.process.waitFor(5, TimeUnit.SECONDS)).isTrue();
        }
        org.awaitility.Awaitility.await()
            .atMost(Duration.ofSeconds(22))
            .pollInterval(Duration.ofMillis(200))
            .until(
                () -> {
                  try (var c = f.connection();
                      var q =
                          c.prepareStatement(
                              "SELECT lease_until<=clock_timestamp() FROM group_owner WHERE group_id=?")) {
                    q.setInt(1, group);
                    try (var r = q.executeQuery()) {
                      r.next();
                      return r.getBoolean(1);
                    }
                  }
                });
        long expiredAt = System.nanoTime();
        var replacement =
            CoordinatorGrantIT.done(
                    f.groups.acquireTracked(
                        group, "TEST_ONLY_PROCESS_RECOVERY", UUID.randomUUID(), UUID.randomUUID()))
                .orElseThrow();
        assertThat(replacement.token().epoch()).isGreaterThan(old.token().epoch());
        var workflow =
            new CallWorkflowService(
                f.runtime.sql, "c001", 1, "TEST_ONLY_PROCESS_RECOVERY", (t, s) -> false);
        if (pause) {
          child
              .process
              .getOutputStream()
              .write("MUTATE\n".getBytes(java.nio.charset.StandardCharsets.UTF_8));
          child.process.getOutputStream().flush();
          child.resume();
          child.marker("FENCED");
          assertThat(child.process.waitFor(15, TimeUnit.SECONDS))
              .as(Files.readString(child.log))
              .isTrue();
          assertThat(child.process.exitValue()).as(Files.readString(child.log)).isZero();
        } else {
          var actors = ActorTestKit.create();
          try {
            var commands = f.service();
            var backend =
                new CallCommandHandler(
                    workflow, commands, () -> Optional.of(replacement.token()), 1);
            var entities =
                new ConcurrentHashMap<
                    CallId,
                    org.apache.pekko.actor.typed.ActorRef<
                        io.webrtc.signaling.actors.cluster.CallMessage>>();
            try (var recovery =
                new CallRecoveryService(
                    f.runtime.sql,
                    "c001",
                    1,
                    (id, budget) -> {
                      var actor =
                          entities.computeIfAbsent(
                              id,
                              value ->
                                  actors.spawn(
                                      CallActor.create(
                                          value,
                                          backend,
                                          () -> Optional.of(replacement.token()),
                                          Clock.systemUTC())));
                      return AskPattern.ask(
                          actor,
                          reply -> new CallActor.WakeCall(id, reply, Instant.now().plus(budget)),
                          budget,
                          actors.scheduler());
                    })) {
              var result = recovery.sweep().toCompletableFuture().get(5, TimeUnit.SECONDS);
              assertThat(result.hydrated()).isEqualTo(1);
              assertThat(result.cycleComplete()).isTrue();
            }
          } finally {
            actors.shutdownTestKit();
          }
        }
        var current =
            CoordinatorGrantIT.done(workflow.load(call, replacement.token(), Duration.ofSeconds(2)))
                .orElseThrow();
        assertThat(current.state()).isEqualTo("RINGING");
        assertThat(current.version()).isEqualTo(original.version());
        assertThat(current.deadlines()).isEqualTo(original.deadlines());
        assertThat(CoordinatorGrantIT.done(f.groups.releaseTracked(old.token()))).isTrue();
        assertThat(
                f.groups
                    .reconcile(group, replacement.token().node(), replacement.token().incarnation())
                    .toCompletableFuture()
                    .join()
                    .map(GroupOwnerRepository.Grant::token))
            .contains(replacement.token());
        var observations =
            Map.of(
                "childPid",
                child.process.pid(),
                "faultAtNanos",
                faultAt,
                "expiredAtNanos",
                expiredAt,
                "oldEpoch",
                old.token().epoch(),
                "newEpoch",
                replacement.token().epoch(),
                "originalDeadlinePreserved",
                true,
                "staleReleaseNoOp",
                true);
        Path evidence =
            Path.of(
                "target/fault-receipts/"
                    + (pause ? "actor-jvm-pause" : "actor-jvm-kill")
                    + ".json");
        Files.createDirectories(evidence.getParent());
        Files.writeString(
            evidence,
            new ObjectMapper()
                    .writeValueAsString(
                        Map.of(
                            "testOnly",
                            true,
                            "scope",
                            "LOCAL_ACTOR_JVM",
                            "scenario",
                            pause ? "actor-jvm-pause" : "actor-jvm-kill",
                            "observations",
                            observations))
                + "\n");
      }
    }
  }

  /**
   * Child process uses production lease, actor and backend; only its authority proof is TEST ONLY.
   */
  public static final class Child {
    public static void main(String[] args) throws Exception {
      var config = new Properties();
      try (var reader = Files.newBufferedReader(Path.of(args[0]))) {
        config.load(reader);
      }
      var call = new CallId(config.getProperty("call"));
      var timers = Executors.newSingleThreadScheduledExecutor();
      try (var runtime =
          new DbTestRuntime(
              config.getProperty("jdbc"),
              config.getProperty("username"),
              config.getProperty("password"))) {
        var roots = new GroupOwnerRepository(runtime.sql, "c001", 1);
        int group = HomeParticipationService.group(call);
        var settings =
            LeaseSettings.apply(
                ConfigFactory.parseString(
                    "heartbeat-interval=5s\nheartbeat-timeout=15s\nlease-operation-timeout=2s"),
                "process-fault-shard-SignalingCallV1-" + group,
                "127.0.0.1:2552");
        var lease =
            new PostgresShardLease(
                settings,
                new PostgresShardLease.Namespace(
                    "process-fault", "c001", 1, "127.0.0.1:2552", OWNER),
                roots,
                timers,
                () -> true,
                System::nanoTime);
        assertThat(lease.acquire().toCompletableFuture().get(3, TimeUnit.SECONDS)).isTrue();
        var old = lease.currentGrant().orElseThrow().token();
        var workflow =
            new CallWorkflowService(
                runtime.sql, "c001", 1, OWNER, (t, s) -> t.proof().equals("TEST_ONLY_VERIFIED"));
        var commands =
            new CallCommandService(
                runtime.sql,
                "c001",
                1,
                c -> {
                  throw new AssertionError();
                },
                (c, s, p) -> false);
        var actors = ActorTestKit.create();
        try {
          var gate =
              (java.util.function.Supplier<Optional<AuthoritySql.GroupToken>>)
                  () ->
                      lease.checkLease()
                          ? lease.currentGrant().map(GroupOwnerRepository.Grant::token)
                          : Optional.empty();
          var backend = new CallCommandHandler(workflow, commands, gate, 1);
          var actor = actors.spawn(CallActor.create(call, backend, gate, Clock.systemUTC()));
          org.awaitility.Awaitility.await()
              .atMost(Duration.ofSeconds(5))
              .until(
                  () -> {
                    Optional<CallSnapshotRepository.Snapshot> snapshot =
                        AskPattern.ask(
                                actor,
                                CallActor.GetSnapshot::new,
                                Duration.ofSeconds(1),
                                actors.scheduler())
                            .toCompletableFuture()
                            .get(2, TimeUnit.SECONDS);
                    return snapshot.isPresent();
                  });
          System.out.println("TASK22:READY");
          System.out.flush();
          assertThat(new BufferedReader(new InputStreamReader(System.in)).readLine())
              .isEqualTo("MUTATE");
          var stale =
              new CallWorkflowService.Transition(
                  call,
                  old,
                  1,
                  1,
                  UUID.randomUUID(),
                  CallWorkflowService.Step.TERMINATE,
                  null,
                  List.of(),
                  null,
                  Instant.now().plusSeconds(5),
                  Instant.now().plusSeconds(30),
                  "TEST_ONLY_VERIFIED",
                  "CANCELED");
          assertThatThrownBy(
                  () -> CoordinatorGrantIT.done(workflow.apply(stale, Duration.ofSeconds(2))))
              .hasCauseInstanceOf(AuthoritySql.FencedException.class);
          assertThat(lease.checkLease()).isFalse();
          assertThat(CoordinatorGrantIT.done(roots.releaseTracked(old))).isTrue();
          assertThat(
                  roots
                      .reconcile(group, old.node(), old.incarnation())
                      .toCompletableFuture()
                      .join())
              .isEmpty();
          lease.drain().toCompletableFuture().get(5, TimeUnit.SECONDS);
          System.out.println("TASK22:FENCED");
          System.out.flush();
        } finally {
          actors.shutdownTestKit();
        }
      } finally {
        timers.shutdownNow();
      }
    }
  }
}
