package io.webrtc.signaling.actors.user;

import static org.assertj.core.api.Assertions.*;

import io.webrtc.signaling.actors.cluster.UserMessage;
import io.webrtc.signaling.protocol.Identity.*;
import io.webrtc.signaling.storage.*;
import io.webrtc.signaling.storage.HomeParticipationService.*;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import org.apache.pekko.actor.testkit.typed.javadsl.*;
import org.apache.pekko.actor.typed.*;
import org.apache.pekko.cluster.sharding.typed.javadsl.ClusterSharding;
import org.junit.jupiter.api.*;

class UserActorTest {
  static final UserId USER = new UserId("alice");
  static final Instant NOW = Instant.parse("2026-10-03T00:00:00Z");
  static final ActorTestKit kit = ActorTestKit.create(ManualTime.config());
  static final ManualTime time = ManualTime.get(kit.system());
  final TestProbe<ClusterSharding.ShardCommand> shard = kit.createTestProbe();
  final TestProbe<UserCommand.Result> replies = kit.createTestProbe();
  final Backend backend = new Backend();

  static final class Pending<T> {
    final CompletableFuture<T> logical = new CompletableFuture<>();
    final CompletableFuture<DbOperation.PhysicalCompletion> physical = new CompletableFuture<>();

    DbOperation<T> handle() {
      return new DbOperation<>(logical.minimalCompletionStage(), physical.minimalCompletionStage());
    }

    void complete(T value) {
      logical.complete(value);
      physical.complete(DbOperation.PhysicalCompletion.FINISHED);
    }
  }

  static final class Backend implements UserActor.Backend {
    final Pending<UserSnapshotService.Snapshot> hydration = new Pending<>();
    final BlockingQueue<Pending<UserCommand.Result>> work = new LinkedBlockingQueue<>();
    volatile int started;

    public DbOperation<UserSnapshotService.Snapshot> load(
        UserId user, long epoch, Duration budget) {
      return hydration.handle();
    }

    public DbOperation<UserCommand.Result> execute(
        UserCommand.Operation operation, Duration budget) {
      started++;
      var p = new Pending<UserCommand.Result>();
      work.add(p);
      return p.handle();
    }

    Pending<UserCommand.Result> next() throws Exception {
      return Objects.requireNonNull(work.poll(2, TimeUnit.SECONDS));
    }
  }

  ActorRef<UserMessage> spawn(List<SessionRepository.Route> routes) {
    return spawn(backend, routes);
  }

  ActorRef<UserMessage> spawn(Backend ownBackend, List<SessionRepository.Route> routes) {
    var actor =
        kit.spawn(
            UserActor.create(USER, 1, shard.ref(), ownBackend, Clock.fixed(NOW, ZoneOffset.UTC)));
    ownBackend.hydration.complete(new UserSnapshotService.Snapshot(USER, routes, null));
    return actor;
  }

  static SessionRepository.Route route(int device, long generation) {
    return new SessionRepository.Route(
        USER,
        new SessionKey("https://test-only.invalid", "jti-" + device),
        new SessionIncarnation(new UUID(0, device + 1)),
        generation,
        "gateway",
        new UUID(0, 10),
        new UUID(0, device + 20),
        NOW.plusSeconds(3600),
        "test-key",
        1);
  }

  static Request request() {
    var call = new CallId("c001.e1.00000000-0000-0000-0000-000000000001");
    return new Request(
        USER,
        call,
        new UUID(0, 90),
        "a".repeat(64),
        1,
        Phase.PREPARING,
        new Grant(
            "c001",
            1,
            1,
            HomeParticipationService.group(call),
            2,
            1,
            new UUID(0, 91),
            NOW,
            NOW.plusSeconds(5),
            "TEST_ONLY"));
  }

  static Participation participation(Winner winner) {
    var r = request();
    return new Participation(
        r.call(),
        USER,
        r.acquireOperation(),
        r.payloadHash(),
        "RESERVED",
        new UUID(0, 92),
        winner == null ? 1 : 2,
        NOW.plusSeconds(15),
        winner,
        2);
  }

  void send(ActorRef<UserMessage> actor, UserCommand.Operation op) {
    actor.tell(new UserCommand.Mutate(op, replies.ref(), NOW.plusSeconds(2), 1024));
  }

  UserState state(ActorRef<UserMessage> actor) {
    var probe = kit.<UserState>createTestProbe();
    actor.tell(new UserCommand.GetState(probe.ref()));
    return probe.receiveMessage();
  }

  @AfterAll
  static void shutdown() {
    kit.shutdownTestKit();
  }

  @Test
  void rehydratesFiveRoutesRejectsStaleBindingAndHasNoSocketChildren() {
    var actor = spawn(java.util.stream.IntStream.range(0, 5).mapToObj(n -> route(n, 2)).toList());
    assertThat(state(actor).routes()).hasSize(5);
    send(actor, new UserCommand.Accept(request(), new UUID(0, 92), route(0, 1)));
    assertThat(replies.receiveMessage().code()).isEqualTo(UserCommand.Code.STALE_BINDING);
    assertThat(backend.started).isZero();
    assertThatThrownBy(
            () ->
                new UserSnapshotService.Snapshot(
                    USER,
                    java.util.stream.IntStream.range(0, 6).mapToObj(n -> route(n, 1)).toList(),
                    null))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void serializesReserveReleaseUntilPhysicalCleanupEvenAfterCommittedLogicalReply()
      throws Exception {
    var actor = spawn(List.of());
    send(actor, new UserCommand.Reserve(request()));
    send(actor, new UserCommand.Release(request(), new UUID(0, 92), 1));
    var first = backend.next();
    first.logical.complete(
        new UserCommand.Result(UserCommand.Code.RESERVED, null, participation(null), null));
    assertThat(replies.receiveMessage().code()).isEqualTo(UserCommand.Code.RESERVED);
    assertThat(backend.started).isEqualTo(1);
    assertThat(backend.work).isEmpty();
    first.physical.complete(DbOperation.PhysicalCompletion.FINISHED);
    var second = backend.next();
    var r = request();
    second.complete(
        new UserCommand.Result(
            UserCommand.Code.RELEASED,
            null,
            new Participation(
                r.call(),
                USER,
                r.acquireOperation(),
                r.payloadHash(),
                "RELEASED",
                new UUID(0, 92),
                0,
                null,
                null,
                2),
            null));
    assertThat(replies.receiveMessage().code()).isEqualTo(UserCommand.Code.RELEASED);
    assertThat(state(actor).participation().terminal()).isTrue();
  }

  @Test
  void preservesImmutableWinnerInReplyAndCache() throws Exception {
    var route = route(0, 2);
    var actor = spawn(List.of(route));
    send(actor, new UserCommand.Accept(request(), new UUID(0, 92), route));
    var p = backend.next();
    var winner = new Winner(route.key(), route.incarnation(), 2);
    var claim =
        new AcceptWinnerService.Claim("CLAIMED", winner, new UUID(0, 92), 2, NOW.plusSeconds(15));
    p.complete(
        new UserCommand.Result(UserCommand.Code.CLAIMED, null, participation(winner), claim));
    assertThat(replies.receiveMessage().winner().winner()).isEqualTo(winner);
    assertThat(state(actor).participation().winner()).isEqualTo(winner);
    assertThatThrownBy(() -> state(actor).routes().clear())
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  void unknownOutcomeNeverStartsQueuedReplacementMutationBeforePhysicalCompletion()
      throws Exception {
    var actor = spawn(List.of());
    send(actor, new UserCommand.Reserve(request()));
    var p = backend.next();
    p.logical.completeExceptionally(new DbOutcomeUnknownException());
    assertThat(replies.receiveMessage().code()).isEqualTo(UserCommand.Code.UNKNOWN);
    send(actor, new UserCommand.Release(request(), null, 0));
    assertThat(replies.receiveMessage().code()).isEqualTo(UserCommand.Code.UNAVAILABLE);
    assertThat(backend.started).isEqualTo(1);
    shard.expectNoMessage(Duration.ofMillis(50));
    p.physical.complete(DbOperation.PhysicalCompletion.FINISHED);
    assertThat(shard.receiveMessage()).isInstanceOf(ClusterSharding.Passivate.class);
  }

  @Test
  void idlePassivationWaitsForInflightMutationWithoutPinningLiveSockets() throws Exception {
    var idle = spawn(List.of(route(0, 2)));
    assertThat(state(idle).routes()).hasSize(1);
    time.timePasses(Duration.ofSeconds(60));
    assertThat(shard.receiveMessage()).isInstanceOf(ClusterSharding.Passivate.class);
    kit.stop(idle);
    var activeBackend = new Backend();
    var actor = spawn(activeBackend, List.of());
    send(actor, new UserCommand.Reserve(request()));
    var p = activeBackend.next();
    time.timePasses(Duration.ofSeconds(60));
    shard.expectNoMessage(Duration.ofMillis(50));
    p.complete(new UserCommand.Result(UserCommand.Code.RESERVED, null, participation(null), null));
    assertThat(replies.receiveMessage().code()).isEqualTo(UserCommand.Code.RESERVED);
    assertThat(shard.receiveMessage()).isInstanceOf(ClusterSharding.Passivate.class);
  }

  @Test
  void stopMessageDrainsKnownMutationThenTerminatesOnlyAfterPhysicalCleanup() throws Exception {
    var actor = spawn(List.of());
    send(actor, new UserCommand.Reserve(request()));
    var p = backend.next();
    actor.tell(UserCommand.Stop.INSTANCE);
    p.logical.complete(
        new UserCommand.Result(UserCommand.Code.RESERVED, null, participation(null), null));
    assertThat(replies.receiveMessage().code()).isEqualTo(UserCommand.Code.RESERVED);
    send(actor, new UserCommand.Release(request(), null, 0));
    assertThat(replies.receiveMessage().code()).isEqualTo(UserCommand.Code.UNAVAILABLE);
    p.physical.complete(DbOperation.PhysicalCompletion.FINISHED);
    kit.createTestProbe().expectTerminated(actor);
  }

  @Test
  void lateCompletionAfterStoppedIncarnationCannotChangeReplacementOrSendFalseReply()
      throws Exception {
    var old = spawn(List.of());
    send(old, new UserCommand.Reserve(request()));
    var p = backend.next();
    kit.stop(old);
    var replacement = spawn(new Backend(), List.of(route(1, 3)));
    p.complete(new UserCommand.Result(UserCommand.Code.RESERVED, null, participation(null), null));
    replies.expectNoMessage(Duration.ofMillis(50));
    assertThat(state(replacement).participation()).isNull();
    assertThat(state(replacement).routes()).containsExactly(route(1, 3));
    assertThat(p.physical).isCompleted();
  }
}
