package io.webrtc.signaling.control;

import static org.assertj.core.api.Assertions.*;

import io.webrtc.signaling.protocol.Identity.UserId;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import org.junit.jupiter.api.Test;

class DirectoryBootstrapTest {
  final Instant now = Instant.parse("2026-10-03T00:00:00Z");

  @Test
  void stableHashVectorsUseFixed16384BucketsAndNfc() {
    assertThat(BucketHasher.bucket(new UserId("alice"))).isEqualTo(175);
    assertThat(BucketHasher.bucket(new UserId("bob"))).isEqualTo(1754);
    assertThat(BucketHasher.bucket(new UserId("é"))).isEqualTo(851);
    assertThat(BucketHasher.bucket(new UserId("e\u0301"))).isEqualTo(851);
    assertThat(BucketHasher.bucket(new UserId("用户-1"))).isEqualTo(14634);
  }

  @Test
  void cacheRefreshesAtThirtySecondsAndOutageDoesNotGrantMutationAuthority() {
    var repo = new TestDirectory();
    repo.route = new HomeRoute(175, "c001", 1, "wss://c001.test/signal");
    var service = new DirectoryService(repo, 16384, Duration.ofSeconds(30));
    assertThat(service.resolveHome(new UserId("alice"), now).toCompletableFuture().join().epoch())
        .isEqualTo(1);
    repo.route = new HomeRoute(175, "c002", 2, "wss://c002.test/signal");
    assertThat(
            service
                .resolveHome(new UserId("alice"), now.plusSeconds(30))
                .toCompletableFuture()
                .join()
                .epoch())
        .isEqualTo(2);
    repo.down = true;
    assertThat(
            service
                .resolveHome(new UserId("alice"), now.plusSeconds(61))
                .toCompletableFuture()
                .join()
                .cell())
        .isEqualTo("c002");
    assertThatThrownBy(
            () -> service.requireLocal(new UserId("alice"), "c001", 1).toCompletableFuture().join())
        .hasCauseInstanceOf(WrongCellException.class);
    assertThatThrownBy(() -> new DirectoryService(repo, 16384, Duration.ofSeconds(31)))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void unknownRouteFailsClosedDuringDirectoryOutage() {
    var repo = new TestDirectory();
    repo.down = true;
    var d = new DirectoryService(repo, 16384, Duration.ofSeconds(30));
    assertThatThrownBy(() -> d.resolveHome(new UserId("alice"), now).toCompletableFuture().join())
        .isInstanceOf(CompletionException.class);
  }

  @Test
  void freezeAndDestinationInstallPrecedeCasPublication() {
    var calls = new ArrayList<String>();
    var repo = new TestDirectory();
    repo.route = new HomeRoute(175, "c001", 1, "wss://c001.test/signal");
    var transfer =
        new BucketTransferService(
            repo,
            new BucketTransferService.AuthorityTransfer() {
              public CompletionStage<BucketTransferService.FreezeReceipt> freeze(HomeRoute source) {
                calls.add("freeze");
                return CompletableFuture.completedFuture(
                    new BucketTransferService.FreezeReceipt(
                        source.bucket(), source.cell(), source.epoch(), UUID.randomUUID()));
              }

              public CompletionStage<Void> copyAndInstall(
                  BucketTransferService.FreezeReceipt receipt, HomeRoute target) {
                calls.add("install");
                return CompletableFuture.completedFuture(null);
              }
            });
    transfer
        .transfer(repo.route, new HomeRoute(175, "c002", 2, "wss://c002.test/signal"))
        .toCompletableFuture()
        .join();
    assertThat(calls).containsExactly("freeze", "install");
    assertThat(repo.route.cell()).isEqualTo("c002");
  }

  static final class TestDirectory implements DirectoryRepository {
    HomeRoute route;
    boolean down;

    public CompletionStage<Optional<HomeRoute>> read(int b) {
      return down
          ? CompletableFuture.failedFuture(new IllegalStateException("directory unavailable"))
          : CompletableFuture.completedFuture(Optional.ofNullable(route));
    }

    public CompletionStage<Boolean> compareAndPublish(HomeRoute previous, HomeRoute next) {
      if (!Objects.equals(route, previous)) return CompletableFuture.completedFuture(false);
      route = next;
      return CompletableFuture.completedFuture(true);
    }

    public CompletionStage<Boolean> localActive(int b, String cell, long epoch) {
      return CompletableFuture.completedFuture(
          route != null && route.cell().equals(cell) && route.epoch() == epoch);
    }
  }
}
