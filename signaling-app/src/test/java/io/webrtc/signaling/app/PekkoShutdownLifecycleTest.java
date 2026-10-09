package io.webrtc.signaling.app;

import static org.assertj.core.api.Assertions.*;

import com.typesafe.config.ConfigFactory;
import java.util.concurrent.*;
import org.apache.pekko.actor.CoordinatedShutdown;
import org.apache.pekko.actor.typed.ActorSystem;
import org.apache.pekko.actor.typed.javadsl.Behaviors;
import org.junit.jupiter.api.Test;

class PekkoShutdownLifecycleTest {
  @Test
  void realFrameworkShutdownRetainsDatabaseUntilNativeReleaseAndFinalCleanup() throws Exception {
    var config =
        PekkoShutdownLifecycle.config()
            .withFallback(
                ConfigFactory.parseString(
                    "pekko.actor.provider=local\npekko.coordinated-shutdown.run-by-jvm-shutdown-hook=off"));
    var system = ActorSystem.create(Behaviors.empty(), "lifecycle-test", config);
    var hooks = new ShutdownCoordinatorTest.Hooks();
    hooks.settled.complete(null);
    try {
      PekkoShutdownLifecycle.register(system, hooks);
      var done = CoordinatedShutdown.get(system).runAll(CoordinatedShutdown.unknownReason());
      for (int i = 0; i < 100 && !hooks.events.contains("release"); i++) Thread.sleep(10);
      assertThat(hooks.events).contains("release");
      assertThat(hooks.dbClosed).isFalse();
      hooks.released.complete(null);
      done.toCompletableFuture().get(4, TimeUnit.SECONDS);
      assertThat(hooks.events)
          .containsExactly("ready-off", "shed", "settle", "release", "settle", "db-close");
      assertThat(hooks.events).doesNotContain("leave");
      assertThat(PekkoShutdownLifecycle.totalBudget(config))
          .isLessThanOrEqualTo(java.time.Duration.ofSeconds(65));
    } finally {
      system.terminate();
      system.getWhenTerminated().toCompletableFuture().get(4, TimeUnit.SECONDS);
    }
  }
}
