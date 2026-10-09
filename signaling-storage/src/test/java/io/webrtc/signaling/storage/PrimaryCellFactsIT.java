package io.webrtc.signaling.storage;

import static org.assertj.core.api.Assertions.*;

import java.time.Duration;
import java.time.Instant;
import org.junit.jupiter.api.Test;

class PrimaryCellFactsIT {
  @Test
  void actualSafetyPoolPrimaryFactsDoNotAuthorizeWrongEpochOrRecoveringCell() throws Exception {
    try (var f = new LocalInviteAtomicIT.Fixture()) {
      var source = new PrimaryCellFacts(f.runtime.sql, "c001", 1);
      var began = Instant.now();
      var active = CoordinatorGrantIT.done(source.poll(Duration.ofSeconds(2)));
      assertThat(active.primary()).isTrue();
      assertThat(active.cell()).isEqualTo("c001");
      assertThat(active.storageEpoch()).isEqualTo(1);
      assertThat(active.checkedAt()).isBetween(began, Instant.now());
      assertThat(active.usable(System.nanoTime())).isTrue();
      assertThat(active.usable(active.sampledAtNanos() + Duration.ofSeconds(1).toNanos()))
          .isFalse();
      assertThat(active.usable(active.sampledAtNanos() - 1)).isFalse();
      var wrong =
          CoordinatorGrantIT.done(
              new PrimaryCellFacts(f.runtime.sql, "c001", 2).poll(Duration.ofSeconds(2)));
      assertThat(wrong.primary()).isTrue();
      assertThat(wrong.usable(System.nanoTime())).isFalse();
      try (var c = f.connection();
          var update = c.createStatement()) {
        update.executeUpdate("UPDATE cell_authority SET status='RECOVERING' WHERE singleton_id=1");
      }
      var recovering = CoordinatorGrantIT.done(source.poll(Duration.ofSeconds(2)));
      assertThat(recovering.status()).isEqualTo("RECOVERING");
      assertThat(recovering.usable(System.nanoTime())).isFalse();
    }
  }
}
