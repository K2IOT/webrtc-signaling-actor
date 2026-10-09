package io.webrtc.signaling.storage;

import static org.assertj.core.api.Assertions.*;

import io.webrtc.signaling.auth.AuthPrincipal;
import io.webrtc.signaling.protocol.Identity.*;
import java.sql.*;
import java.time.*;
import java.util.*;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.Test;

class SessionStorageFenceIT {
  @Test
  void restoredCellCannotReuseOrHydrateOldGatewayBootDespiteItsUnexpiredLease() throws Exception {
    String schema = "restore_fence_" + UUID.randomUUID().toString().replace("-", "");
    String url = PgFixture.PG.getJdbcUrl() + "&currentSchema=" + schema;
    Flyway.configure()
        .dataSource(url, PgFixture.PG.getUsername(), PgFixture.PG.getPassword())
        .schemas(schema)
        .defaultSchema(schema)
        .locations("classpath:db/migration")
        .load()
        .migrate();
    try (var runtime =
        new DbTestRuntime(url, PgFixture.PG.getUsername(), PgFixture.PG.getPassword())) {
      var service1 = new SessionRegistryService(runtime.sql, "c001", 1);
      var boot =
          service1
              .startGatewayBoot("restore-test", UUID.randomUUID(), "TEST_ONLY", UUID.randomUUID())
              .toCompletableFuture()
              .join();
      var user = new UserId("restore-user");
      var principal =
          new AuthPrincipal(
              user,
              new SessionKey("TEST_ONLY", "restore-jti"),
              Instant.now().plusSeconds(600),
              Instant.now(),
              "test-key",
              1);
      service1.registerSession(principal, boot, UUID.randomUUID(), 1).toCompletableFuture().join();
      try (var c =
              DriverManager.getConnection(
                  url, PgFixture.PG.getUsername(), PgFixture.PG.getPassword());
          var s = c.createStatement()) {
        s.execute("UPDATE cell_authority SET storage_epoch=2");
      }
      var service2 = new SessionRegistryService(runtime.sql, "c001", 2);
      assertThatThrownBy(
              () ->
                  service2
                      .registerSession(principal, boot, UUID.randomUUID(), 1)
                      .toCompletableFuture()
                      .join())
          .hasCauseInstanceOf(AuthoritySql.FencedException.class);
      assertThatThrownBy(
              () ->
                  service2
                      .renewGatewayBoot(boot, 2, UUID.randomUUID())
                      .toCompletableFuture()
                      .join())
          .hasCauseInstanceOf(AuthoritySql.FencedException.class);
      assertThat(service2.lookupLiveRoutes(user, 1).toCompletableFuture().join()).isEmpty();
      assertThat(
              new UserSnapshotService(runtime.sql, "c001", 2)
                  .load(user, 1, Duration.ofSeconds(2))
                  .logical()
                  .toCompletableFuture()
                  .join()
                  .routes())
          .isEmpty();
      var fresh =
          service2
              .startGatewayBoot("restore-test", UUID.randomUUID(), "TEST_ONLY", UUID.randomUUID())
              .toCompletableFuture()
              .join();
      var route =
          service2
              .registerSession(principal, fresh, UUID.randomUUID(), 1)
              .toCompletableFuture()
              .join();
      assertThat(route.bootId()).isEqualTo(fresh.bootId());
      assertThat(service2.lookupLiveRoutes(user, 1).toCompletableFuture().join())
          .containsExactly(route);
    }
  }
}
