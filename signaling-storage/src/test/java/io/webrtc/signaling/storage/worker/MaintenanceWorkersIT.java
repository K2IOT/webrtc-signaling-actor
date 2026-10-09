package io.webrtc.signaling.storage;

import static org.assertj.core.api.Assertions.*;

import io.webrtc.signaling.auth.*;
import io.webrtc.signaling.protocol.*;
import io.webrtc.signaling.protocol.Identity.*;
import io.webrtc.signaling.storage.worker.*;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import org.junit.jupiter.api.Test;

class MaintenanceWorkersIT {
  static <T> T done(DbOperation<T> op) {
    try {
      return op.logical().toCompletableFuture().join();
    } finally {
      op.physicalCompletion().toCompletableFuture().join();
    }
  }

  static long count(LocalInviteAtomicIT.Fixture f, String table) throws Exception {
    try (var c = f.connection();
        var q = c.createStatement();
        var r = q.executeQuery("SELECT count(*) FROM " + table)) {
      r.next();
      return r.getLong(1);
    }
  }

  @Test
  void lostHintIsScannedAndSocketWriteDoesNotCompleteApplicationDelivery() throws Exception {
    try (var f = new LocalInviteAtomicIT.Fixture()) {
      var caller = f.sender("dispatch-caller");
      var callee = f.sender("dispatch-callee");
      f.service()
          .executeCallCommand(f.invite(caller, callee.userId()))
          .toCompletableFuture()
          .join();
      var sent = new AtomicInteger();
      var application = new AtomicBoolean();
      var dispatcher =
          new OutboxDispatcher(
              new OutboxRepository(f.runtime.sql, "c001", 1),
              "TEST_ONLY_WORKER",
              UUID.randomUUID(),
              (event, budget) -> {
                sent.incrementAndGet();
                return new DbOperation<>(
                    CompletableFuture.completedFuture(
                        new OutboxDispatcher.Receipt(
                            event.eventId(),
                            application.get()
                                ? OutboxDispatcher.Kind.APPLICATION_RECEIVED
                                : OutboxDispatcher.Kind.WRITE_COMPLETED)),
                    CompletableFuture.completedFuture(null));
              },
              (event, receipt) -> event.eventId().equals(receipt.eventId()));
      assertThat(dispatcher.poll(128).toCompletableFuture().join().delivered()).isZero();
      try (var c = f.connection();
          var q = c.createStatement();
          var r =
              q.executeQuery(
                  "SELECT count(*) FROM control_outbox WHERE delivery_state='DELIVERED'")) {
        r.next();
        assertThat(r.getInt(1)).isZero();
      }
      try (var c = f.connection();
          var q = c.createStatement()) {
        q.execute(
            "UPDATE control_outbox SET dispatch_until=clock_timestamp()-interval '1 second',next_attempt=clock_timestamp()-interval '1 second'");
      }
      application.set(true);
      assertThat(dispatcher.poll(128).toCompletableFuture().join().delivered()).isEqualTo(2);
      assertThat(sent.get()).isEqualTo(4);
      assertThat(dispatcher.poll(128).toCompletableFuture().join().claimed()).isZero();
    }
  }

  @Test
  void logicalDeliveryTimeoutKeepsWorkerCreditUntilPhysicalCleanup() throws Exception {
    try (var f = new LocalInviteAtomicIT.Fixture()) {
      var caller = f.sender("physical-caller");
      var callee = f.sender("physical-callee");
      f.service()
          .executeCallCommand(f.invite(caller, callee.userId()))
          .toCompletableFuture()
          .join();
      var cleanup = new CompletableFuture<DbOperation.PhysicalCompletion>();
      var dispatcher =
          new OutboxDispatcher(
              new OutboxRepository(f.runtime.sql, "c001", 1),
              "TEST_ONLY",
              UUID.randomUUID(),
              (event, budget) ->
                  new DbOperation<>(
                      CompletableFuture.failedFuture(new TimeoutException()), cleanup),
              (event, receipt) -> false);
      dispatcher.poll(2).toCompletableFuture().join();
      assertThatThrownBy(() -> dispatcher.poll(2).toCompletableFuture().join())
          .hasCauseInstanceOf(DbOverloadedException.class);
      cleanup.complete(DbOperation.PhysicalCompletion.FINISHED);
      assertThat(dispatcher.poll(2).toCompletableFuture().join().claimed()).isZero();
    }
  }

  @Test
  void stableUpperBoundPaginationDefersConcurrentInsertsToTheNextSweep() throws Exception {
    try (var f = new LocalInviteAtomicIT.Fixture()) {
      for (int n = 0; n < 5; n++) {
        var caller = f.sender("scan-caller-" + n);
        var callee = f.sender("scan-callee-" + n);
        f.service()
            .executeCallCommand(f.invite(caller, callee.userId()))
            .toCompletableFuture()
            .join();
      }
      var scanner = new RecoveryScanner(f.runtime.sql, "c001", 1);
      long start = System.nanoTime();
      var first = done(scanner.scan(null, 2));
      var keys = new HashSet<CallId>();
      first.rows().forEach(row -> keys.add(row.call()));
      var caller = f.sender("scan-inserted-caller");
      var callee = f.sender("scan-inserted-callee");
      var inserted =
          f.service()
              .executeCallCommand(f.invite(caller, callee.userId()))
              .toCompletableFuture()
              .join()
              .callId();
      var cursor = first.next();
      int pages = 1;
      while (cursor != null) {
        var page = done(scanner.scan(cursor, 2));
        page.rows().forEach(row -> assertThat(keys.add(row.call())).isTrue());
        cursor = page.next();
        assertThat(++pages).isLessThan(8);
      }
      var all = done(scanner.scan(null, 512));
      assertThat(all.rows().stream().map(RecoveryScanner.Row::call)).contains(inserted);
      long elapsed = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
      assertThat(elapsed).isLessThan(5000);
      System.out.println(
          "TEST_ONLY active-index sweep: "
              + all.rows().size()
              + " rows, "
              + elapsed
              + " ms; target 5000 ms");
    }
  }

  @Test
  void finalResultRetentionPreservesTwentyFourHoursAndLiveCallOrigin() throws Exception {
    try (var f = new LocalInviteAtomicIT.Fixture()) {
      var caller = f.sender("retain-caller");
      var callee = f.sender("retain-callee");
      var result =
          f.service()
              .executeCallCommand(f.invite(caller, callee.userId()))
              .toCompletableFuture()
              .join();
      int bucket = SessionRegistryService.bucket(caller.userId());
      try (var c = f.connection();
          var q =
              c.prepareStatement(
                  "INSERT INTO command_result(issuer,jti,command_scope,request_id,authority_bucket_id,payload_hash,status,call_id,result,finalized_at,expires_at) VALUES(?,?,'INVITE',?,?,decode(repeat('a',64),'hex'),'FINAL',?,'{}',clock_timestamp()-interval '25 hours',clock_timestamp()-interval '1 hour')")) {
        q.setString(1, caller.key().issuer());
        q.setString(2, caller.key().jti());
        q.setObject(3, UUID.randomUUID());
        q.setInt(4, bucket);
        q.setString(5, result.callId().value());
        q.executeUpdate();
      }
      try (var c = f.connection();
          var q =
              c.prepareStatement(
                  "INSERT INTO command_result(issuer,jti,command_scope,request_id,authority_bucket_id,payload_hash,status,result,finalized_at,expires_at) VALUES(?, ?, 'INVITE', ?, ?,decode(repeat('b',64),'hex'),'FINAL','{}',clock_timestamp()-interval '25 hours',clock_timestamp()-interval '1 hour')")) {
        q.setString(1, caller.key().issuer());
        q.setString(2, caller.key().jti());
        q.setObject(3, UUID.randomUUID());
        q.setInt(4, bucket);
        q.executeUpdate();
      }
      var worker = new RetentionWorker(f.runtime.sql, "c001", 1);
      assertThat(done(worker.prune(bucket, 128)).commandResults()).isEqualTo(1);
      assertThat(count(f, "command_result")).isEqualTo(2);
      try (var c = f.connection();
          var q =
              c.prepareStatement(
                  "UPDATE call_state SET state='TERMINAL',terminal_reason='TEST_ONLY',terminal_at=clock_timestamp(),expires_at=clock_timestamp()+interval '24 hours' WHERE call_id=?")) {
        q.setString(1, result.callId().value());
        q.executeUpdate();
      }
      done(worker.prune(bucket, 128));
      assertThat(count(f, "command_result")).isEqualTo(2);
    }
  }

  @Test
  void authenticatedRevocationHighWaterCannotRegressSkipOrRefreshFromDuplicate() throws Exception {
    try (var f = new LocalInviteAtomicIT.Fixture()) {
      var sender = f.sender("revoked-subject");
      var checked = Instant.now();
      var event =
          new RevocationState.Event(
              sender.key().issuer(), sender.userId(), sender.key().jti(), 1, 1, checked);
      var worker =
          new RevocationReconciler(f.runtime.sql, "c001", 1, Duration.ofSeconds(5), batch -> true);
      var batch = new RevocationReconciler.Batch(0, 1, List.of(event), checked);
      assertThat(done(worker.apply(batch))).isEqualTo(1);
      assertThat(done(worker.apply(batch))).isEqualTo(1);
      try (var c = f.connection()) {
        var principal = SessionAuthReadIT.principal(new SessionRepository().find(c, sender.key()));
        assertThat(worker.allowed(c, principal)).isFalse();
      }
      assertThatThrownBy(
              () -> done(worker.apply(new RevocationReconciler.Batch(3, 4, List.of(), checked))))
          .hasCauseInstanceOf(AuthoritySql.FencedException.class);
    }
  }

  @Test
  void unauthenticatedRevocationProgressCannotMintFreshness() throws Exception {
    try (var f = new LocalInviteAtomicIT.Fixture()) {
      var worker =
          new RevocationReconciler(f.runtime.sql, "c001", 1, Duration.ofSeconds(5), batch -> false);
      assertThatThrownBy(
              () ->
                  done(
                      worker.apply(new RevocationReconciler.Batch(0, 0, List.of(), Instant.now()))))
          .hasCauseInstanceOf(AuthoritySql.FencedException.class);
    }
  }

  @Test
  void privacyDeletionRetainsOpaqueReplayMarkerAndNativeSafetyRecords() throws Exception {
    try (var f = new LocalInviteAtomicIT.Fixture()) {
      var caller = f.sender("privacy-caller");
      var callee = f.sender("privacy-callee");
      f.service()
          .executeCallCommand(f.invite(caller, callee.userId()))
          .toCompletableFuture()
          .join();
      var detached = new AtomicReference<String>();
      var worker =
          new PrivacyDeletionWorker(
              f.runtime.sql,
              "c001",
              1,
              new byte[32],
              opaque -> {
                detached.set(opaque);
                return new DbOperation<>(
                    CompletableFuture.completedFuture(null),
                    CompletableFuture.completedFuture(DbOperation.PhysicalCompletion.FINISHED));
              });
      var request = UUID.randomUUID();
      done(worker.request(caller.key().issuer(), caller.userId(), request));
      assertThat(worker.process(16).toCompletableFuture().join()).isEqualTo(1);
      assertThat(detached.get()).matches("[a-f0-9]{64}").doesNotContain(caller.userId().value());
      assertThat(count(f, "home_participation")).isEqualTo(2);
      assertThat(count(f, "command_result")).isEqualTo(1);
      assertThat(count(f, "privacy_deletion_request")).isEqualTo(1);
      assertThat(worker.process(16).toCompletableFuture().join()).isZero();
    }
  }

  @Test
  void saturatedMaintenanceDoesNotConsumeSafetyRenewalCapacity() throws Exception {
    try (var f = new LocalInviteAtomicIT.Fixture()) {
      var entered = new CountDownLatch(1);
      var release = new CountDownLatch(1);
      var maintenance =
          f.runtime.sql.submitTracked(
              DbClass.MAINTENANCE,
              Duration.ofSeconds(2),
              c -> {
                entered.countDown();
                if (!release.await(1, TimeUnit.SECONDS))
                  throw new AssertionError("Fixture release missing");
                return "DONE";
              });
      assertThat(entered.await(1, TimeUnit.SECONDS)).isTrue();
      try {
        var renewed =
            done(
                f.runtime.sql.submitTracked(
                    DbClass.RENEWAL, Duration.ofSeconds(1), c -> "RENEWED"));
        assertThat(renewed).isEqualTo("RENEWED");
        assertThatThrownBy(() -> done(new RetentionWorker(f.runtime.sql, "c001", 1).prune(0, 128)))
            .hasCauseInstanceOf(DbOverloadedException.class);
      } finally {
        release.countDown();
        done(maintenance);
      }
    }
  }

  @Test
  void routeLossEvidenceComesFromNativeHistoryAndKeepsOriginalObservationTime() throws Exception {
    try (var f = new LocalInviteAtomicIT.Fixture()) {
      var sender = f.sender("loss-caller");
      SessionRepository.Route original;
      try (var c = f.connection()) {
        original = new SessionRepository().find(c, sender.key());
      }
      var reads = new NativeRouteLossReadService(f.runtime.sql, "c001", 1);
      assertThatThrownBy(() -> done(reads.observe(original, 1, Duration.ofSeconds(2))))
          .hasCauseInstanceOf(AuthoritySql.FencedException.class);
      assertThat(f.sessions.closeSessionIfGeneration(original, 1).toCompletableFuture().join())
          .isTrue();
      var closed = done(reads.observe(original, 1, Duration.ofSeconds(2)));
      assertThat(closed.cause()).isEqualTo(NativeRouteLossReadService.Cause.CLOSED);
      assertThat(done(reads.observe(original, 1, Duration.ofSeconds(2))).observedAt())
          .isEqualTo(closed.observedAt());
      var current =
          f.sessions
              .registerSession(SessionAuthReadIT.principal(original), f.boot, UUID.randomUUID(), 1)
              .toCompletableFuture()
              .join();
      var replaced = done(reads.observe(original, 1, Duration.ofSeconds(2)));
      assertThat(replaced.cause()).isEqualTo(NativeRouteLossReadService.Cause.REPLACED);
      assertThat(replaced.session()).isEqualTo(sender);
      assertThatThrownBy(() -> done(reads.observe(current, 1, Duration.ofSeconds(2))))
          .hasCauseInstanceOf(AuthoritySql.FencedException.class);
    }
  }

  @Test
  void keyRetirementDeniesExistingNativeAuthorizationAndProgressIsReadFromPrimary()
      throws Exception {
    try (var f = new LocalInviteAtomicIT.Fixture()) {
      var sender = f.sender("retired-key-subject");
      SessionRepository.Route route;
      try (var c = f.connection()) {
        route = new SessionRepository().find(c, sender.key());
      }
      var principal = SessionAuthReadIT.principal(route);
      var worker =
          new RevocationReconciler(f.runtime.sql, "c001", 1, Duration.ofSeconds(5), batch -> true);
      var now = Instant.now();
      done(worker.apply(new RevocationReconciler.Batch(0, 0, List.of(), now)));
      try (var c = f.connection()) {
        assertThat(worker.allowed(c, principal)).isTrue();
      }
      var retirement =
          new RevocationReconciler.KeyRetirement(
              route.key().issuer(), route.signingKeyId(), 1, Instant.now());
      done(
          worker.apply(
              new RevocationReconciler.Batch(
                  0, 1, List.of(), retirement.committedAt(), "TEST_ONLY", List.of(retirement))));
      assertThat(done(worker.progress()).offset()).isEqualTo(1);
      try (var c = f.connection()) {
        assertThat(worker.allowed(c, principal)).isFalse();
      }
    }
  }
}
