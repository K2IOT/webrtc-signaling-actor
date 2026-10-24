package io.webrtc.signaling.storage;

import io.webrtc.signaling.auth.AuthPrincipal;
import io.webrtc.signaling.protocol.CallCommand;
import io.webrtc.signaling.protocol.Identity.*;
import io.webrtc.signaling.protocol.SignalEnvelope;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeoutException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CallCommandIT {
    static DbTestRuntime runtime;
    static CallCommandService commands;
    static SessionRegistryService sessions;
    static GatewayLeaseRepository.Boot boot;
    static UUID owner = UUID.randomUUID();

    @BeforeAll
    static void setup() throws Exception {
        runtime = new DbTestRuntime();
        sessions = new SessionRegistryService(runtime.sql, "c001", 1);
        boot =
                sessions
                        .startGatewayBoot("command-gateway", UUID.randomUUID(), "test", UUID.randomUUID())
                        .toCompletableFuture()
                        .join();
        try (var c = PgFixture.connection();
             var s =
                     c.prepareStatement(
                             "INSERT INTO group_owner(cell_id,ownership_hash_version,group_id,storage_epoch,group_epoch,owner_node,owner_incarnation,status,lease_until,lease_sequence,acquire_operation_id) SELECT 'c001',1,n,1,1,'test-owner',?,'OWNED',clock_timestamp()+interval '60 seconds',1,gen_random_uuid() FROM generate_series(0,1023) n ON CONFLICT DO NOTHING")) {
            s.setObject(1, owner);
            s.executeUpdate();
        }
        commands =
                new CallCommandService(
                        runtime.sql,
                        "c001",
                        1,
                        command -> {
                            CallId call =
                                    command.callId() == null ? CallId.create("c001", 1) : command.callId();
                            var token =
                                    new AuthoritySql.GroupToken(
                                            "c001",
                                            1,
                                            1,
                                            HomeParticipationService.group(call),
                                            1,
                                            "test-owner",
                                            owner);
                            return CompletableFuture.completedFuture(
                                    new CallCommandService.Authority(call, token, 1, "TEST_ONLY"));
                        },
                        (command, snapshot, proof) -> proof.equals("TEST_ONLY"))
                        .businessAdmission(() -> true);
    }

    @AfterAll
    static void close() {
        runtime.close();
    }

    static AuthenticatedSession sender(String user) {
        var p =
                new AuthPrincipal(
                        new UserId(user),
                        new SessionKey("test-issuer", UUID.randomUUID().toString()),
                        Instant.now().plusSeconds(600),
                        Instant.now(),
                        "test-key",
                        0);
        var r = sessions.registerSession(p, boot, UUID.randomUUID(), 1).toCompletableFuture().join();
        return new AuthenticatedSession(
                r.user(), r.key(), r.incarnation(), r.connectionGeneration(), r.connectionId());
    }

    static CallCommand command(
            SignalEnvelope.Type type,
            AuthenticatedSession sender,
            RequestId request,
            CallId call,
            String payload,
            String hash) {
        return new CallCommand(
                type,
                sender,
                request,
                call,
                type == SignalEnvelope.Type.INVITE ? CommandScope.invite() : CommandScope.call(call),
                type == SignalEnvelope.Type.INVITE ? new UserId("target-" + sender.userId().value()) : null,
                null,
                null,
                payload,
                hash);
    }

    @Test
    void duplicateInviteRetainsOriginalCallReservationAndPendingIdentity() throws Exception {
        var sender = sender("dedup-caller");
        var command =
                command(
                        SignalEnvelope.Type.INVITE,
                        sender,
                        new RequestId(UUID.randomUUID()),
                        null,
                        "{}",
                        "11".repeat(32));
        var first = commands.executeCallCommand(command).toCompletableFuture().join();
        var replay = commands.executeCallCommand(command).toCompletableFuture().join();
        assertThat(replay).isEqualTo(first);
        assertThat(first.status()).isEqualTo("PENDING");
        try (var c = PgFixture.connection();
             var s = c.prepareStatement("SELECT count(*) FROM call_state WHERE caller_jti=?")) {
            s.setString(1, sender.key().jti());
            try (var r = s.executeQuery()) {
                r.next();
                assertThat(r.getInt(1)).isEqualTo(1);
            }
        }
        var conflict =
                command(
                        SignalEnvelope.Type.INVITE, sender, command.requestId(), null, "{}", "22".repeat(32));
        assertThatThrownBy(() -> commands.executeCallCommand(conflict).toCompletableFuture().join())
                .hasCauseInstanceOf(CommandResultRepository.IntentConflict.class);
    }

    @Test
    void scopesAreIndependentAndLostAckRecoversOriginalFinalOutcome() {
        var sender = sender("scope-caller");
        var request = new RequestId(UUID.randomUUID());
        var invite = command(SignalEnvelope.Type.INVITE, sender, request, null, "{}", "33".repeat(32));
        var created = commands.executeCallCommand(invite).toCompletableFuture().join();
        var cancel =
                command(
                        SignalEnvelope.Type.CANCEL, sender, request, created.callId(), "{}", "44".repeat(32));
        var lostAck =
                commands
                        .executeCallCommand(cancel)
                        .thenApply(
                                committed -> {
                                    throw new CompletionException(
                                            new TimeoutException("client lost post-COMMIT ACK"));
                                });
        assertThatThrownBy(() -> lostAck.toCompletableFuture().join())
                .hasCauseInstanceOf(TimeoutException.class);
        var recovered = commands.executeCallCommand(cancel).toCompletableFuture().join();
        assertThat(recovered.status()).isEqualTo("FINAL");
        assertThat(recovered.state()).isEqualTo("TERMINAL");
        assertThat(recovered.version()).isEqualTo(2);
        assertThat(
                commands.getCommandResult(sender, cancel.scope(), request).toCompletableFuture().join())
                .contains(recovered);
        var again =
                command(
                        SignalEnvelope.Type.CANCEL,
                        sender,
                        new RequestId(UUID.randomUUID()),
                        created.callId(),
                        "{}",
                        "55".repeat(32));
        var terminal = commands.executeCallCommand(again).toCompletableFuture().join();
        assertThat(terminal.version()).isEqualTo(2);
    }

    @Test
    void staleVersionCannotMutateAndUnrelatedSessionCannotCancel() {
        var sender = sender("version-caller");
        var initial =
                commands
                        .executeCallCommand(
                                command(
                                        SignalEnvelope.Type.INVITE,
                                        sender,
                                        new RequestId(UUID.randomUUID()),
                                        null,
                                        "{}",
                                        "66".repeat(32)))
                        .toCompletableFuture()
                        .join();
        var staleActor =
                new CallCommandService(
                        runtime.sql,
                        "c001",
                        1,
                        c ->
                                CompletableFuture.completedFuture(
                                        new CallCommandService.Authority(
                                                initial.callId(),
                                                new AuthoritySql.GroupToken(
                                                        "c001",
                                                        1,
                                                        1,
                                                        HomeParticipationService.group(initial.callId()),
                                                        1,
                                                        "test-owner",
                                                        owner),
                                                1,
                                                "TEST_ONLY",
                                                9)),
                        (c, s, p) -> p.equals("TEST_ONLY"))
                        .businessAdmission(() -> true);
        var stale =
                staleActor
                        .executeCallCommand(
                                command(
                                        SignalEnvelope.Type.CANCEL,
                                        sender,
                                        new RequestId(UUID.randomUUID()),
                                        initial.callId(),
                                        "{}",
                                        "77".repeat(32)))
                        .toCompletableFuture()
                        .join();
        assertThat(stale.code()).isEqualTo("STALE_VERSION");
        assertThat(
                commands
                        .loadCallSnapshot(sender, initial.callId())
                        .toCompletableFuture()
                        .join()
                        .state())
                .isEqualTo("PREPARING");
        var unrelated = sender("intruder");
        assertThatThrownBy(
                () ->
                        commands
                                .executeCallCommand(
                                        command(
                                                SignalEnvelope.Type.CANCEL,
                                                unrelated,
                                                new RequestId(UUID.randomUUID()),
                                                initial.callId(),
                                                "{}",
                                                "88".repeat(32)))
                                .toCompletableFuture()
                                .join())
                .hasCauseInstanceOf(CallCommandService.AuthorizationRejected.class);
    }

    @Test
    void beforeWorkTimeoutCannotCreateBusinessState() {
        var sender = sender("expired-budget");
        var command =
                command(
                        SignalEnvelope.Type.INVITE,
                        sender,
                        new RequestId(UUID.randomUUID()),
                        null,
                        "{}",
                        "99".repeat(32));
        assertThatThrownBy(
                () -> commands.executeCallCommand(command, Duration.ZERO).toCompletableFuture().join())
                .hasCauseInstanceOf(DbOverloadedException.class);
        assertThat(
                commands
                        .getCommandResult(sender, command.scope(), command.requestId())
                        .toCompletableFuture()
                        .join())
                .isEmpty();
    }

    @Test
    void callerTimeoutDuringSqlRetainsOriginalIdentityAndRollbackIsObservedOnPrimary() {
        var sender = sender("sql-timeout");
        UUID request = UUID.randomUUID();
        var timed =
                runtime.sql.submit(
                        DbClass.CRITICAL,
                        Duration.ofMillis(100),
                        c -> {
                            AuthoritySql.home(
                                    c,
                                    "c001",
                                    1,
                                    Map.of(SessionRegistryService.bucket(sender.userId()), 1L),
                                    List.of(sender.userId().value()));
                            try (var s =
                                         c.prepareStatement(
                                                 "INSERT INTO command_result(issuer,jti,command_scope,request_id,authority_bucket_id,payload_hash,status) VALUES(?,?,'INVITE',?,?,decode(repeat('aa',32),'hex'),'PENDING')")) {
                                s.setString(1, sender.key().issuer());
                                s.setString(2, sender.key().jti());
                                s.setObject(3, request);
                                s.setInt(4, SessionRegistryService.bucket(sender.userId()));
                                s.executeUpdate();
                            }
                            c.createStatement().execute("SELECT pg_sleep(1)");
                            return "impossible";
                        });
        assertThatThrownBy(() -> timed.toCompletableFuture().join())
                .cause()
                .isInstanceOfAny(DbOutcomeUnknownException.class, SqlTransactions.SqlWorkException.class);
        org.awaitility.Awaitility.await()
                .atMost(Duration.ofSeconds(3))
                .untilAsserted(
                        () ->
                                assertThat(
                                        commands
                                                .getCommandResult(sender, CommandScope.invite(), new RequestId(request))
                                                .toCompletableFuture()
                                                .join())
                                        .isEmpty());
    }

    static double secondsUntilNextAttempt(UUID event) throws Exception {
        try (var c = PgFixture.connection();
             var q =
                     c.prepareStatement(
                             "SELECT EXTRACT(EPOCH FROM next_attempt-clock_timestamp()) FROM control_outbox"
                                     + " WHERE event_id=?")) {
            q.setObject(1, event);
            try (var r = q.executeQuery()) {
                r.next();
                return r.getDouble(1);
            }
        }
    }

    @Test
    void undeliverableEventBacksOffExponentiallyUpToThirtySecondsInsteadOfEverySecond()
            throws Exception {
        var sender = sender("outbox-backoff-caller");
        var created =
                commands
                        .executeCallCommand(
                                command(
                                        SignalEnvelope.Type.INVITE,
                                        sender,
                                        new RequestId(UUID.randomUUID()),
                                        null,
                                        "{}",
                                        "cc".repeat(32)))
                        .toCompletableFuture()
                        .join();
        commands
                .executeCallCommand(
                        command(
                                SignalEnvelope.Type.CANCEL,
                                sender,
                                new RequestId(UUID.randomUUID()),
                                created.callId(),
                                "{}",
                                "dd".repeat(32)))
                .toCompletableFuture()
                .join();
        var outbox = new OutboxRepository(runtime.sql, "c001", 1);
        UUID incarnation = UUID.randomUUID();
        UUID event;
        try (var c = PgFixture.connection();
             var q = c.prepareStatement("SELECT event_id FROM control_outbox WHERE call_id=? LIMIT 1")) {
            q.setString(1, created.callId().value());
            try (var r = q.executeQuery()) {
                assertThat(r.next()).isTrue();
                event = r.getObject(1, UUID.class);
            }
        }
        var delays = new java.util.ArrayList<Double>();
        for (int attempt = 1; attempt <= 8; attempt++) {
            var target = event;
            var claim =
                    outbox
                            .scanOutboxBatchTracked("worker", incarnation, 2, List.of(event), java.util.Set.of())
                            .logical()
                            .toCompletableFuture()
                            .join()
                            .claims()
                            .stream()
                            .filter(c -> c.eventId().equals(target))
                            .findFirst()
                            .orElseThrow();
            assertThat(outbox.defer(claim, "worker", incarnation, false).logical().toCompletableFuture().join())
                    .isTrue();
            delays.add(secondsUntilNextAttempt(event));
            // TEST_ONLY: make the deferred event due again instead of sleeping through the backoff.
            try (var c = PgFixture.connection();
                 var q = c.prepareStatement("UPDATE control_outbox SET next_attempt=clock_timestamp() WHERE event_id=?")) {
                q.setObject(1, event);
                q.executeUpdate();
            }
        }
        assertThat(delays.get(0)).isBetween(0.4, 1.0);
        assertThat(delays.get(3)).isBetween(3.9, 8.0);
        assertThat(delays.get(7)).isBetween(14.9, 30.0);
        assertThat(delays).allSatisfy(d -> assertThat(d).isLessThanOrEqualTo(30.0));
        // A claim this worker had no capacity to try keeps the one-second retry.
        var target = event;
        var unattempted =
                outbox
                        .scanOutboxBatchTracked("worker", incarnation, 2, List.of(event), java.util.Set.of())
                        .logical()
                        .toCompletableFuture()
                        .join()
                        .claims()
                        .stream()
                        .filter(c -> c.eventId().equals(target))
                        .findFirst()
                        .orElseThrow();
        assertThat(
                outbox
                        .defer(unattempted, "worker", incarnation, false, false)
                        .logical()
                        .toCompletableFuture()
                        .join())
                .isTrue();
        assertThat(secondsUntilNextAttempt(event)).isBetween(0.9, 1.0);

        // Only due work keeps the scanner at its busy cadence: a deferred event lets it back off.
        try (var c = PgFixture.connection();
             var q = c.createStatement()) {
            q.executeUpdate(
                    "UPDATE control_outbox SET next_attempt=clock_timestamp()+interval '1 hour' WHERE"
                            + " delivery_state='PENDING'");
        }
        var idle =
                outbox.scanOutboxBatchTracked("worker", incarnation, 2, List.of(), java.util.Set.of())
                        .logical()
                        .toCompletableFuture()
                        .join();
        assertThat(idle.claims()).isEmpty();
        assertThat(idle.pending()).isFalse();
        try (var c = PgFixture.connection();
             var q = c.prepareStatement("UPDATE control_outbox SET next_attempt=clock_timestamp() WHERE event_id=?")) {
            q.setObject(1, event);
            q.executeUpdate();
        }
        assertThat(
                outbox.scanOutboxBatchTracked("worker", incarnation, 1, List.of(), java.util.Set.of(event))
                        .logical()
                        .toCompletableFuture()
                        .join()
                        .pending())
                .isTrue();
    }

    @Test
    void outboxIsCommittedWithTerminalResultAndClaimGenerationFencesLateCompletion() {
        var sender = sender("outbox-caller");
        var created =
                commands
                        .executeCallCommand(
                                command(
                                        SignalEnvelope.Type.INVITE,
                                        sender,
                                        new RequestId(UUID.randomUUID()),
                                        null,
                                        "{}",
                                        "aa".repeat(32)))
                        .toCompletableFuture()
                        .join();
        commands
                .executeCallCommand(
                        command(
                                SignalEnvelope.Type.CANCEL,
                                sender,
                                new RequestId(UUID.randomUUID()),
                                created.callId(),
                                "{}",
                                "bb".repeat(32)))
                .toCompletableFuture()
                .join();
        var outbox = new OutboxRepository(runtime.sql, "c001", 1);
        UUID incarnation = UUID.randomUUID();
        var claims = outbox.claimOutboxBatch("worker", incarnation, 128).toCompletableFuture().join();
        assertThat(claims).isNotEmpty();
        var claim =
                claims.stream().filter(c -> c.callId().equals(created.callId())).findFirst().orElseThrow();
        assertThat(outbox.complete(claim, "other-worker", incarnation).toCompletableFuture().join())
                .isFalse();
        assertThat(outbox.complete(claim, "worker", incarnation).toCompletableFuture().join()).isTrue();
    }
}
