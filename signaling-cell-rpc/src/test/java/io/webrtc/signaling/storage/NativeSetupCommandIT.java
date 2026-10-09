package io.webrtc.signaling.storage;

import static org.assertj.core.api.Assertions.*;

import io.webrtc.signaling.actors.call.CallActor;
import io.webrtc.signaling.actors.user.*;
import io.webrtc.signaling.auth.*;
import io.webrtc.signaling.protocol.*;
import io.webrtc.signaling.protocol.Identity.*;
import io.webrtc.signaling.rpc.*;
import java.security.*;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import org.junit.jupiter.api.Test;

/**
 * Native PostgreSQL business/proof pipeline; direct tracked ingress is TEST_ONLY, no deployed
 * capacity claim.
 */
class NativeSetupCommandIT {
  @Test
  void publicAcceptDrivesBothNativeHomesAndActivationWithoutFixturePhasePlanner() throws Exception {
    run(false);
  }

  @Test
  void callerReconnectDuringRingingRebindsBeforeBothNativeHomesActivate() throws Exception {
    run(true);
  }

  @Test
  void relayAuthorizationUsesNativeCommittedRoundAndBothActiveHomes() throws Exception {
    run(false, true);
  }

  private void run(boolean reconnect) throws Exception {
    run(reconnect, false);
  }

  private void run(boolean reconnect, boolean relayCheck) throws Exception {
    try (var f = new LocalInviteAtomicIT.Fixture()) {
      var caller = f.sender("setup-caller");
      var callee = f.sender("setup-callee");
      var invite = f.invite(caller, callee.userId());
      var call = f.service().executeCallCommand(invite).toCompletableFuture().join().callId();
      var group = f.token(call);
      var keys = KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
      var proofs =
          new HomeAuthorizationProof(
              "c001", "test", keys.getPrivate(), Map.of("c001/test", keys.getPublic()));
      var clock = Clock.systemUTC();
      var bindings = new ProofBindings(proofs, clock);
      var issuer = new NativeProofIssuer(proofs, clock, () -> true, group::equals);
      var commands =
          new CallCommandService(
                  f.runtime.sql,
                  "c001",
                  1,
                  1,
                  c -> CompletableFuture.failedFuture(new AssertionError()),
                  bindings.commandVerifier(
                      "c001", u -> new ProofBindings.TrustedHome("c001", 1, 1)),
                  bindings.negotiationVerifier(
                      "c001", u -> new ProofBindings.TrustedHome("c001", 1, 1)))
              .businessAdmission(() -> true);
      if (reconnect) {
        var original = SessionAuthReadIT.route(f, caller);
        var principal = SessionAuthReadIT.principal(original);
        var newer =
            f.sessions
                .registerSession(principal, f.boot, UUID.randomUUID(), 1)
                .toCompletableFuture()
                .join();
        var rebound =
            new AuthenticatedSession(
                newer.user(),
                newer.key(),
                newer.incarnation(),
                newer.connectionGeneration(),
                newer.connectionId());
        var resume =
            new CallCommand(
                SignalEnvelope.Type.RESUME,
                rebound,
                new RequestId(UUID.randomUUID()),
                call,
                CommandScope.call(call),
                null,
                null,
                null,
                "{}",
                "e".repeat(64));
        var nativeCurrent =
            NativeProofSagaIT.done(
                new SessionRegistryService(f.runtime.sql, "c001", 1, (c, p) -> true)
                    .readCurrentSessionTracked(newer, principal, 1, Duration.ofSeconds(2)));
        var resumed =
            NativeProofSagaIT.done(
                commands.executeUnderAuthorityTracked(
                    resume,
                    new CallCommandService.Authority(
                        call, group, 1, proofs.sessionProofs().issue(nativeCurrent, resume), 1),
                    Duration.ofSeconds(2)));
        assertThat(resumed.code()).isEqualTo("RESUMED");
        assertThat(resumed.state()).isEqualTo("RINGING");
        assertThat(resumed.version()).isEqualTo(2);
      }
      var home =
          new HomeParticipationService(f.runtime.sql, "c001", 1, bindings.homeVerifier("c001"));
      var users =
          new PostgresUserBackend(
              new UserSnapshotService(f.runtime.sql, "c001", 1),
              f.sessions,
              new UserReservationService(home),
              new AcceptWinnerService(home, (c, r) -> true),
              new HomeActivationService(home, (c, u) -> true),
              new HomeProofReadService(home, (c, u) -> true, (c, r) -> true));
      var grants = new CoordinatorGrantService(f.runtime.sql, "c001", 1, "TEST_ONLY_LOCAL_OWNER");
      var workflow =
          new CallWorkflowService(
              f.runtime.sql,
              "c001",
              1,
              "TEST_ONLY_LOCAL_OWNER",
              bindings.workflowVerifier("c001", u -> new ProofBindings.TrustedHome("c001", 1, 1)));
      var current =
          new AtomicReference<>(
              NativeProofSagaIT.done(workflow.load(call, group, Duration.ofSeconds(2)))
                  .orElseThrow());
      var actors =
          new RpcBusinessHandler.ActorIngress() {
            public CompletionStage<CallActor.GrantReply> grant(
                HomeParticipationService.Request r,
                HomeParticipationService.AuthorizationIntent a,
                String d,
                Instant until,
                int bytes) {
              return grantTracked(r, a, d, until, bytes).logical();
            }

            public RpcOperation<CallActor.GrantReply> grantTracked(
                HomeParticipationService.Request r,
                HomeParticipationService.AuthorizationIntent a,
                String d,
                Instant until,
                int bytes) {
              var work =
                  grants.issue(
                      r,
                      a,
                      group,
                      1,
                      current.get().version(),
                      Duration.between(clock.instant(), until));
              return new RpcOperation<>(
                  work.logical()
                      .thenApply(
                          v -> new CallActor.GrantReply("GRANTED", v, issuer.coordinator(r, d, v))),
                  work.physicalCompletion());
            }

            public CompletionStage<UserCommand.Result> user(
                UserCommand.Operation o, Instant until, int bytes) {
              return userTracked(o, until, bytes).logical();
            }

            public RpcOperation<UserCommand.Result> userTracked(
                UserCommand.Operation o, Instant until, int bytes) {
              var work = users.execute(o, Duration.between(clock.instant(), until));
              return new RpcOperation<>(work.logical(), work.physicalCompletion());
            }

            public CompletionStage<CallCommandService.Outcome> call(
                CallCommand c, String p, Instant until, int bytes) {
              return callTracked(c, p, until, bytes).logical();
            }

            public RpcOperation<CallCommandService.Outcome> callTracked(
                CallCommand c, String p, Instant until, int bytes) {
              var work =
                  commands.executeUnderAuthorityTracked(
                      c,
                      new CallCommandService.Authority(call, group, 1, p, current.get().version()),
                      Duration.between(clock.instant(), until));
              return new RpcOperation<>(work.logical(), work.physicalCompletion());
            }

            public CompletionStage<CallWorkflowService.Outcome> progress(
                CallWorkflowService.Transition t, Instant until, int bytes) {
              return progressTracked(t, until, bytes).logical();
            }

            public RpcOperation<CallWorkflowService.Outcome> progressTracked(
                CallWorkflowService.Transition t, Instant until, int bytes) {
              var work = workflow.apply(t, Duration.between(clock.instant(), until));
              return new RpcOperation<>(
                  work.logical()
                      .thenApply(
                          v -> {
                            current.set(v.snapshot());
                            return v;
                          }),
                  work.physicalCompletion());
            }
          };
      var bridge =
          new RpcBusinessHandler(
                  "c001",
                  actors,
                  bindings,
                  proofs,
                  u -> new ProofBindings.TrustedHome("c001", 1, 1),
                  r -> CompletableFuture.failedFuture(new AssertionError()),
                  r -> CompletableFuture.failedFuture(new AssertionError()),
                  clock,
                  issuer)
              .businessAdmission(() -> true);
      var nativeRoundHomeReads = new AtomicInteger();
      var network =
          new NativeSagaEffects.Network() {
            public CompletionStage<io.webrtc.signaling.protocol.internal.InternalReply> call(
                CellRpcServer.Operation op,
                io.webrtc.signaling.protocol.internal.InternalCommand c,
                Duration b) {
              return callTracked(op, c, b).logical();
            }

            public RpcOperation<io.webrtc.signaling.protocol.internal.InternalReply> callTracked(
                CellRpcServer.Operation op,
                io.webrtc.signaling.protocol.internal.InternalCommand c,
                Duration b) {
              if (c.getType().equals("QueryParticipation")) nativeRoundHomeReads.incrementAndGet();
              return bridge.executeTracked(op, c, new CellRpcServer.Peer("c001", "actor"), b);
            }
          };
      var executor =
          new NativeSetupCommandExecutor(
              commands,
              new NativeHomeProofClient(actors, network, clock),
              actors,
              network,
              u -> new ProofBindings.TrustedHome("c001", 1, 1),
              "c001",
              1,
              clock);
      var accept = AcceptCompletionIT.accept(callee, call);
      var route = SessionAuthReadIT.route(f, callee);
      var nativeSession =
          NativeProofSagaIT.done(
              new SessionRegistryService(f.runtime.sql, "c001", 1, (c, p) -> true)
                  .readCurrentSessionTracked(
                      route, SessionAuthReadIT.principal(route), 1, Duration.ofSeconds(2)));
      var signed = proofs.sessionProofs().issue(nativeSession, accept);
      bridge.nativeSetup(executor);
      java.util.function.Function<UUID, com.google.protobuf.ByteString> uuid =
          id ->
              com.google.protobuf.ByteString.copyFrom(
                  java.nio.ByteBuffer.allocate(16)
                      .putLong(id.getMostSignificantBits())
                      .putLong(id.getLeastSignificantBits())
                      .array());
      var wire =
          io.webrtc.signaling.protocol.internal.InternalCommand.newBuilder()
              .setSchemaMajor(1)
              .setDestinationCell("c001")
              .setType("ACCEPT")
              .setOperationId(accept.requestId().value().toString())
              .setCallId(call.value())
              .setCommandScope(accept.scope().value())
              .setPayloadHash(
                  com.google.protobuf.ByteString.copyFrom(
                      HexFormat.of().parseHex(accept.intentHash())))
              .setRemainingBudgetMs(2000)
              .setPayload(
                  com.google.protobuf.ByteString.copyFrom(
                      RpcBusinessHandler.encode(
                          new RpcBusinessHandler.CallPayload(accept, signed))))
              .setSender(
                  io.webrtc.signaling.protocol.internal.SessionIdentity.newBuilder()
                      .setUserId(callee.userId().value())
                      .setIssuer(callee.key().issuer())
                      .setJti(callee.key().jti())
                      .setIncarnation(uuid.apply(callee.incarnation().value()))
                      .setConnectionGeneration(callee.connectionGeneration())
                      .setConnectionId(uuid.apply(callee.connectionId())))
              .build();
      var work =
          bridge.executeTracked(
              CellRpcServer.Operation.EXECUTE,
              wire,
              new CellRpcServer.Peer("c001", "actor"),
              Duration.ofSeconds(2));
      var reply = work.logical().toCompletableFuture().get(3, TimeUnit.SECONDS);
      work.physicalCompletion().toCompletableFuture().get(3, TimeUnit.SECONDS);
      assertThat(reply.getAckCommitted()).isTrue();
      var result =
          new com.fasterxml.jackson.databind.ObjectMapper()
              .readValue(reply.getResult().toByteArray(), CallCommandService.Outcome.class);

      assertThat(result.status()).isEqualTo("FINAL");
      assertThat(result.code()).isEqualTo("ACCEPTED_PENDING_ACTIVATION");
      assertThat(current.get().state()).isEqualTo("CONNECTING");
      assertThat(current.get().version()).isEqualTo(reconnect ? 5 : 4);
      var retry = executor.execute(accept, signed, Duration.ofSeconds(2));
      assertThat(retry.logical().toCompletableFuture().get(3, TimeUnit.SECONDS)).isEqualTo(result);
      retry.physicalCompletion().toCompletableFuture().get(3, TimeUnit.SECONDS);
      assertThat(current.get().version()).isEqualTo(reconnect ? 5 : 4);
      if (relayCheck) {
        var nativeCaller =
            NativeProofSagaIT.done(
                new SessionRegistryService(f.runtime.sql, "c001", 1, (c, p) -> true)
                    .readCurrentSessionTracked(
                        SessionAuthReadIT.route(f, caller),
                        SessionAuthReadIT.principal(SessionAuthReadIT.route(f, caller)),
                        1,
                        Duration.ofSeconds(2)));
        var negotiate =
            new CallCommand(
                SignalEnvelope.Type.NEGOTIATE_REQUEST,
                caller,
                new RequestId(UUID.randomUUID()),
                call,
                CommandScope.call(call),
                null,
                null,
                null,
                "{}",
                "a".repeat(64));
        var critical =
            new NativeCriticalCommandExecutor(
                commands,
                new NativeHomeProofClient(actors, network, clock),
                actors,
                u -> new ProofBindings.TrustedHome("c001", 1, 1),
                "c001",
                1,
                clock);
        var negotiation =
            critical.execute(
                negotiate,
                proofs.sessionProofs().issue(nativeCaller, negotiate),
                Duration.ofSeconds(2));
        assertThat(negotiation.logical().toCompletableFuture().get(3, TimeUnit.SECONDS).code())
            .isEqualTo("NEGOTIATION_GRANTED");
        negotiation.physicalCompletion().toCompletableFuture().get(3, TimeUnit.SECONDS);
        current.set(
            NativeProofSagaIT.done(workflow.load(call, group, Duration.ofSeconds(2)))
                .orElseThrow());
        var owned = new AtomicBoolean(true);
        var trusted = new AtomicBoolean(true);
        var authority =
            new NativeRelayAuthorization(
                commands,
                new NativeHomeProofClient(actors, network, clock),
                proofs,
                u -> new ProofBindings.TrustedHome("c001", 1, 1),
                c -> owned.get() ? Optional.of(group) : Optional.empty(),
                clock,
                trusted::get);
        var offer =
            new CallCommand(
                SignalEnvelope.Type.OFFER,
                caller,
                new RequestId(UUID.randomUUID()),
                call,
                CommandScope.call(call),
                null,
                new NegotiationId(1),
                new IceGeneration(1),
                "{\"sdp\":\"v=0\\r\\n\"}",
                "b".repeat(64));
        var offerProof = proofs.relaySessionProofs().issue(nativeCaller, offer);
        nativeRoundHomeReads.set(0);
        var roundCache =
            new NativeRelayRoundCache(
                4,
                2,
                authority,
                System::nanoTime,
                trusted::get,
                c -> owned.get() ? Optional.of(group) : Optional.empty());
        var observed = roundCache.load(offer, offerProof, Duration.ofSeconds(2));
        var authorizedRound = observed.logical().toCompletableFuture().get(3, TimeUnit.SECONDS);
        var snapshot = authorizedRound.authorization();
        var committedGrant = authorizedRound.grant();
        var destination = authorizedRound.destination();
        assertThat(destination.cell()).isEqualTo("c001");
        assertThat(destination.gatewayId()).isEqualTo(route.gatewayId());
        assertThat(destination.bootId()).isEqualTo(route.bootId());
        assertThat(destination.recipient()).isEqualTo(callee);
        assertThat(authorizedRound.authorizationUntil())
            .isAfter(Instant.now().plusMillis(255))
            .isBeforeOrEqualTo(Instant.now().plusSeconds(5));
        assertThat(committedGrant.call()).isEqualTo(call);
        assertThat(committedGrant.activationId()).isEqualTo(snapshot.activationId());
        assertThat(committedGrant.callVersion()).isEqualTo(snapshot.callVersion());
        assertThat(committedGrant.offerer()).isEqualTo(caller);
        assertThat(committedGrant.answerer()).isEqualTo(callee);
        assertThat(committedGrant.group()).isEqualTo(group);
        assertThat(committedGrant.negotiationId()).isEqualTo(1);
        assertThat(committedGrant.iceGeneration()).isEqualTo(1);
        assertThat(committedGrant.untilNanos() - snapshot.checkedAtNanos())
            .isPositive()
            .isLessThanOrEqualTo(Duration.ofSeconds(20).toNanos());
        observed.physicalCompletion().toCompletableFuture().get(3, TimeUnit.SECONDS);
        var retryOffer =
            new CallCommand(
                offer.type(),
                offer.sender(),
                new RequestId(UUID.randomUUID()),
                offer.callId(),
                offer.scope(),
                null,
                offer.negotiationId(),
                offer.iceGeneration(),
                offer.payloadJson(),
                offer.intentHash());
        var cached = roundCache.load(retryOffer, offerProof, Duration.ofSeconds(1));
        assertThat(cached.logical().toCompletableFuture().get(2, TimeUnit.SECONDS))
            .isSameAs(authorizedRound);
        cached.physicalCompletion().toCompletableFuture().get(2, TimeUnit.SECONDS);
        assertThat(nativeRoundHomeReads).hasValue(2);
        roundCache.drain().toCompletableFuture().get(2, TimeUnit.SECONDS);
        assertThat(snapshot.callId()).isEqualTo(call);
        assertThat(snapshot.activationId()).isEqualTo(current.get().activationId());
        assertThat(snapshot.negotiationId()).isEqualTo(1);
        assertThat(snapshot.iceGeneration()).isEqualTo(1);
        assertThat(snapshot.sender()).isEqualTo(caller);
        assertThat(snapshot.recipient()).isEqualTo(callee);
        assertThat(snapshot.group()).isEqualTo(group);
        assertThat(snapshot.securityUntilNanos() - snapshot.checkedAtNanos())
            .isLessThanOrEqualTo(Duration.ofSeconds(5).toNanos());
        var stale =
            new CallCommand(
                SignalEnvelope.Type.OFFER,
                caller,
                new RequestId(UUID.randomUUID()),
                call,
                CommandScope.call(call),
                null,
                new NegotiationId(2),
                new IceGeneration(1),
                "{}",
                "c".repeat(64));
        assertThatThrownBy(
                () ->
                    authority
                        .load(
                            stale,
                            proofs.sessionProofs().issue(nativeCaller, stale),
                            Duration.ofSeconds(2))
                        .logical()
                        .toCompletableFuture()
                        .join())
            .hasRootCauseInstanceOf(CallCommandService.AuthorizationRejected.class);
        assertThatThrownBy(
                () ->
                    authority
                        .load(offer, "forged", Duration.ofSeconds(2))
                        .logical()
                        .toCompletableFuture()
                        .join())
            .hasRootCauseInstanceOf(CallCommandService.AuthorizationRejected.class);
        owned.set(false);
        assertThatThrownBy(
                () ->
                    authority
                        .load(offer, offerProof, Duration.ofSeconds(2))
                        .logical()
                        .toCompletableFuture()
                        .join())
            .hasRootCauseInstanceOf(AuthoritySql.FencedException.class);
        owned.set(true);
        var wrongOfferer =
            new CallCommand(
                SignalEnvelope.Type.OFFER,
                callee,
                new RequestId(UUID.randomUUID()),
                call,
                CommandScope.call(call),
                null,
                new NegotiationId(1),
                new IceGeneration(1),
                "{}",
                "d".repeat(64));
        var calleeProof = proofs.sessionProofs().issue(nativeSession, wrongOfferer);
        assertThatThrownBy(
                () ->
                    authority
                        .load(wrongOfferer, calleeProof, Duration.ofSeconds(2))
                        .logical()
                        .toCompletableFuture()
                        .join())
            .hasRootCauseInstanceOf(CallCommandService.AuthorizationRejected.class);
        trusted.set(false);
        assertThatThrownBy(
                () ->
                    authority
                        .load(offer, offerProof, Duration.ofSeconds(2))
                        .logical()
                        .toCompletableFuture()
                        .join())
            .hasRootCauseInstanceOf(AuthoritySql.FencedException.class);
        trusted.set(true);
        f.sessions
            .registerSession(SessionAuthReadIT.principal(route), f.boot, UUID.randomUUID(), 1)
            .toCompletableFuture()
            .join();
        assertThatThrownBy(
                () ->
                    authority
                        .load(offer, offerProof, Duration.ofSeconds(2))
                        .logical()
                        .toCompletableFuture()
                        .join())
            .hasRootCauseInstanceOf(AuthoritySql.FencedException.class);
      }
      var losingSession = f.sender("setup-callee");
      var read =
          new CallCommand(
              SignalEnvelope.Type.SYNC_CALL,
              losingSession,
              new RequestId(UUID.randomUUID()),
              call,
              CommandScope.call(call),
              null,
              null,
              null,
              "{}",
              "c".repeat(64));
      var losingRoute = SessionAuthReadIT.route(f, losingSession);
      var losingCurrent =
          NativeProofSagaIT.done(
              new SessionRegistryService(f.runtime.sql, "c001", 1, (c, p) -> true)
                  .readCurrentSessionTracked(
                      losingRoute,
                      SessionAuthReadIT.principal(losingRoute),
                      1,
                      Duration.ofSeconds(2)));
      var losingProof = proofs.sessionProofs().issue(losingCurrent, read);
      assertThatThrownBy(
              () ->
                  commands
                      .loadCallSnapshotAuthorized(read, losingProof, Duration.ofSeconds(2))
                      .logical()
                      .toCompletableFuture()
                      .join())
          .hasCauseInstanceOf(CallCommandService.AuthorizationRejected.class);
      var winningRead =
          new CallCommand(
              SignalEnvelope.Type.SYNC_CALL,
              callee,
              new RequestId(UUID.randomUUID()),
              call,
              CommandScope.call(call),
              null,
              null,
              null,
              "{}",
              "d".repeat(64));
      var winningProof = proofs.sessionProofs().issue(nativeSession, winningRead);
      assertThat(
              NativeProofSagaIT.done(
                      commands.loadCallSnapshotAuthorized(
                          winningRead, winningProof, Duration.ofSeconds(2)))
                  .winner()
                  .key())
          .isEqualTo(callee.key());
    }
  }
}
