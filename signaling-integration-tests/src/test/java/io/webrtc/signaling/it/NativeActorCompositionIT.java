package io.webrtc.signaling.storage;

import static org.assertj.core.api.Assertions.*;

import com.typesafe.config.*;
import io.webrtc.signaling.actors.cluster.*;
import io.webrtc.signaling.actors.lease.*;
import io.webrtc.signaling.actors.relay.RelayBufferBudget;
import io.webrtc.signaling.auth.*;
import io.webrtc.signaling.gateway.*;
import io.webrtc.signaling.protocol.*;
import io.webrtc.signaling.protocol.Identity.*;
import io.webrtc.signaling.rpc.*;
import java.security.*;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import org.apache.pekko.actor.typed.*;
import org.apache.pekko.actor.typed.javadsl.*;
import org.apache.pekko.cluster.MemberStatus;
import org.apache.pekko.cluster.typed.*;
import org.junit.jupiter.api.Test;

/**
 * Actual native backends and EntityRefs across four test-only loopback TCP members; external TLS/AZ
 * admission remains separate.
 */
class NativeActorCompositionIT {
  static Config config(String zone) {
    return ConfigFactory.parseString(
            """
        signaling.cell-id="c001"
        signaling.cluster-fingerprint="aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        pekko.remote.artery.transport=tcp
        pekko.remote.artery.canonical.hostname="127.0.0.1"
        pekko.remote.artery.canonical.port=0
        pekko.management.http.hostname="127.0.0.1"
        pekko.discovery.kubernetes-api.pod-namespace="test-only-c001"
        pekko.discovery.kubernetes-api.pod-label-selector="app=webrtc-signaling,plane=actor,cell=c001"
        pekko.management.cluster.bootstrap.contact-point-discovery.service-name="signaling-c001"
        pekko.cluster.roles=["signaling-actor","az-%s"]
        pekko.loglevel=WARNING
        pekko.cluster.jmx.multi-mbeans-in-same-jvm=on
        """
                .formatted(zone))
        .withFallback(ShardingBootstrap.baseConfig())
        .resolve();
  }

  @Test
  void realEntityRefsSelectNativeRootsAndCommitThroughInstalledBackendFactories() throws Exception {
    var systems = new ArrayList<ActorSystem<Void>>();
    var compositions = new ArrayList<NativeActorComposition>();
    var f = new LocalInviteAtomicIT.Fixture();
    try (var recipientGateway = new RelayGatewayFixture(f);
        var verifier =
            new BoundedTokenVerifier(
                (token, now) -> {
                  throw new IllegalArgumentException("TEST_ONLY_UNUSED_AUTH");
                },
                1,
                8,
                Duration.ofSeconds(1))) {
      var caller = recipientGateway.register("composition-caller");
      var callee = recipientGateway.register("composition-callee");
      var key = KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
      var proofs =
          new HomeAuthorizationProof(
              "c001", "test", key.getPrivate(), Map.of("c001/test", key.getPublic()));
      for (var zone : List.of("a", "a", "b", "c")) {
        var system =
            ActorSystem.<Void>create(Behaviors.empty(), "native-composition-c001", config(zone));
        systems.add(system);
        var readiness = new ClusterReadiness();
        readiness.update(
            new ClusterReadiness.Snapshot(false, false, true, true, true, true, 0, 0, false));
        var inputs =
            new NativeActorComposition.Inputs(
                f.runtime.sql,
                "c001",
                1,
                1,
                UUID.randomUUID(),
                proofs,
                u -> new ProofBindings.TrustedHome("c001", 1, 1),
                (c, p) -> true,
                (c, u) -> true,
                (c, from, to) -> false,
                verifier,
                CallAuthorizationPolicy.denyAll(),
                Clock.systemUTC(),
                () -> true,
                (c, r) -> true);
        compositions.add(new NativeActorComposition(system, inputs, readiness));
      }
      for (var composition : compositions) {
        var membership =
            new NativeClusterMembership(
                composition.system(), composition.readiness(), Set.of("az-a", "az-b", "az-c"));
        membership.refresh();
        assertThat(composition.readiness().snapshot().localUp()).isFalse();
        assertThat(composition.readiness().businessReady()).isFalse();
      }
      var seed = Cluster.get(systems.getFirst()).selfMember().address();
      for (var system : systems)
        Cluster.get(system).manager().tell(new JoinSeedNodes(List.of(seed)));
      org.awaitility.Awaitility.await()
          .atMost(Duration.ofSeconds(25))
          .until(
              () ->
                  systems.stream()
                      .allMatch(
                          s -> Cluster.get(s).selfMember().status().equals(MemberStatus.up())));
      for (var composition : compositions) {
        var membership =
            new NativeClusterMembership(
                composition.system(), composition.readiness(), Set.of("az-a", "az-b", "az-c"));
        org.awaitility.Awaitility.await()
            .atMost(Duration.ofSeconds(10))
            .until(
                () -> {
                  membership.refresh();
                  return composition.readiness().snapshot().upActors() == 4
                      && composition.readiness().snapshot().reachableAzCount() == 3;
                });
        assertThat(composition.readiness().businessReady()).isFalse();
        composition.register();
        assertThat(composition.readiness().businessReady()).isTrue();
        var incompleteEnrollment =
            new NativeClusterMembership(
                composition.system(), composition.readiness(), Set.of("az-a"));
        incompleteEnrollment.refresh();
        assertThat(composition.readiness().snapshot().upActors()).isEqualTo(2);
        assertThat(composition.readiness().snapshot().reachableAzCount()).isEqualTo(1);
        assertThat(composition.readiness().businessReady()).isFalse();
        membership.refresh();
        assertThat(composition.readiness().businessReady()).isTrue();
      }
      var cell = compositions.getLast();
      var call = CallId.create("c001", 1);
      var original = f.invite(caller, callee.userId());
      var command =
          new CallCommand(
              original.type(),
              caller,
              original.requestId(),
              call,
              original.scope(),
              callee.userId(),
              null,
              null,
              "{}",
              original.intentHash());
      var route = SessionAuthReadIT.route(f, caller);
      var registry = new SessionRegistryService(f.runtime.sql, "c001", 1, (c, p) -> true);
      var auth = currentSession(registry, route);
      var signed = proofs.sessionProofs().issue(auth, command);
      CallCommandService.Outcome result = null;
      long end = System.nanoTime() + Duration.ofSeconds(15).toNanos();
      while (System.nanoTime() < end) {
        auth = currentSession(registry, route);
        signed = proofs.sessionProofs().issue(auth, command);
        var operation =
            cell.ingress()
                .callTracked(
                    command,
                    signed,
                    Instant.now().plusSeconds(2),
                    RpcBusinessHandler.encode(new RpcBusinessHandler.CallPayload(command, signed))
                        .length);
        try {
          result = operation.logical().toCompletableFuture().join();
        } catch (CompletionException unknown) {
          assertThat(unknown.getCause())
              .isInstanceOfAny(
                  org.apache.pekko.pattern.AskTimeoutException.class,
                  DbOutcomeUnknownException.class,
                  TimeoutException.class);
        } finally {
          operation.physicalCompletion().toCompletableFuture().get(8, TimeUnit.SECONDS);
        }
        if (result != null && result.status().equals("FINAL")) break;
        java.util.concurrent.locks.LockSupport.parkNanos(20_000_000);
      }
      assertThat(result).isNotNull();
      assertThat(result.status()).isEqualTo("FINAL");
      assertThat(result.state()).isEqualTo("RINGING");
      var holders =
          compositions.stream()
              .filter(
                  c ->
                      PostgresShardLeaseProvider.currentGrant(
                              Adapter.toClassic(c.system()), HomeParticipationService.group(call))
                          .isPresent())
              .toList();
      assertThat(holders).hasSize(1);
      var token =
          PostgresShardLeaseProvider.currentGrant(
                  Adapter.toClassic(holders.getFirst().system()),
                  HomeParticipationService.group(call))
              .orElseThrow()
              .token();
      assertThat(token.node()).contains("#");
      assertThat(token.incarnation()).isNotEqualTo(f.incarnation);
      var queryAction =
          new HomeParticipationService.AuthorizationIntent("QUERY", null, 0, null, 0, null, null);
      var now = Instant.now();
      var template =
          new HomeParticipationService.Request(
              caller.userId(),
              call,
              command.requestId().value(),
              command.intentHash(),
              1,
              HomeParticipationService.Phase.RINGING,
              new HomeParticipationService.Grant(
                  "c001",
                  1,
                  1,
                  token.group(),
                  token.epoch(),
                  1,
                  UUID.randomUUID(),
                  now,
                  now.plusSeconds(5),
                  "UNSIGNED"));
      var grant =
          cell.ingress()
              .grant(
                  template,
                  queryAction,
                  "c001",
                  Instant.now().plusSeconds(2),
                  RpcBusinessHandler.encode(template).length)
              .toCompletableFuture()
              .join();
      assertThat(grant.code()).isEqualTo("GRANTED");
      assertThat(
              new ProofBindings(proofs, Clock.systemUTC())
                  .homeVerifier("c001")
                  .verify(grant.signedRequest(), queryAction))
          .isTrue();
      var host = holders.getFirst();
      var target = new java.util.concurrent.atomic.AtomicReference<RpcBusinessHandler>();
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
              assertThat(c.getPayload().size()).isLessThan(37000);
              return target
                  .get()
                  .executeTracked(
                      op, c, new CellRpcServer.Peer("c001", "actor", "TEST_ONLY_NATIVE_ACTOR"), b);
            }
          };
      var backend =
          host.backend(
              network,
              r ->
                  CompletableFuture.failedFuture(
                      new AssertionError("Legacy relay must stay unused")));
      target.set(backend);
      var winnerRoute = SessionAuthReadIT.route(f, callee);
      var winnerAuth = currentSession(registry, winnerRoute);
      var accept = AcceptCompletionIT.accept(callee, call);
      CallCommandService.Outcome setup = null;
      long acceptEnd = System.nanoTime() + Duration.ofSeconds(15).toNanos();
      for (int attempt = 0; attempt < 8 && System.nanoTime() < acceptEnd; attempt++) {
        winnerAuth = currentSession(registry, winnerRoute);
        try {
          setup = publicCommand(backend, accept, proofs.sessionProofs().issue(winnerAuth, accept));
          break;
        } catch (ExecutionException unknown) {
          assertThat(unknown.getCause()).isInstanceOf(DbOutcomeUnknownException.class);
        }
      }
      assertThat(setup).describedAs("Same-operation ACCEPT reconciliation").isNotNull();
      assertThat(setup.code()).isEqualTo("ACCEPTED_PENDING_ACTIVATION");
      auth = currentSession(registry, route);
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
              "b".repeat(64));
      var critical =
          publicCommand(backend, negotiate, proofs.sessionProofs().issue(auth, negotiate));
      assertThat(critical.code()).isEqualTo("NEGOTIATION_GRANTED");
      auth = currentSession(registry, route);
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
              "{\"sdp\":\"" + "x".repeat(65536) + "\"}",
              "c".repeat(64));
      var relay =
          host.relayAuthorization(network)
              .load(offer, proofs.relaySessionProofs().issue(auth, offer), Duration.ofSeconds(2));
      var authorized = relay.logical().toCompletableFuture().get(3, TimeUnit.SECONDS);
      relay.physicalCompletion().toCompletableFuture().get(5, TimeUnit.SECONDS);
      assertThat(authorized.sender()).isEqualTo(caller);
      assertThat(authorized.recipient()).isEqualTo(callee);
      assertThat(authorized.group()).isEqualTo(token);
      assertThat(authorized.callVersion()).isEqualTo(5);
      assertThat(authorized.negotiationId()).isEqualTo(1);
      var nonhost = compositions.stream().filter(c -> c != host).findFirst().orElseThrow();
      String offerProof = proofs.relaySessionProofs().issue(auth, offer);
      assertThatThrownBy(
              () ->
                  nonhost
                      .relayAuthorization(network)
                      .load(offer, offerProof, Duration.ofSeconds(2))
                      .logical()
                      .toCompletableFuture()
                      .join())
          .hasRootCauseInstanceOf(AuthoritySql.FencedException.class);

      host.readiness().beginDrain();
      assertThat(host.readiness().businessReady()).isFalse();
      var existing =
          host.relayAuthorization(network).load(offer, offerProof, Duration.ofSeconds(2));
      assertThat(existing.logical().toCompletableFuture().get(3, TimeUnit.SECONDS).recipient())
          .isEqualTo(callee);
      existing.physicalCompletion().toCompletableFuture().get(5, TimeUnit.SECONDS);
      // Joined TEST_ONLY native PostgreSQL + EntityRef authority + actual TLS RPC + Netty write.
      try (var gatewayClient = recipientGateway.client();
          var ingress =
              mainIngress(
                  host,
                  new io.webrtc.signaling.app.runtime.NativeActorIngressEnrollment(
                      "test",
                      0,
                      RpcTlsContexts.server(
                          "test",
                          "c001",
                          RelayGatewayFixture.cert("ca.crt"),
                          RelayGatewayFixture.cert("actor.crt"),
                          RelayGatewayFixture.cert("actor.key")),
                      new RpcAdmission(16, 1048576, 16, 1048576),
                      network,
                      4,
                      new RelayBufferBudget(262144),
                      gatewayClient,
                      (peer, gateway) -> peer.workloadId().equals(gateway.gatewayId()),
                      event ->
                          CompletableFuture.failedFuture(
                              new IllegalArgumentException("TEST_ONLY_NO_CONTROL_DELIVERY"))));
          var relayClient =
              new CellRpcClient(
                  "test",
                  Map.of(
                      "c001",
                      new CellRpcClient.Endpoint(
                          "localhost", ingress.server().port(), "localhost")),
                  RpcTlsContexts.clients(
                      "test",
                      RelayGatewayFixture.cert("ca.crt"),
                      RelayGatewayFixture.cert("gateway.crt"),
                      RelayGatewayFixture.cert("gateway.key")),
                  new RpcAdmission(16, 1048576, 16, 1048576))) {
        var protocol = new ProtocolValidator(ProtocolLimits.v1());
        var originalOffer = new java.util.concurrent.atomic.AtomicReference<CallCommand>();
        var originalOfferProof = new java.util.concurrent.atomic.AtomicReference<String>();
        for (var type :
            List.of(
                SignalEnvelope.Type.OFFER,
                SignalEnvelope.Type.ANSWER,
                SignalEnvelope.Type.ICE_CANDIDATES,
                SignalEnvelope.Type.END_OF_CANDIDATES)) {
          var sender = type == SignalEnvelope.Type.ANSWER ? callee : caller;
          var destination = sender.equals(caller) ? callee : caller;
          String payload =
              switch (type) {
                case OFFER, ANSWER -> "{\"sdp\":\"v=0\\r\\n" + "x".repeat(16384) + "\"}";
                case ICE_CANDIDATES ->
                    "{\"startSequence\":\"1\",\"candidates\":[{\"candidate\":\"candidate:1 1 UDP 1 127.0.0.1 9 typ host\",\"sdpMid\":\"0\",\"usernameFragment\":\"TEST_ONLY_UFRAG\"}]}";
                case END_OF_CANDIDATES -> "{\"terminalSequence\":\"2\"}";
                default -> throw new AssertionError();
              };
          var originalRelay =
              protocol.bind(
                  new SignalEnvelope(
                      1,
                      type,
                      new RequestId(UUID.randomUUID()),
                      call,
                      new NegotiationId(1),
                      new IceGeneration(1),
                      payload),
                  sender);
          var nativeSender = currentSession(registry, SessionAuthReadIT.route(f, sender));
          var r1 = proofs.relaySessionProofs().issue(nativeSender, originalRelay);
          if (type == SignalEnvelope.Type.OFFER) {
            originalOffer.set(originalRelay);
            originalOfferProof.set(r1);
          }
          var senderRoute = SessionAuthReadIT.route(f, sender);
          var receipt = publicRelay(relayClient, originalRelay, proofs, registry, senderRoute);
          assertThat(receipt.matches(originalRelay)).isTrue();
          assertThat(receipt.callVersion()).isEqualTo(5);
          var delivered = recipientGateway.take(destination);
          assertThat(delivered.path("type").asText()).isEqualTo(type.name());
          assertThat(delivered.path("payload"))
              .isEqualTo(new com.fasterxml.jackson.databind.ObjectMapper().readTree(payload));
          assertThat(delivered.path("sessionIncarnation").asText())
              .isEqualTo(destination.incarnation().value().toString());
          if (type == SignalEnvelope.Type.OFFER) {
            assertThat(publicRelay(relayClient, originalRelay, proofs, registry, senderRoute))
                .isEqualTo(receipt);
            assertThat(recipientGateway.empty(destination)).isTrue();
          }
        }
        assertThat(gatewayClient.channelCount()).isEqualTo(2);
        var terminate =
            new CallCommand(
                SignalEnvelope.Type.HANGUP,
                caller,
                new RequestId(UUID.randomUUID()),
                call,
                CommandScope.call(call),
                null,
                null,
                null,
                "{}",
                "d".repeat(64));
        var terminated =
            publicCommand(
                backend,
                terminate,
                proofs.sessionProofs().issue(currentSession(registry, route), terminate));
        assertThat(terminated.state()).isEqualTo("TERMINAL");
        var afterTerminal = relayReply(relayClient, originalOffer.get(), originalOfferProof.get());
        assertThat(afterTerminal.getStatus()).isEqualTo("REJECTED");
        assertThat(afterTerminal.getErrorCode()).isEqualTo("RESYNC_REQUIRED");
        assertThat(recipientGateway.empty(callee)).isTrue();
      }

      host.readiness().beginDrain();
      assertThat(host.readiness().businessReady()).isFalse();
      cell.ingress().settleAdmitted().toCompletableFuture().get(3, TimeUnit.SECONDS);
      cell.ingress().drain().toCompletableFuture().get(3, TimeUnit.SECONDS);
      var anotherIngress = new ShardedActorIngress(cell.system(), Clock.systemUTC());
      assertThatThrownBy(
              () ->
                  anotherIngress.grantTracked(
                      template, queryAction, "c001", Instant.now().plusSeconds(2), 100))
          .isInstanceOf(io.webrtc.signaling.actors.admission.EntityAdmission.Overloaded.class);
      cell.readiness().beginDrain();
      new NativeClusterMembership(cell.system(), cell.readiness(), Set.of("az-a", "az-b", "az-c"))
          .refresh();
      assertThat(cell.readiness().snapshot().draining()).isTrue();
      assertThat(cell.readiness().businessReady()).isFalse();

    } finally {
      for (var system : systems) system.terminate();
      for (var system : systems)
        system.getWhenTerminated().toCompletableFuture().get(25, TimeUnit.SECONDS);
      for (var composition : compositions)
        composition.drainRoots().toCompletableFuture().get(8, TimeUnit.SECONDS);
      f.close();
    }
  }

  private static NativeActorRpcIngress mainIngress(
      NativeActorComposition actors,
      io.webrtc.signaling.app.runtime.NativeActorIngressEnrollment enrollment)
      throws Exception {
    var defaults =
        new org.springframework.boot.env.YamlPropertySourceLoader()
            .load(
                "TEST_ONLY_defaults",
                new org.springframework.core.io.FileSystemResource(
                    "../config/production-defaults.yaml"));
    var installed = new java.util.concurrent.atomic.AtomicReference<NativeActorRpcIngress>();
    new org.springframework.boot.test.context.runner.ApplicationContextRunner()
        .withUserConfiguration(io.webrtc.signaling.app.SignalingApplication.class)
        .withBean(NativeActorComposition.class, () -> actors)
        .withBean(
            io.webrtc.signaling.app.runtime.NativeActorIngressEnrollment.class, () -> enrollment)
        .withInitializer(
            context -> {
              defaults.forEach(
                  value -> context.getEnvironment().getPropertySources().addLast(value));
              context.getEnvironment().setActiveProfiles("actor");
            })
        .withPropertyValues(
            "signaling.identity.issuer=TEST_ONLY_ISSUER",
            "signaling.identity.audience=TEST_ONLY_AUDIENCE")
        .run(
            context -> {
              assertThat(context).hasNotFailed().hasSingleBean(NativeActorRpcIngress.class);
              installed.set(context.getBean(NativeActorRpcIngress.class));
              assertThat(installed.get().server().port()).isPositive();
            });
    return installed.get();
  }

  /**
   * TEST_ONLY boot/security adapters; native current sessions/routes and TLS/Netty writes are real.
   */
  static final class RelayGatewayFixture implements AutoCloseable {
    final LocalInviteAtomicIT.Fixture f;
    final GatewayLeaseRepository.Boot boot;
    final ConnectionRegistry connections;
    final Map<UserId, io.netty.channel.embedded.EmbeddedChannel> channels = new HashMap<>();
    final GatewayRelayRpcServer server;
    final ScheduledExecutorService renewal = Executors.newSingleThreadScheduledExecutor();
    final ScheduledFuture<?> renewalTask;

    RelayGatewayFixture(LocalInviteAtomicIT.Fixture f) throws Exception {
      this.f = f;
      boot =
          f.sessions
              .startGatewayBoot("gw-1", UUID.randomUUID(), "TEST_ONLY", UUID.randomUUID())
              .toCompletableFuture()
              .join();
      connections = new ConnectionRegistry("gw-1", boot.bootId(), 8, 16);
      var services =
          new GatewayServices() {
            public boolean currentBoot() {
              return true;
            }

            public AuthorizationStatus cachedSecurity(AuthPrincipal p, Instant now) {
              return AuthorizationStatus.ALLOWED;
            }

            public CompletionStage<AuthPrincipal> verify(String t, Instant n) {
              throw new AssertionError();
            }

            public CompletionStage<SessionRepository.Route> register(
                AuthPrincipal p, UUID c, Duration b) {
              throw new AssertionError();
            }

            public CompletionStage<SessionRepository.Route> refresh(
                SessionRepository.Route r, AuthPrincipal p, Duration b) {
              throw new AssertionError();
            }

            public CompletionStage<Void> close(SessionRepository.Route r) {
              throw new AssertionError();
            }

            public CompletionStage<String> command(CallCommand c, Duration b) {
              throw new AssertionError();
            }
          };
      var stream =
          new GatewayRelayStream(
              connections,
              services,
              Clock.systemUTC(),
              Runnable::run,
              new RpcAdmission(8, 1048576, 8, 1048576),
              () -> true);
      server =
          new GatewayRelayRpcServer(
                  "c001",
                  "gw-1",
                  boot.bootId(),
                  "test",
                  0,
                  RpcTlsContexts.gatewayServer(
                      "test",
                      "c001",
                      "gw-1",
                      cert("ca.crt"),
                      cert("gateway.crt"),
                      cert("gateway.key")),
                  new RpcAdmission(8, 1048576, 8, 1048576),
                  stream::send)
              .start();
      var currentBoot = new java.util.concurrent.atomic.AtomicReference<>(boot);
      renewalTask =
          renewal.scheduleWithFixedDelay(
              () -> {
                var current = currentBoot.get();
                var work =
                    f.sessions.renewGatewayBootTracked(
                        current.gatewayId(),
                        current.bootId(),
                        current.renewalSequence() + 1,
                        UUID.randomUUID(),
                        Duration.ofSeconds(2));
                try {
                  currentBoot.set(work.logical().toCompletableFuture().join().boot());
                } finally {
                  work.physicalCompletion().toCompletableFuture().join();
                }
              },
              1,
              1,
              TimeUnit.SECONDS);
    }

    static java.io.File cert(String name) {
      return new java.io.File(
          Objects.requireNonNull(
                  NativeActorCompositionIT.class.getResource("/test-only-pki/relay/" + name))
              .getFile());
    }

    AuthenticatedSession register(String name) {
      var channel =
          new io.netty.channel.embedded.EmbeddedChannel(
              new OutboundAdmissionHandler(new DeliveryCreditController(64, 1048576, 8, 131072)));
      var id = connections.attach(channel);
      var principal =
          new AuthPrincipal(
              new UserId(name),
              new SessionKey("TEST_ONLY", UUID.randomUUID().toString()),
              Instant.now().truncatedTo(java.time.temporal.ChronoUnit.SECONDS).plusSeconds(600),
              Instant.now(),
              "TEST_ONLY",
              1);
      var route = f.sessions.registerSession(principal, boot, id, 1).toCompletableFuture().join();
      assertThat(connections.bind(id, route, principal)).isTrue();
      channels.put(route.user(), channel);
      return new AuthenticatedSession(
          route.user(),
          route.key(),
          route.incarnation(),
          route.connectionGeneration(),
          route.connectionId());
    }

    GatewayRelayRpcClient client() throws Exception {
      return new GatewayRelayRpcClient(
          "test",
          2,
          Map.of(
              new GatewayRelayRpcClient.Target("c001", "gw-1", boot.bootId()),
              new CellRpcClient.Endpoint("localhost", server.port(), "localhost")),
          RpcTlsContexts.gatewayClients(
              "test", cert("ca.crt"), cert("actor.crt"), cert("actor.key")),
          new RpcAdmission(8, 1048576, 8, 1048576));
    }

    com.fasterxml.jackson.databind.JsonNode take(AuthenticatedSession recipient) throws Exception {
      var frame =
          (io.netty.handler.codec.http.websocketx.TextWebSocketFrame)
              channels.get(recipient.userId()).readOutbound();
      assertThat(frame).isNotNull();
      try {
        return new com.fasterxml.jackson.databind.ObjectMapper().readTree(frame.text());
      } finally {
        frame.release();
      }
    }

    boolean empty(AuthenticatedSession recipient) {
      return channels.get(recipient.userId()).outboundMessages().isEmpty();
    }

    public void close() throws Exception {
      renewal.shutdown();
      try {
        assertThat(renewal.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
        if (renewalTask.isDone() && !renewalTask.isCancelled()) renewalTask.get();
      } finally {
        renewal.shutdownNow();
        server.drain().toCompletableFuture().get(3, TimeUnit.SECONDS);
        channels.values().forEach(io.netty.channel.embedded.EmbeddedChannel::finishAndReleaseAll);
      }
    }
  }

  private static RelayWriteReceipt publicRelay(
      CellRpcClient client,
      CallCommand command,
      HomeAuthorizationProof proofs,
      SessionRegistryService registry,
      SessionRepository.Route route)
      throws Exception {
    io.webrtc.signaling.protocol.internal.InternalReply reply = null;
    long end = System.nanoTime() + Duration.ofSeconds(15).toNanos();
    for (int attempt = 0; attempt < 8 && System.nanoTime() < end; attempt++) {
      var proof = proofs.relaySessionProofs().issue(currentSession(registry, route), command);
      reply = relayReply(client, command, proof);
      if (!reply.getErrorCode().equals("OUTCOME_UNKNOWN")) break;
    }
    assertThat(reply).isNotNull();
    assertThat(reply.getAckCommitted()).isFalse();
    assertThat(reply.getStatus()).describedAs(reply.getErrorCode()).isEqualTo("WRITE_COMPLETED");
    return new com.fasterxml.jackson.databind.ObjectMapper()
        .findAndRegisterModules()
        .readValue(reply.getResult().toByteArray(), RelayWriteReceipt.class);
  }

  private static io.webrtc.signaling.protocol.internal.InternalReply relayReply(
      CellRpcClient client, CallCommand command, String proof) throws Exception {
    java.util.function.Function<UUID, com.google.protobuf.ByteString> uuid =
        id ->
            com.google.protobuf.ByteString.copyFrom(
                java.nio.ByteBuffer.allocate(16)
                    .putLong(id.getMostSignificantBits())
                    .putLong(id.getLeastSignificantBits())
                    .array());
    var sender = command.sender();
    var wire =
        io.webrtc.signaling.protocol.internal.InternalCommand.newBuilder()
            .setSchemaMajor(1)
            .setType(command.type().name())
            .setOperationId(command.requestId().value().toString())
            .setCallId(command.callId().value())
            .setCommandScope(command.scope().value())
            .setDestinationCell("c001")
            .setRemainingBudgetMs(1000)
            .setPayloadHash(
                com.google.protobuf.ByteString.copyFrom(
                    HexFormat.of().parseHex(command.intentHash())))
            .setPayload(
                com.google.protobuf.ByteString.copyFrom(
                    RpcBusinessHandler.encode(new RpcBusinessHandler.CallPayload(command, proof))))
            .setSender(
                io.webrtc.signaling.protocol.internal.SessionIdentity.newBuilder()
                    .setUserId(sender.userId().value())
                    .setIssuer(sender.key().issuer())
                    .setJti(sender.key().jti())
                    .setIncarnation(uuid.apply(sender.incarnation().value()))
                    .setConnectionGeneration(sender.connectionGeneration())
                    .setConnectionId(uuid.apply(sender.connectionId())))
            .build();
    var operation = client.callTracked(CellRpcServer.Operation.RELAY, wire, Duration.ofSeconds(1));
    try {
      return operation.logical().toCompletableFuture().get(2, TimeUnit.SECONDS);
    } finally {
      operation.physicalCompletion().toCompletableFuture().get(3, TimeUnit.SECONDS);
    }
  }

  /** Fixture eligibility read: known native contention retries within one original read budget. */
  private static SessionRegistryService.SessionProofView currentSession(
      SessionRegistryService registry, SessionRepository.Route route) throws Exception {
    long end = System.nanoTime() + Duration.ofSeconds(2).toNanos();
    for (; ; ) {
      long left = end - System.nanoTime();
      if (left <= 0) throw new TimeoutException("Native fixture session read budget");
      var work =
          registry.readCurrentSessionTracked(
              route, SessionAuthReadIT.principal(route), 1, Duration.ofNanos(left));
      try {
        return work.logical().toCompletableFuture().join();
      } catch (CompletionException failure) {
        Throwable cause = failure.getCause();
        while (cause instanceof CompletionException) cause = cause.getCause();
        if (!(cause instanceof AuthoritySql.RetryableConflict
            || cause instanceof DbOverloadedException)) throw failure;
      } finally {
        long cleanup = end - System.nanoTime();
        if (cleanup <= 0) throw new TimeoutException("Native fixture session cleanup budget");
        work.physicalCompletion().toCompletableFuture().get(cleanup, TimeUnit.NANOSECONDS);
      }
      java.util.concurrent.locks.LockSupport.parkNanos(
          Math.min(25_000_000, Math.max(0, end - System.nanoTime())));
    }
  }

  private static CallCommandService.Outcome publicCommand(
      RpcBusinessHandler backend, CallCommand command, String proof) throws Exception {
    java.util.function.Function<UUID, com.google.protobuf.ByteString> uuid =
        id ->
            com.google.protobuf.ByteString.copyFrom(
                java.nio.ByteBuffer.allocate(16)
                    .putLong(id.getMostSignificantBits())
                    .putLong(id.getLeastSignificantBits())
                    .array());
    var sender = command.sender();
    var wire =
        io.webrtc.signaling.protocol.internal.InternalCommand.newBuilder()
            .setSchemaMajor(1)
            .setType(command.type().name())
            .setOperationId(command.requestId().value().toString())
            .setCallId(command.callId().value())
            .setCommandScope(command.scope().value())
            .setDestinationCell("c001")
            .setRemainingBudgetMs(2000)
            .setPayloadHash(
                com.google.protobuf.ByteString.copyFrom(
                    HexFormat.of().parseHex(command.intentHash())))
            .setPayload(
                com.google.protobuf.ByteString.copyFrom(
                    RpcBusinessHandler.encode(new RpcBusinessHandler.CallPayload(command, proof))))
            .setSender(
                io.webrtc.signaling.protocol.internal.SessionIdentity.newBuilder()
                    .setUserId(sender.userId().value())
                    .setIssuer(sender.key().issuer())
                    .setJti(sender.key().jti())
                    .setIncarnation(uuid.apply(sender.incarnation().value()))
                    .setConnectionGeneration(sender.connectionGeneration())
                    .setConnectionId(uuid.apply(sender.connectionId())))
            .build();
    var op =
        backend.executeTracked(
            CellRpcServer.Operation.EXECUTE,
            wire,
            new CellRpcServer.Peer("c001", "actor"),
            Duration.ofSeconds(2));
    io.webrtc.signaling.protocol.internal.InternalReply reply;
    try {
      reply = op.logical().toCompletableFuture().get(3, TimeUnit.SECONDS);
    } finally {
      op.physicalCompletion().toCompletableFuture().get(5, TimeUnit.SECONDS);
    }
    assertThat(reply.getAckCommitted()).describedAs(reply.getErrorCode()).isTrue();
    return new com.fasterxml.jackson.databind.ObjectMapper()
        .readValue(reply.getResult().toByteArray(), CallCommandService.Outcome.class);
  }
}
