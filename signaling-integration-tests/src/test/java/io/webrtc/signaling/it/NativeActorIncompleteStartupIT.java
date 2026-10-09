package io.webrtc.signaling.storage;

import static org.assertj.core.api.Assertions.*;

import com.typesafe.config.*;
import io.webrtc.signaling.actors.cluster.*;
import io.webrtc.signaling.actors.relay.RelayBufferBudget;
import io.webrtc.signaling.app.*;
import io.webrtc.signaling.app.runtime.*;
import io.webrtc.signaling.auth.*;
import io.webrtc.signaling.rpc.*;
import io.webrtc.signaling.storage.worker.*;
import java.net.*;
import java.security.*;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.pekko.actor.typed.*;
import org.apache.pekko.actor.typed.javadsl.*;
import org.apache.pekko.cluster.MemberStatus;
import org.apache.pekko.cluster.typed.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.springframework.beans.factory.support.RootBeanDefinition;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.env.YamlPropertySourceLoader;
import org.springframework.core.io.FileSystemResource;

/** A bound native listener and sources cannot replace native process ownership. */
class NativeActorIncompleteStartupIT {
  @ParameterizedTest
  @CsvSource({"false,false", "true,false", "false,true"})
  void mainRequiresNativeShutdownOwnershipAndRetainsOriginalRpc(
      boolean heldRpc, boolean completeOwner) throws Exception {
    var runtime = new DbTestRuntime();
    var cryptoEntered = new CountDownLatch(1);
    var cryptoRelease = new CountDownLatch(1);
    try (var c = PgFixture.connection();
        var q = c.createStatement()) {
      q.execute(
          "INSERT INTO group_owner(cell_id,ownership_hash_version,group_id,storage_epoch,group_epoch,lease_sequence,status) SELECT 'c001',1,n,1,1,0,'IDLE' FROM generate_series(0,1023) n ON CONFLICT DO NOTHING");
    }
    var config =
        PekkoShutdownLifecycle.config(
            ConfigFactory.parseString(
                    """
            pekko.cluster.min-nr-of-members=1
            pekko.cluster.role.signaling-actor.min-nr-of-members=1
            pekko.coordinated-shutdown.run-by-jvm-shutdown-hook=off
            """)
                .withFallback(NativeActorCompositionIT.config("a")));
    var system = ActorSystem.<Void>create(Behaviors.empty(), "incomplete-startup-c001", config);
    var keys = KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
    var pod = UUID.randomUUID();
    var boot = UUID.randomUUID();
    var readiness = new ClusterReadiness();
    var monitor =
        new ClockSafetyMonitor(
            "c001",
            1,
            pod,
            boot,
            Map.of("TEST_ONLY", keys.getPublic()),
            Clock.systemUTC(),
            System::nanoTime);
    var clock =
        new NativeClockSource(
            URI.create("https://localhost:1/v1/clock"),
            NativeClockSourceIT.clientTls(true),
            monitor,
            pod,
            boot);
    var primary = new NativeCellHealthSource(new PrimaryCellFacts(runtime.sql, "c001", 1));
    var sourceVerifier =
        new RevocationSourceVerifier("c001", "TEST_ONLY", Map.of("TEST_ONLY", keys.getPublic()));
    var revocations =
        new NativeRevocationSource(
            URI.create("https://localhost:1/v1/revocations"),
            NativeClockSourceIT.clientTls(true),
            sourceVerifier,
            new RevocationReconciler(runtime.sql, "c001", 1, Duration.ofSeconds(1), sourceVerifier),
            pod,
            boot);
    var sources =
        new NativeActorSafetySources(
            system,
            readiness,
            Set.of("az-a"),
            "a".repeat(64),
            monitor,
            clock,
            primary,
            revocations);
    var workers = new NativeWorkerScheduler(sources.jobs(), e -> {});
    var tokens =
        new BoundedTokenVerifier(
            (token, now) -> {
              cryptoEntered.countDown();
              try {
                cryptoRelease.await();
              } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
              }
              throw new AuthException();
            },
            1,
            8,
            Duration.ofSeconds(1));
    var gateways =
        new GatewayRelayRpcClient(
            "test",
            1,
            Map.of(),
            RpcTlsContexts.gatewayClients(
                "test",
                NativeActorCompositionIT.RelayGatewayFixture.cert("ca.crt"),
                NativeActorCompositionIT.RelayGatewayFixture.cert("actor.crt"),
                NativeActorCompositionIT.RelayGatewayFixture.cert("actor.key")),
            new RpcAdmission(2, 196608, 2, 196608));
    var context = new AtomicReference<org.springframework.context.ConfigurableApplicationContext>();
    NativeActorRpcIngress ingress = null;
    PrivateHealthServer health = null;
    CellRpcClient caller = null;
    CellRpcClient processClient = null;
    try {
      var proofs =
          new HomeAuthorizationProof(
              "c001", "test", keys.getPrivate(), Map.of("c001/test", keys.getPublic()));
      var actors =
          new NativeActorComposition(
              system,
              new NativeActorComposition.Inputs(
                  runtime.sql,
                  "c001",
                  1,
                  1,
                  pod,
                  proofs,
                  u -> new ProofBindings.TrustedHome("c001", 1, 1),
                  (c, p) -> false,
                  (c, u) -> false,
                  (c, a, b) -> false,
                  tokens,
                  CallAuthorizationPolicy.denyAll(),
                  Clock.systemUTC(),
                  () -> heldRpc || (monitor.valid()),
                  (c, r) -> false),
              readiness);
      Cluster.get(system).manager().tell(new Join(Cluster.get(system).selfMember().address()));
      org.awaitility.Awaitility.await()
          .atMost(Duration.ofSeconds(10))
          .until(() -> Cluster.get(system).selfMember().status().equals(MemberStatus.up()));
      actors.register();
      var network =
          new NativeSagaEffects.Network() {
            public CompletionStage<io.webrtc.signaling.protocol.internal.InternalReply> call(
                CellRpcServer.Operation o,
                io.webrtc.signaling.protocol.internal.InternalCommand c,
                Duration b) {
              throw new AssertionError("TEST_ONLY no traffic");
            }

            public RpcOperation<io.webrtc.signaling.protocol.internal.InternalReply> callTracked(
                CellRpcServer.Operation o,
                io.webrtc.signaling.protocol.internal.InternalCommand c,
                Duration b) {
              throw new AssertionError("TEST_ONLY no traffic");
            }
          };
      ingress =
          actors
              .rpcIngress(
                  "test",
                  0,
                  RpcTlsContexts.server(
                      "test",
                      "c001",
                      NativeActorCompositionIT.RelayGatewayFixture.cert("ca.crt"),
                      NativeActorCompositionIT.RelayGatewayFixture.cert("actor.crt"),
                      NativeActorCompositionIT.RelayGatewayFixture.cert("actor.key")),
                  new RpcAdmission(2, 196608, 2, 196608),
                  network,
                  4,
                  new RelayBufferBudget(262144),
                  gateways,
                  (p, g) -> g.gatewayId().equals("gw-1"),
                  e -> CompletableFuture.failedFuture(new AssertionError()))
              .start();
      health =
          new PrivateHealthServer(
              new InetSocketAddress("127.0.0.1", 0),
              () -> true,
              readiness::businessReady,
              () -> "TEST_ONLY 1\n");
      health.start().toCompletableFuture().get(2, TimeUnit.SECONDS);
      var actualIngress = ingress;
      var actualHealth = health;
      int rpcPort = ingress.server().port();
      if (completeOwner)
        processClient =
            new CellRpcClient(
                "test",
                Map.of("c001", new CellRpcClient.Endpoint("localhost", rpcPort, "localhost")),
                RpcTlsContexts.clients(
                    "test",
                    NativeActorCompositionIT.RelayGatewayFixture.cert("ca.crt"),
                    NativeActorCompositionIT.RelayGatewayFixture.cert("actor.crt"),
                    NativeActorCompositionIT.RelayGatewayFixture.cert("actor.key")),
                new RpcAdmission(2, 196608, 2, 196608));
      var actualProcessClient = processClient;
      var lease =
          org.apache.pekko.coordination.lease.javadsl.LeaseProvider.get(Adapter.toClassic(system))
              .getLease(
                  "incomplete-startup-c001-shard-SignalingCallV1-985",
                  "signaling.postgres-lease",
                  Cluster.get(system).selfMember().address().hostPort());
      RpcOperation<io.webrtc.signaling.protocol.internal.SessionReply> original = null;
      if (heldRpc) {
        assertThat(lease.acquire().toCompletableFuture().join()).isTrue();
        caller =
            new CellRpcClient(
                "test",
                Map.of("c001", new CellRpcClient.Endpoint("localhost", rpcPort, "localhost")),
                RpcTlsContexts.clients(
                    "test",
                    NativeActorCompositionIT.RelayGatewayFixture.cert("ca.crt"),
                    NativeActorCompositionIT.RelayGatewayFixture.cert("gateway.crt"),
                    NativeActorCompositionIT.RelayGatewayFixture.cert("gateway.key")),
                new RpcAdmission(2, 196608, 2, 196608));
        var request =
            new NativeSessionHandler.Request(
                "REGISTER",
                new NativeSessionHandler.GatewayIdentity(
                    "gw-1", UUID.randomUUID(), "c001", 1, "TEST_ONLY"),
                "TEST_ONLY_HELD",
                null,
                UUID.randomUUID(),
                1,
                0,
                UUID.randomUUID());
        var command =
            io.webrtc.signaling.protocol.internal.SessionCommand.newBuilder()
                .setSchemaMajor(1)
                .setDestinationCell("c001")
                .setType("REGISTER")
                .setOperationId(request.operation().toString())
                .setRemainingBudgetMs(2000)
                .setPayload(
                    com.google.protobuf.ByteString.copyFrom(RpcBusinessHandler.encode(request)))
                .build();
        original = caller.sessionTracked(command, Duration.ofSeconds(2));
        assertThat(cryptoEntered.await(2, TimeUnit.SECONDS)).isTrue();
      }
      var defaults =
          new YamlPropertySourceLoader()
              .load(
                  "TEST_ONLY_defaults",
                  new FileSystemResource("../config/production-defaults.yaml"));
      var application = SignalingApplication.application();
      application.setWebApplicationType(WebApplicationType.NONE);
      application.setRegisterShutdownHook(false);
      application.addInitializers(
          c -> {
            defaults.forEach(s -> c.getEnvironment().getPropertySources().addLast(s));
            var registry =
                (org.springframework.beans.factory.support.BeanDefinitionRegistry)
                    c.getBeanFactory();
            owned(registry, "testOnlyActors", NativeActorComposition.class, actors);
            owned(registry, "testOnlyIngress", NativeActorRpcIngress.class, actualIngress);
            owned(registry, "testOnlySources", NativeActorSafetySources.class, sources);
            owned(registry, "testOnlyWorkers", NativeWorkerScheduler.class, workers);
            owned(registry, "testOnlyHealth", PrivateHealthServer.class, actualHealth);
            owned(registry, "testOnlyDatabase", DbBoundary.class, runtime.boundary);
            if (completeOwner) {
              owned(registry, "testOnlyServer", CellRpcServer.class, actualIngress.server());
              owned(registry, "testOnlyClient", CellRpcClient.class, actualProcessClient);
              owned(registry, "testOnlyPools", DbPools.class, runtime.pools);
            }
            // Partial cases intentionally lack native pool/client ownership.
          });
      var launch =
          CompletableFuture.supplyAsync(
              () ->
                  catchThrowable(
                      () ->
                          context.set(
                              application.run(
                                  "--spring.profiles.active=actor",
                                  "--signaling.identity.issuer=TEST_ONLY_ISSUER",
                                  "--signaling.identity.audience=TEST_ONLY_AUDIENCE"))));
      if (heldRpc) {
        org.awaitility.Awaitility.await()
            .atMost(Duration.ofSeconds(5))
            .until(() -> readiness.snapshot().draining());
        assertThatCode(
                () ->
                    org.awaitility.Awaitility.await()
                        .during(Duration.ofSeconds(2))
                        .atMost(Duration.ofSeconds(3))
                        .untilAsserted(
                            () ->
                                assertThat(lease.checkLease())
                                    .as(
                                        "original listener RPC must settle before native root release")
                                    .isTrue()))
            .doesNotThrowAnyException();
        assertThat(launch).isNotDone();
        cryptoRelease.countDown();
        original.physicalCompletion().toCompletableFuture().get(3, TimeUnit.SECONDS);
      }
      var failure = launch.get(25, TimeUnit.SECONDS);
      if (completeOwner) {
        assertThat(failure).isNull();
        assertThat(context.get().getBean(NativeActorSpringLifecycle.class).isRunning()).isTrue();
        assertThat(readiness.businessReady())
            .as("unhealthy TEST_ONLY sources cannot become business-ready")
            .isFalse();
        context.get().stop();
        assertThat(context.get().getBean(NativeActorRuntimeHooks.class).databaseClosed()).isTrue();
      } else {
        assertThat(failure).as("Main must require original actor lifecycle ownership").isNotNull();
        assertThat(failure).hasMessage("Native runtime not installed: ACTOR");
      }
      assertThat(system.getWhenTerminated().toCompletableFuture()).isCompleted();
      assertThatThrownBy(() -> new Socket("127.0.0.1", rpcPort))
          .isInstanceOf(java.io.IOException.class);
      assertThat(runtime.pools.closed())
          .as("only the published process pool transfers native ownership")
          .isEqualTo(completeOwner);
    } finally {
      cryptoRelease.countDown();
      if (caller != null) caller.drain().toCompletableFuture().get(5, TimeUnit.SECONDS);
      if (processClient != null)
        processClient.drain().toCompletableFuture().get(5, TimeUnit.SECONDS);
      if (context.get() != null) context.get().close();
      workers.drain().toCompletableFuture().get(3, TimeUnit.SECONDS);
      if (ingress != null) ingress.drain().toCompletableFuture().get(5, TimeUnit.SECONDS);
      sources.drain().toCompletableFuture().get(3, TimeUnit.SECONDS);
      if (health != null) health.stop().toCompletableFuture().get(5, TimeUnit.SECONDS);
      system.terminate();
      system.getWhenTerminated().toCompletableFuture().get(25, TimeUnit.SECONDS);
      tokens.close();
      gateways.drain().toCompletableFuture().get(3, TimeUnit.SECONDS);
      runtime.close();
    }
  }

  private static <T> void owned(
      org.springframework.beans.factory.support.BeanDefinitionRegistry registry,
      String name,
      Class<T> type,
      T value) {
    var definition = new RootBeanDefinition(type, () -> value);
    definition.setDestroyMethodName("");
    registry.registerBeanDefinition(name, definition);
  }
}
