package io.webrtc.signaling.app.runtime.local;

import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpVersion;
import io.webrtc.signaling.app.runtime.NativeCachedRevocationSource;
import io.webrtc.signaling.app.runtime.NativeCachedSecurity;
import io.webrtc.signaling.app.runtime.NativeCellRpcEnrollment;
import io.webrtc.signaling.app.runtime.NativeClockSource;
import io.webrtc.signaling.app.runtime.NativeCompatibilityObservationEnrollment;
import io.webrtc.signaling.app.runtime.NativeGatewayBusinessEnrollment;
import io.webrtc.signaling.app.runtime.NativeGatewayIngressEnrollment;
import io.webrtc.signaling.app.runtime.NativeGatewayLifecycleEnrollment;
import io.webrtc.signaling.app.runtime.NativeTelemetry;
import io.webrtc.signaling.app.runtime.NativeWorkerScheduler;
import io.webrtc.signaling.app.runtime.local.LocalRuntimeFailure.Category;
import io.webrtc.signaling.app.runtime.local.LocalRuntimeFailure.Role;
import io.webrtc.signaling.auth.AuthPrincipal;
import io.webrtc.signaling.auth.AuthorizationStatus;
import io.webrtc.signaling.auth.BoundedTokenVerifier;
import io.webrtc.signaling.auth.ClockSafetyMonitor;
import io.webrtc.signaling.auth.RevocationState;
import io.webrtc.signaling.protocol.Identity.SessionKey;
import io.webrtc.signaling.protocol.Identity.UserId;
import io.webrtc.signaling.rpc.GatewayRelayRpcClient;
import io.webrtc.signaling.rpc.NativeSessionHandler;
import io.webrtc.signaling.rpc.ProofBindings;
import io.webrtc.signaling.storage.DbBoundary;
import io.webrtc.signaling.storage.DbPools;
import io.webrtc.signaling.storage.SqlTransactions;
import io.webrtc.signaling.storage.worker.RevocationReconciler;
import io.webrtc.signaling.storage.worker.RevocationSourceVerifier;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.mock.env.MockEnvironment;

import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.security.PrivateKey;
import java.security.Signature;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.LongSupplier;

import static io.webrtc.signaling.app.runtime.local.LocalActorFixture.BOOT_C001;
import static io.webrtc.signaling.app.runtime.local.LocalActorFixture.BOOT_C002;
import static io.webrtc.signaling.app.runtime.local.LocalActorInstallerTest.assertFailure;
import static io.webrtc.signaling.app.runtime.local.LocalActorInstallerTest.destroy;
import static io.webrtc.signaling.app.runtime.local.LocalActorInstallerTest.instance;
import static io.webrtc.signaling.app.runtime.local.LocalActorInstallerTest.userInBucket;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class LocalGatewayInstallerTest {
    static final String GATEWAY_URI = "spiffe://signaling/local-test-only/cell/c001/gateway/local-c001";
    static final String ORIGIN = "https://app.example.test";

    /** Exactly the definitions the installer registers, one per type. */
    static final List<Class<?>> REGISTERED =
            List.of(
                    NativeGatewayIngressEnrollment.class,
                    NativeCellRpcEnrollment.class,
                    NativeCompatibilityObservationEnrollment.class,
                    ClockSafetyMonitor.class,
                    NativeCachedSecurity.class,
                    BoundedTokenVerifier.class,
                    NativeClockSource.class,
                    NativeCachedRevocationSource.class,
                    NativeWorkerScheduler.class,
                    NativeGatewayBusinessEnrollment.class,
                    NativeGatewayLifecycleEnrollment.class);

    @TempDir
    static Path pki;
    static LocalTestPki authority;
    static LocalTestPki.Leaf gateway;

    @TempDir
    Path mount;
    LocalTestKeys keys;

    @BeforeAll
    static void authority() throws Exception {
        authority = LocalTestPki.create(pki);
        gateway = authority.leaf(pki, "gateway", "URI:" + GATEWAY_URI + ",DNS:localhost");
    }

    /** A gateway mount: the actor fixture's two cells and gateways, this one pinned at BOOT_C001. */
    LocalRuntimeContract contract(
            Consumer<Map<String, Object>> profileEdit, Consumer<Map<String, Object>> snapshotEdit)
            throws Exception {
        authority.install(mount, gateway);
        keys = LocalTestKeys.install(mount);
        return LocalActorFixture.write(
                mount,
                p -> {
                    p.put("plane", "GATEWAY");
                    p.put("gatewayId", "local-c001");
                    p.put("gatewayBootId", BOOT_C001.toString());
                    p.put("wal", null);
                    p.put("kube", null);
                    p.put("privacyPseudonymKey", null);
                    profileEdit.accept(p);
                },
                snapshotEdit);
    }

    LocalRuntimeContract contract(Consumer<Map<String, Object>> profileEdit) throws Exception {
        return contract(profileEdit, s -> {
        });
    }

    LocalRuntimeContract contract() throws Exception {
        return contract(p -> {
        });
    }

    /** The gateway needs only the cluster fingerprint: no remoting passwords, no Kubernetes API. */
    static GenericApplicationContext context() {
        var ctx = new GenericApplicationContext();
        ctx.setEnvironment(
                new MockEnvironment()
                        .withProperty("SIGNALING_CLUSTER_FINGERPRINT", LocalPekkoConfigTest.FINGERPRINT)
                        .withProperty("SIGNALING_CELL_ID", "c001"));
        return ctx;
    }

    static GenericApplicationContext install(LocalRuntimeContract c) {
        var ctx = context();
        LocalGatewayInstaller.install(ctx, c);
        return ctx;
    }

    /** Every supplier run; only telemetry, which needs the auto-configured registry, is a mock. */
    static GenericApplicationContext refreshed(LocalRuntimeContract c, Clock wall, LongSupplier elapsed) {
        var ctx = context();
        LocalGatewayInstaller.install(ctx, c, wall, elapsed);
        var telemetry = mock(NativeTelemetry.class);
        when(telemetry.scrape()).thenReturn("TEST_ONLY_METRICS");
        ctx.registerBean(NativeTelemetry.class, () -> telemetry, d -> d.setDestroyMethodName(""));
        ctx.refresh();
        return ctx;
    }

    static GenericApplicationContext refreshed(LocalRuntimeContract c) {
        return refreshed(c, Clock.systemUTC(), System::nanoTime);
    }

    /** The lifecycle owns these; a test that refreshes without one retires them itself. */
    static void retireSources(GenericApplicationContext ctx) throws Exception {
        ctx.getBean(NativeWorkerScheduler.class).drain().toCompletableFuture().get(5, TimeUnit.SECONDS);
        ctx.getBean(NativeClockSource.class).drain().toCompletableFuture().get(5, TimeUnit.SECONDS);
        ctx.getBean(NativeCachedRevocationSource.class).drain().toCompletableFuture().get(5, TimeUnit.SECONDS);
    }

    @Test
    void registersGatewayEnrollmentsAndSourceBeans() throws Exception {
        var c = contract();
        var definitions = install(c);
        var factory = definitions.getBeanFactory();
        assertThat(factory.getBeanDefinitionNames())
                .containsExactlyInAnyOrderElementsOf(REGISTERED.stream().map(Class::getName).toList());
        for (var type : REGISTERED)
            assertThat(factory.getType(LocalActorInstallerTest.name(definitions, type), false)).isEqualTo(type);
        // No database owner exists on a gateway.
        for (var type : List.of(SqlTransactions.class, DbBoundary.class, DbPools.class))
            assertThat(factory.getBeanNamesForType(type, true, false)).as(type.getName()).isEmpty();
        // The verifier's pool is closed by the context; the sources and their worker are drained by
        // the gateway lifecycle (or startup cleanup), never by bean destruction.
        assertThat(destroy(definitions, BoundedTokenVerifier.class)).isEqualTo("close");
        for (var type :
                List.of(NativeClockSource.class, NativeCachedRevocationSource.class, NativeWorkerScheduler.class))
            assertThat(destroy(definitions, type)).as(type.getName()).isEmpty();

        var ctx = refreshed(c);
        var tokens = ctx.getBean(BoundedTokenVerifier.class);
        try {
            var business = ctx.getBean(NativeGatewayBusinessEnrollment.class);
            assertThat(business.identity())
                    .isEqualTo(new NativeSessionHandler.GatewayIdentity("local-c001", BOOT_C001, "c001", 1, "c001"));
            assertThat(business.routingEpoch()).isEqualTo(1);
            assertThat(business.tokens()).isSameAs(tokens);
            assertThat(business.relayCacheCapacity()).isEqualTo(LocalGatewayInstaller.RELAY_CACHE_CAPACITY);
            assertThat(business.relayPendingLimit()).isEqualTo(LocalGatewayInstaller.RELAY_PENDING_LIMIT);
            assertThat(business.homes().apply(userInBucket(8192, 16383)))
                    .isEqualTo(new ProofBindings.TrustedHome("c002", 3, 1));

            // Admission is the scoped cache, which starts closed: never an always-allow function.
            var principal = principal("TEST_ONLY_allowed", Instant.now().plusSeconds(600));
            assertThat(business.cachedSecurity().apply(principal, Instant.now()))
                    .isEqualTo(AuthorizationStatus.FRESHNESS_UNKNOWN);
            assertThat(business.securityFresh().getAsBoolean()).isFalse();

            var clock = ctx.getBean(ClockSafetyMonitor.class);
            assertThat(ctx.getBean(NativeClockSource.class).monitors(clock)).isTrue();

            var lifecycle = ctx.getBean(NativeGatewayLifecycleEnrollment.class);
            assertThat(lifecycle.metrics().get()).isEqualTo("TEST_ONLY_METRICS");
            assertThat(lifecycle.live().getAsBoolean()).isTrue();

            var cells = ctx.getBean(NativeCellRpcEnrollment.class);
            assertThat(cells.environment()).isEqualTo("local-test-only");
            assertThat(cells.destinations()).containsOnlyKeys("c001", "c002");
            assertThat(cells.admission()).isEqualTo(LocalActorInstaller.ADMISSION);

            var observation = ctx.getBean(NativeCompatibilityObservationEnrollment.class);
            assertThat(observation.cell()).isEqualTo("c001");
            assertThat(observation.installedBuild().fingerprint()).isEqualTo(LocalPekkoConfigTest.FINGERPRINT);
            assertThat(observation.gateways())
                    .containsExactlyInAnyOrder(
                            new GatewayRelayRpcClient.Target("c001", "local-c001", BOOT_C001),
                            new GatewayRelayRpcClient.Target("c002", "local-c002", BOOT_C002));

            for (var type : REGISTERED)
                assertThat(ctx.getBean(type).getClass()).as(type.getName()).isEqualTo(type);
            retireSources(ctx);
        } finally {
            ctx.close();
        }
        assertThat(tokens.drain().toCompletableFuture()).isCompleted();
    }

    @Test
    void wssPathIsExactlyWsAndSubprotocolIsV1() throws Exception {
        var ingress = instance(install(contract()), NativeGatewayIngressEnrollment.class);
        var policy = ingress.upgradePolicy();
        assertThat(policy.origins()).containsExactly(ORIGIN);
        assertThat(policy.nativePolicy()).isSameAs(LocalGatewayInstaller.NO_NATIVE_CLIENTS);

        assertThat(policy.accepts(upgrade(HttpMethod.GET, "/ws", ORIGIN, "webrtc-signaling.v1"))).isTrue();
        for (var path : List.of("/ws/", "/WS", "/ws?x=1", "/", "/wss", "/v1/ws"))
            assertThat(policy.accepts(upgrade(HttpMethod.GET, path, ORIGIN, "webrtc-signaling.v1")))
                    .as(path)
                    .isFalse();
        for (var protocol : List.of("webrtc-signaling.v2", "webrtc-signaling", "chat"))
            assertThat(policy.accepts(upgrade(HttpMethod.GET, "/ws", ORIGIN, protocol))).as(protocol).isFalse();
        assertThat(policy.accepts(upgrade(HttpMethod.GET, "/ws", ORIGIN, null))).as("no subprotocol").isFalse();
        assertThat(policy.accepts(upgrade(HttpMethod.GET, "/ws", "https://other.example.test", "webrtc-signaling.v1")))
                .as("origin outside the allow-list")
                .isFalse();
        assertThat(policy.accepts(upgrade(HttpMethod.GET, "/ws", ORIGIN + ":8443", "webrtc-signaling.v1")))
                .as("origins are exact, port included")
                .isFalse();
        assertThat(policy.accepts(upgrade(HttpMethod.GET, "/ws", null, "webrtc-signaling.v1")))
                .as("origin-less native clients are refused")
                .isFalse();
        assertThat(policy.accepts(upgrade(HttpMethod.POST, "/ws", ORIGIN, "webrtc-signaling.v1"))).isFalse();

        assertThat(ingress.wssAddress()).isEqualTo(new InetSocketAddress("0.0.0.0", 8443));
        assertThat(ingress.rpcPort()).isEqualTo(9443);
        assertThat(ingress.rpcEnvironment()).isEqualTo("local-test-only");
        assertThat(ingress.eventLoops()).isEqualTo(LocalGatewayInstaller.EVENT_LOOPS);
        assertThat(ingress.maxUnauthenticated()).isEqualTo(LocalGatewayInstaller.MAX_UNAUTHENTICATED);
        assertThat(ingress.maxConnections()).isEqualTo(LocalGatewayInstaller.MAX_CONNECTIONS);
        assertThat(ingress.rpcAdmission()).isEqualTo(LocalActorInstaller.ADMISSION);
        assertThat(ingress.writeAdmission()).isEqualTo(LocalActorInstaller.ADMISSION);

        // A gateway without any allowed origin would refuse every browser: refused at startup.
        assertFailure(() -> install(contract(p -> p.put("allowedOrigins", List.of()))), Role.PROFILE, Category.INVALID);
    }

    static FullHttpRequest upgrade(HttpMethod method, String path, String origin, String protocol) {
        var request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, method, path);
        request.headers().set(HttpHeaderNames.UPGRADE, "websocket");
        request.headers().set(HttpHeaderNames.CONNECTION, "Upgrade");
        if (origin != null) request.headers().set(HttpHeaderNames.ORIGIN, origin);
        if (protocol != null) request.headers().set(HttpHeaderNames.SEC_WEBSOCKET_PROTOCOL, protocol);
        return request;
    }

    @Test
    void healthAddressIsConcreteNotWildcard() throws Exception {
        var lifecycle = instance(install(contract()), NativeGatewayLifecycleEnrollment.class);
        assertThat(lifecycle.healthAddress()).isEqualTo(new InetSocketAddress("192.0.2.10", 9090));
        assertThat(lifecycle.healthAddress().getAddress().isAnyLocalAddress()).isFalse();

        assertThat(LocalGatewayInstaller.concrete(new InetSocketAddress("192.0.2.10", 9090))).isTrue();
        assertThat(LocalGatewayInstaller.concrete(new InetSocketAddress("0.0.0.0", 9090))).isFalse();
        assertThat(LocalGatewayInstaller.concrete(InetSocketAddress.createUnresolved("192.0.2.10", 9090)))
                .isFalse();

        var wildcard = contract(p -> p.put("podIp", "0.0.0.0"));
        var ctx = context();
        assertFailure(() -> LocalGatewayInstaller.install(ctx, wildcard), Role.PROFILE, Category.INVALID);
        assertThat(ctx.getBeanDefinitionCount()).as("refused before any definition").isZero();

        // The relay listener may not share the WSS port.
        assertFailure(() -> install(contract(p -> p.put("relayPort", 8443))), Role.PROFILE, Category.INVALID);
        assertFailure(() -> install(contract(p -> p.put("relayPort", 9090))), Role.PROFILE, Category.INVALID);
    }

    @Test
    void refusesProfileWithTransferCells() throws Exception {
        // Valid twin: the same gateway mount without transfer cells loads and installs.
        assertThat(install(contract()).getBeanDefinitionCount()).isEqualTo(REGISTERED.size());
        assertFailure(
                () -> contract(p -> p.put("transferCells", List.of(LocalRuntimeFixtures.transferCell("c002", 1)))),
                Role.PROFILE,
                Category.INVALID);
        // A profile for another plane is never installed as a gateway.
        var actor = contract(p -> p.put("plane", "ACTOR"));
        var ctx = context();
        assertFailure(() -> LocalGatewayInstaller.install(ctx, actor), Role.PROFILE, Category.INVALID);
        assertThat(ctx.getBeanDefinitionCount()).isZero();
        // So is a gateway profile without a gateway identity.
        assertFailure(
                () -> install(contract(p -> {
                    p.put("gatewayId", null);
                    p.put("gatewayBootId", null);
                })),
                Role.PROFILE,
                Category.INVALID);
    }

    @Test
    void gatewayBootIdComesFromProfileNotRandom() throws Exception {
        var c = contract();
        var one = refreshed(c);
        var two = refreshed(c);
        try {
            var a = one.getBean(NativeGatewayBusinessEnrollment.class).identity();
            var b = two.getBean(NativeGatewayBusinessEnrollment.class).identity();
            assertThat(a.bootId()).isEqualTo(BOOT_C001).isEqualTo(c.profile().gatewayBootId());
            assertThat(b.bootId()).isEqualTo(a.bootId());
            assertThat(a.bootId()).as("the JVM's process boot is a different value").isNotEqualTo(c.processBoot());
            retireSources(one);
            retireSources(two);
        } finally {
            one.close();
            two.close();
        }

        // A boot the snapshot does not pin for this gateway is not this run's gateway.
        var unpinned = UUID.fromString("00000000-0000-4000-8000-0000000000ff");
        assertFailure(
                () -> install(contract(p -> p.put("gatewayBootId", unpinned.toString()))),
                Role.IDENTITY,
                Category.IDENTITY_MISMATCH);
        // Nor is another cell's gateway at its own pinned boot.
        assertFailure(
                () -> install(contract(p -> {
                    p.put("gatewayId", "local-c002");
                    p.put("gatewayBootId", BOOT_C002.toString());
                })),
                Role.IDENTITY,
                Category.IDENTITY_MISMATCH);
    }

    @Test
    void cachedSecurityDeniesWhenRevocationFeedIsStale() throws Exception {
        var c = contract();
        var wall = new MutableClock(Instant.parse("2026-10-21T00:00:00Z"));
        var elapsed = new AtomicLong(1_000_000_000L);
        var ctx = refreshed(c, wall, elapsed::get);
        try {
            var business = ctx.getBean(NativeGatewayBusinessEnrollment.class);
            var security = ctx.getBean(NativeCachedSecurity.class);
            var allowed = principal("TEST_ONLY_allowed", wall.instant().plusSeconds(600));
            var revoked = principal("TEST_ONLY_revoked", wall.instant().plusSeconds(600));

            // No page yet: closed.
            assertThat(business.cachedSecurity().apply(allowed, wall.instant()))
                    .isEqualTo(AuthorizationStatus.FRESHNESS_UNKNOWN);
            assertThat(business.securityFresh().getAsBoolean()).isFalse();

            // A page not signed by the contract's source key is refused and admission stays closed.
            var forged = java.security.KeyPairGenerator.getInstance("Ed25519").generateKeyPair().getPrivate();
            assertThat(security.apply(page(forged, wall.instant(), revoked.userId()), elapsed.get())).isFalse();
            assertThat(business.securityFresh().getAsBoolean()).isFalse();

            // A fresh signed page opens admission and carries its revocation.
            assertThat(security.apply(page(keys.source.getPrivate(), wall.instant(), revoked.userId()), elapsed.get()))
                    .isTrue();
            assertThat(business.securityFresh().getAsBoolean()).isTrue();
            assertThat(business.cachedSecurity().apply(allowed, wall.instant())).isEqualTo(AuthorizationStatus.ALLOWED);
            assertThat(business.cachedSecurity().apply(revoked, wall.instant())).isEqualTo(AuthorizationStatus.REVOKED);

            // Just inside the 5 s bound it still admits; at the bound the feed is stale.
            wall.advance(Duration.ofMillis(4999));
            elapsed.addAndGet(Duration.ofMillis(4999).toNanos());
            assertThat(business.cachedSecurity().apply(allowed, wall.instant())).isEqualTo(AuthorizationStatus.ALLOWED);
            wall.advance(Duration.ofMillis(1));
            elapsed.addAndGet(Duration.ofMillis(1).toNanos());
            assertThat(business.cachedSecurity().apply(allowed, wall.instant()))
                    .isEqualTo(AuthorizationStatus.FRESHNESS_UNKNOWN);
            assertThat(business.securityFresh().getAsBoolean()).isFalse();

            // Monotonic staleness alone closes it, even if the wall clock stood still.
            var restart = refreshed(c, wall, elapsed::get);
            try {
                var again = restart.getBean(NativeGatewayBusinessEnrollment.class);
                assertThat(restart.getBean(NativeCachedSecurity.class)
                        .apply(page(keys.source.getPrivate(), wall.instant(), revoked.userId()), elapsed.get()))
                        .isTrue();
                assertThat(again.securityFresh().getAsBoolean()).isTrue();
                elapsed.addAndGet(Duration.ofSeconds(5).toNanos());
                assertThat(again.cachedSecurity().apply(allowed, wall.instant()))
                        .isEqualTo(AuthorizationStatus.FRESHNESS_UNKNOWN);
                assertThat(again.securityFresh().getAsBoolean()).isFalse();
                retireSources(restart);
            } finally {
                restart.close();
            }

            // A draining source closes freshness even over a fresh cache.
            assertThat(security.apply(page(keys.source.getPrivate(), wall.instant(), revoked.userId(), 1), elapsed.get()))
                    .isTrue();
            assertThat(business.securityFresh().getAsBoolean()).isTrue();
            retireSources(ctx);
            assertThat(business.securityFresh().getAsBoolean()).isFalse();
        } finally {
            ctx.close();
        }
    }

    @Test
    void homesRejectUnknownBucket() throws Exception {
        var c =
                contract(
                        p -> {
                        },
                        s -> s.put("buckets", new ArrayList<>(List.of(LocalActorFixture.range(0, 8191, "c001")))));
        var ctx = refreshed(c);
        try {
            var homes = ctx.getBean(NativeGatewayBusinessEnrollment.class).homes();
            assertThat(homes.apply(userInBucket(0, 8191))).isEqualTo(new ProofBindings.TrustedHome("c001", 3, 1));
            assertThat(homes.apply(userInBucket(8192, 16383))).as("a bucket the snapshot does not name").isNull();
            retireSources(ctx);
        } finally {
            ctx.close();
        }
    }

    static AuthPrincipal principal(String user, Instant expires) {
        return new AuthPrincipal(
                new UserId(user),
                new SessionKey("LOCAL_TEST_ONLY_ISSUER", UUID.randomUUID().toString()),
                expires,
                expires.minusSeconds(600),
                "rsa",
                1);
    }

    static RevocationReconciler.Batch page(PrivateKey signer, Instant checkedAt, UserId revoked) throws Exception {
        return page(signer, checkedAt, revoked, 0);
    }

    /** One signed page from {@code from}: a user-scope revocation at epoch 1, caught up. */
    static RevocationReconciler.Batch page(PrivateKey signer, Instant checkedAt, UserId revoked, long from)
            throws Exception {
        var events =
                List.of(new RevocationState.Event("LOCAL_TEST_ONLY_ISSUER", revoked, null, 1, from + 1, checkedAt));
        var unsigned = new RevocationReconciler.Batch(from, from + 1, events, checkedAt, "", List.of(), from + 1);
        var signature = Signature.getInstance("Ed25519");
        signature.initSign(signer);
        signature.update(RevocationSourceVerifier.signingBytes("c001", "LOCAL_TEST_ONLY_ISSUER", unsigned));
        return new RevocationReconciler.Batch(
                from,
                from + 1,
                events,
                checkedAt,
                "source." + Base64.getUrlEncoder().withoutPadding().encodeToString(signature.sign()),
                List.of(),
                from + 1);
    }

    /** TEST_ONLY wall clock the test advances explicitly. */
    static final class MutableClock extends Clock {
        private volatile Instant now;

        MutableClock(Instant now) {
            this.now = now;
        }

        void advance(Duration by) {
            now = now.plus(by);
        }

        @Override
        public ZoneId getZone() {
            return ZoneOffset.UTC;
        }

        @Override
        public Clock withZone(ZoneId zone) {
            return this;
        }

        @Override
        public Instant instant() {
            return now;
        }
    }
}
