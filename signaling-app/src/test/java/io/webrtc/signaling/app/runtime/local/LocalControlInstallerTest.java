package io.webrtc.signaling.app.runtime.local;

import io.webrtc.signaling.app.runtime.NativeBucketTransferEnrollment;
import io.webrtc.signaling.app.runtime.NativeCachedRevocationSource;
import io.webrtc.signaling.app.runtime.NativeCachedSecurity;
import io.webrtc.signaling.app.runtime.NativeCellRpcEnrollment;
import io.webrtc.signaling.app.runtime.NativeClockSource;
import io.webrtc.signaling.app.runtime.NativeCompatibility;
import io.webrtc.signaling.app.runtime.NativeCompatibilityCollectors;
import io.webrtc.signaling.app.runtime.NativeCompatibilityEnrollment;
import io.webrtc.signaling.app.runtime.NativeCompatibilityObservationEnrollment;
import io.webrtc.signaling.app.runtime.NativeControlBusinessEnrollment;
import io.webrtc.signaling.app.runtime.NativeControlDatabaseEnrollment;
import io.webrtc.signaling.app.runtime.NativeControlDatabaseResources;
import io.webrtc.signaling.app.runtime.NativeControlIngressEnrollment;
import io.webrtc.signaling.app.runtime.NativeControlSourceConfiguration;
import io.webrtc.signaling.app.runtime.NativeControlSourceEnrollment;
import io.webrtc.signaling.app.runtime.NativeTelemetry;
import io.webrtc.signaling.app.runtime.NativeWorkerScheduler;
import io.webrtc.signaling.app.runtime.local.LocalRuntimeFailure.Category;
import io.webrtc.signaling.app.runtime.local.LocalRuntimeFailure.Role;
import io.webrtc.signaling.auth.AuthorizationStatus;
import io.webrtc.signaling.auth.BoundedTokenVerifier;
import io.webrtc.signaling.auth.ClockSafetyMonitor;
import io.webrtc.signaling.control.PostgresDirectoryRepository;
import io.webrtc.signaling.protocol.internal.CompatibilityActorDescriptor;
import io.webrtc.signaling.protocol.internal.CompatibilityObservationReply;
import io.webrtc.signaling.rpc.CellRpcClient;
import io.webrtc.signaling.rpc.RpcOperation;
import io.webrtc.signaling.storage.DbBoundary;
import io.webrtc.signaling.storage.DbPools;
import io.webrtc.signaling.storage.SqlTransactions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.context.annotation.AnnotationConfigUtils;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.mock.env.MockEnvironment;

import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.LongSupplier;

import static io.webrtc.signaling.app.runtime.local.LocalActorInstallerTest.assertFailure;
import static io.webrtc.signaling.app.runtime.local.LocalActorInstallerTest.destroy;
import static io.webrtc.signaling.app.runtime.local.LocalActorInstallerTest.instance;
import static io.webrtc.signaling.app.runtime.local.LocalActorInstallerTest.name;
import static io.webrtc.signaling.app.runtime.local.LocalGatewayInstallerTest.page;
import static io.webrtc.signaling.app.runtime.local.LocalGatewayInstallerTest.principal;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class LocalControlInstallerTest {
    static final String CONTROL_URI = "spiffe://signaling/local-test-only/cell/c001/control";

    /** Exactly the definitions the installer registers, one per type. */
    static final List<Class<?>> REGISTERED =
            List.of(
                    NativeControlDatabaseEnrollment.class,
                    NativeControlSourceEnrollment.class,
                    NativeControlIngressEnrollment.class,
                    NativeCompatibilityObservationEnrollment.class,
                    NativeCellRpcEnrollment.class,
                    NativeCachedSecurity.class,
                    BoundedTokenVerifier.class,
                    NativeCachedRevocationSource.class,
                    NativeWorkerScheduler.class,
                    SqlTransactions.class,
                    NativeBucketTransferEnrollment.class,
                    NativeControlBusinessEnrollment.class);

    @TempDir
    static Path pki;
    static LocalTestPki authority;
    static LocalTestPki.Leaf control, actor;

    @TempDir
    Path mount;
    LocalTestKeys keys;

    @BeforeAll
    static void authority() throws Exception {
        authority = LocalTestPki.create(pki);
        control = authority.leaf(pki, "control", "URI:" + CONTROL_URI + ",DNS:localhost");
        actor = authority.leaf(pki, "actor", "URI:" + LocalTestPki.ACTOR_URI + ",DNS:localhost");
    }

    /** A control mount: the actor fixture's two cells, c001 local, c002 the one transfer cell. */
    LocalRuntimeContract contract(LocalTestPki.Leaf leaf, Consumer<Map<String, Object>> profileEdit)
            throws Exception {
        authority.install(mount, leaf);
        keys = LocalTestKeys.install(mount);
        return LocalActorFixture.write(
                mount,
                p -> {
                    p.put("plane", "CONTROL");
                    p.put("cellAzRoles", List.of());
                    p.put("wal", null);
                    p.put("kube", null);
                    p.put("privacyPseudonymKey", null);
                    p.put("transferCells", List.of(LocalRuntimeFixtures.transferCell("c002", 1)));
                    profileEdit.accept(p);
                },
                s -> {
                });
    }

    LocalRuntimeContract contract(Consumer<Map<String, Object>> profileEdit) throws Exception {
        return contract(control, profileEdit);
    }

    LocalRuntimeContract contract() throws Exception {
        return contract(p -> {
        });
    }

    static MockEnvironment environment() {
        var env = LocalGatewayInstallerTest.environment();
        env.setActiveProfiles("control");
        return env;
    }

    static GenericApplicationContext context(MockEnvironment env) {
        var ctx = new GenericApplicationContext();
        ctx.setEnvironment(env);
        return ctx;
    }

    static final Function<NativeControlDatabaseEnrollment.Database, SqlTransactions> NO_OPEN =
            database -> {
                throw new AssertionError("TEST_ONLY: no database is opened during install");
            };

    static GenericApplicationContext install(LocalRuntimeContract c) {
        var ctx = context(environment());
        LocalControlInstaller.install(ctx, c, Clock.systemUTC(), System::nanoTime, NO_OPEN);
        return ctx;
    }

    static SqlTransactions sql() {
        var sql = mock(SqlTransactions.class);
        when(sql.boundary()).thenReturn(mock(DbBoundary.class));
        when(sql.pools()).thenReturn(mock(DbPools.class));
        return sql;
    }

    /**
     * The installer's definitions refreshed with the real control source configuration (the owner
     * of the clock monitor). Only what needs a database or the auto-configured registry is a mock:
     * telemetry, the regional/local database resources and the directory repository built on them.
     */
    record Refreshed(
            GenericApplicationContext ctx,
            List<NativeControlDatabaseEnrollment.Database> opened,
            SqlTransactions local,
            SqlTransactions transfer,
            PostgresDirectoryRepository directory) {
    }

    static Refreshed refreshed(LocalRuntimeContract c, Clock wall, LongSupplier elapsed) {
        var opened = new ArrayList<NativeControlDatabaseEnrollment.Database>();
        var local = sql();
        var transfer = sql();
        var ctx = context(environment());
        LocalControlInstaller.install(
                ctx,
                c,
                wall,
                elapsed,
                database -> {
                    opened.add(database);
                    return transfer;
                });
        assertThat(opened).as("install itself opens nothing").isEmpty();
        var telemetry = mock(NativeTelemetry.class);
        when(telemetry.scrape()).thenReturn("TEST_ONLY_METRICS");
        var resources = mock(NativeControlDatabaseResources.class);
        when(resources.local()).thenReturn(local);
        var directory = mock(PostgresDirectoryRepository.class);
        Consumer<org.springframework.beans.factory.config.BeanDefinition> ownedElsewhere =
                definition -> definition.setDestroyMethodName("");
        ctx.registerBean(NativeTelemetry.class, () -> telemetry, ownedElsewhere::accept);
        ctx.registerBean(NativeControlDatabaseResources.class, () -> resources, ownedElsewhere::accept);
        ctx.registerBean(PostgresDirectoryRepository.class, () -> directory, ownedElsewhere::accept);
        AnnotationConfigUtils.registerAnnotationConfigProcessors(ctx);
        ctx.registerBean(NativeControlSourceConfiguration.class);
        ctx.refresh();
        return new Refreshed(ctx, opened, local, transfer, directory);
    }

    static Refreshed refreshed(LocalRuntimeContract c) {
        return refreshed(c, Clock.systemUTC(), System::nanoTime);
    }

    /** NativeControlProcess owns these; a test that refreshes without one retires them itself. */
    static void retireSources(GenericApplicationContext ctx) throws Exception {
        for (var worker : ctx.getBeansOfType(NativeWorkerScheduler.class).values())
            worker.drain().toCompletableFuture().get(5, TimeUnit.SECONDS);
        ctx.getBean(NativeClockSource.class).drain().toCompletableFuture().get(5, TimeUnit.SECONDS);
        ctx.getBean(NativeCachedRevocationSource.class).drain().toCompletableFuture().get(5, TimeUnit.SECONDS);
    }

    @Test
    void registersControlEnrollmentsWithTwoDistinctTransferStores() throws Exception {
        var c = contract();
        var definitions = install(c);
        var factory = definitions.getBeanFactory();
        // Exactly one definition per type, and nothing else.
        assertThat(factory.getBeanDefinitionNames()).hasSize(REGISTERED.size());
        for (var type : REGISTERED) assertThat(factory.getType(name(definitions, type), false)).isEqualTo(type);
        assertThat(name(definitions, SqlTransactions.class)).isEqualTo(LocalControlInstaller.TRANSFER_TARGET_SQL);
        // The verifier's pool is closed by the context. The sources, their worker and the transfer
        // database are drained by NativeControlProcess (or startup cleanup), never by destruction.
        assertThat(destroy(definitions, BoundedTokenVerifier.class)).isEqualTo("close");
        for (var type :
                List.of(NativeCachedRevocationSource.class, NativeWorkerScheduler.class, SqlTransactions.class))
            assertThat(destroy(definitions, type)).as(type.getName()).isEmpty();
        // No DbBoundary/DbPools leaves: the control database configuration owns the cell databases.
        for (var type : List.of(DbBoundary.class, DbPools.class))
            assertThat(factory.getBeanNamesForType(type, true, false)).as(type.getName()).isEmpty();

        var r = refreshed(c);
        var ctx = r.ctx();
        var tokens = ctx.getBean(BoundedTokenVerifier.class);
        try {
            var transfer = ctx.getBean(NativeBucketTransferEnrollment.class);
            assertThat(transfer.source().sql()).isSameAs(r.local());
            assertThat(transfer.source().cell()).isEqualTo("c001");
            assertThat(transfer.source().storageEpoch()).isEqualTo(1);
            assertThat(transfer.target().sql()).isSameAs(r.transfer()).isNotSameAs(transfer.source().sql());
            assertThat(transfer.target().cell()).isEqualTo("c002");
            assertThat(transfer.target().storageEpoch()).isEqualTo(1);

            // Exactly three credentials: the regional directory and this cell (both opened by the
            // control database configuration) and the one transfer cell (opened here).
            var databases = ctx.getBean(NativeControlDatabaseEnrollment.class);
            assertThat(databases.regional()).isEqualTo(c.contract().directoryDatabase());
            assertThat(databases.local()).isEqualTo(c.contract().cellDatabase());
            assertThat(databases.regional()).isNotEqualTo(databases.local());
            assertThat(databases.cell()).isEqualTo("c001");
            assertThat(databases.storageEpoch()).isEqualTo(1);
            assertThat(databases.admittedRegionalWriter())
                    .isSameAs(LocalControlInstaller.SOLE_REGIONAL_WRITER);
            assertThat(r.opened()).containsExactly(c.profile().transferCells().getFirst().database());
            assertThat(r.opened().getFirst())
                    .isNotEqualTo(c.contract().cellDatabase())
                    .isNotEqualTo(c.contract().directoryDatabase());

            var business = ctx.getBean(NativeControlBusinessEnrollment.class);
            assertThat(business.directory()).isSameAs(r.directory());
            assertThat(business.tokens()).isSameAs(tokens);
            assertThat(business.directoryRefresh()).isEqualTo(Duration.ofSeconds(30));

            var source = ctx.getBean(NativeControlSourceEnrollment.class);
            assertThat(source.cell()).isEqualTo("c001");
            assertThat(source.storageEpoch()).isEqualTo(1);
            assertThat(source.podUid()).isEqualTo(c.profile().podUid());
            assertThat(source.processBoot()).isEqualTo(c.processBoot());
            assertThat(source.clock().uri()).hasToString(c.contract().sources().clockUrl());

            retireSources(ctx);
        } finally {
            ctx.close();
        }
        assertThat(tokens.drain().toCompletableFuture()).isCompleted();
    }

    @Test
    void ingressServesHttpsOnAllInterfacesAndProbesAConcreteAddress() throws Exception {
        var r = refreshed(contract());
        try {
            var ingress = r.ctx().getBean(NativeControlIngressEnrollment.class);
            assertThat(ingress.address()).isEqualTo(new InetSocketAddress("0.0.0.0", 8443));
            assertThat(ingress.tls().isServer()).isTrue();
            assertThat(ingress.healthAddress()).isEqualTo(new InetSocketAddress("192.0.2.10", 9090));
            assertThat(ingress.live().getAsBoolean()).isTrue();
            assertThat(ingress.metrics().get()).isEqualTo("TEST_ONLY_METRICS");
            retireSources(r.ctx());
        } finally {
            r.ctx().close();
        }
        var wildcard = contract(p -> p.put("podIp", "0.0.0.0"));
        var ctx = context(environment());
        assertFailure(
                () -> LocalControlInstaller.install(ctx, wildcard, Clock.systemUTC(), System::nanoTime, NO_OPEN),
                Role.PROFILE,
                Category.INVALID);
        assertThat(ctx.getBeanDefinitionCount()).isZero();
    }

    @Test
    void rejectsTransferCellEqualToOwnCell() throws Exception {
        assertThat(install(contract()).getBeanDefinitionCount()).as("valid twin").isEqualTo(REGISTERED.size());
        var own = contract(p -> p.put("transferCells", List.of(LocalRuntimeFixtures.transferCell("c001", 1))));
        var ctx = context(environment());
        assertFailure(
                () -> LocalControlInstaller.install(ctx, own, Clock.systemUTC(), System::nanoTime, NO_OPEN),
                Role.PROFILE,
                Category.INVALID);
        assertThat(ctx.getBeanDefinitionCount()).isZero();
    }

    @Test
    void rejectsZeroOrTwoTransferCells() throws Exception {
        assertThat(install(contract()).getBeanDefinitionCount()).as("valid twin").isEqualTo(REGISTERED.size());
        for (var cells :
                List.<List<Object>>of(
                        List.of(),
                        List.of(
                                LocalRuntimeFixtures.transferCell("c002", 1),
                                LocalRuntimeFixtures.transferCell("c003", 1)))) {
            var c = contract(p -> p.put("transferCells", cells));
            var ctx = context(environment());
            assertFailure(
                    () -> LocalControlInstaller.install(ctx, c, Clock.systemUTC(), System::nanoTime, NO_OPEN),
                    Role.PROFILE,
                    Category.INVALID);
            assertThat(ctx.getBeanDefinitionCount()).as(cells.size() + " transfer cells").isZero();
        }
    }

    @Test
    void rejectsATransferCellTheContractDoesNotEnrollAtThatEpoch() throws Exception {
        for (var cell :
                List.of(LocalRuntimeFixtures.transferCell("c003", 1), LocalRuntimeFixtures.transferCell("c002", 2))) {
            var c = contract(p -> p.put("transferCells", List.of(cell)));
            var ctx = context(environment());
            assertFailure(
                    () -> LocalControlInstaller.install(ctx, c, Clock.systemUTC(), System::nanoTime, NO_OPEN),
                    Role.PROFILE,
                    Category.INVALID);
            assertThat(ctx.getBeanDefinitionCount()).isZero();
        }
    }

    @Test
    void refusesAnotherPlane() throws Exception {
        var gateway =
                contract(
                        p -> {
                            p.put("plane", "GATEWAY");
                            p.put("transferCells", List.of());
                        });
        var ctx = context(environment());
        assertFailure(
                () -> LocalControlInstaller.install(ctx, gateway, Clock.systemUTC(), System::nanoTime, NO_OPEN),
                Role.PROFILE,
                Category.INVALID);
        assertThat(ctx.getBeanDefinitionCount()).isZero();
    }

    @Test
    void controlClockMonitorIsTheSameInstanceAsTheSource() throws Exception {
        var c = contract();
        var r = refreshed(c);
        try {
            var business = r.ctx().getBean(NativeControlBusinessEnrollment.class);
            var monitor = r.ctx().getBean(ClockSafetyMonitor.class);
            assertThat(business.clock()).isSameAs(monitor);
            // NativeControlProcess refuses a clock source that monitors any other instance.
            assertThat(r.ctx().getBean(NativeClockSource.class).monitors(business.clock())).isTrue();
            // A second monitor built from the same contract is a different owner and is not watched.
            assertThat(r.ctx().getBean(NativeClockSource.class).monitors(LocalTrust.clockMonitor(c)))
                    .isFalse();
            retireSources(r.ctx());
        } finally {
            r.ctx().close();
        }
    }

    @Test
    void controlCompatibilityCanOpenWithoutTestOnlyCapabilities() throws Exception {
        var c = contract();
        var definitions = install(c);
        // Nothing the installer registers, nor anything it builds, comes from test code.
        for (var name : definitions.getBeanDefinitionNames())
            assertThat(definitions.getBeanFactory().getType(name, false).getName())
                    .as(name)
                    .doesNotContain("TestOnly");
        // The gate is derived by the observation configuration from real collectors: the installer
        // never registers a compatibility enrollment of its own.
        assertThat(definitions.getBeanFactory().getBeanNamesForType(NativeCompatibilityEnrollment.class))
                .isEmpty();

        var observation = instance(definitions, NativeCompatibilityObservationEnrollment.class);
        assertThat(observation.cell()).isEqualTo("c001");
        assertThat(observation.cells()).containsExactlyInAnyOrder("c001", "c002");
        assertThat(observation.installedBuild().fingerprint()).isEqualTo(LocalPekkoConfigTest.FINGERPRINT);
        assertThat(observation.approvedWindow())
                .isEqualTo(new NativeCompatibilityEnrollment.ApprovedWindow(1, 1, 2));
        var cells = instance(definitions, NativeCellRpcEnrollment.class);
        assertThat(cells.environment()).isEqualTo("local-test-only");
        assertThat(cells.destinations()).containsOnlyKeys("c001", "c002");
        assertThat(cells.tls().context("c001").isClient()).isTrue();

        // controlReady() depends on observed actors: closed without them, open with a valid fresh
        // observation of this cell, closed again when the observation fails.
        var client = mock(CellRpcClient.class);
        var collectors = new NativeCompatibilityCollectors(observation, () -> client, () -> null, () -> null);
        var compatibility =
                new NativeCompatibility(
                        "c001", LocalPekkoConfigTest.FINGERPRINT, () -> null, () -> null, collectors::enrollment);
        assertThat(compatibility.controlReady()).isFalse();

        when(client.observeCompatibility(eq("c001"), any())).thenAnswer(i -> observed(actors("c001")));
        when(client.observeCompatibility(eq("c002"), any())).thenAnswer(i -> failed());
        poll(collectors);
        assertThat(compatibility.controlReady()).isTrue();

        when(client.observeCompatibility(eq("c001"), any())).thenAnswer(i -> failed());
        poll(collectors);
        assertThat(compatibility.controlReady()).isFalse();
        // A peer set without session-s1 does not open it either.
        when(client.observeCompatibility(eq("c001"), any()))
                .thenAnswer(i -> observed(actors("c001", "FULL_CONNECTION_ACTIVE")));
        poll(collectors);
        assertThat(compatibility.controlReady()).isFalse();
    }

    @Test
    void installRefusesALeafThatIsNotThisCellsControlIdentity() throws Exception {
        var c = contract(actor, p -> {
        });
        var ctx = context(environment());
        assertFailure(
                () -> LocalControlInstaller.install(ctx, c, Clock.systemUTC(), System::nanoTime, NO_OPEN),
                Role.IDENTITY,
                Category.IDENTITY_MISMATCH);
        assertThat(ctx.getBeanDefinitionCount()).isZero();
    }

    static void poll(NativeCompatibilityCollectors collectors) throws Exception {
        collectors
                .pollJob()
                .run()
                .apply(Duration.ofSeconds(1))
                .logical()
                .toCompletableFuture()
                .get(5, TimeUnit.SECONDS);
    }

    static RpcOperation<CompatibilityObservationReply> observed(CompatibilityObservationReply reply) {
        return new RpcOperation<>(
                CompletableFuture.completedFuture(reply), CompletableFuture.completedFuture(null));
    }

    static RpcOperation<CompatibilityObservationReply> failed() {
        return new RpcOperation<>(
                CompletableFuture.failedFuture(new IllegalStateException("TEST_ONLY unavailable")),
                CompletableFuture.completedFuture(null));
    }

    /** Four authenticated actors of {@code cell} in two zones, on the enrolled build. */
    static CompatibilityObservationReply actors(String cell, String... features) {
        var names = features.length == 0 ? new String[]{"SESSION_S1", "FULL_CONNECTION_ACTIVE"} : features;
        long now = Instant.now().toEpochMilli();
        var reply =
                CompatibilityObservationReply.newBuilder()
                        .setSourceCell(cell)
                        .setObservedAtMs(now)
                        .setValidUntilMs(now + 4000);
        for (int i = 0; i < 4; i++)
            reply.addActors(
                    CompatibilityActorDescriptor.newBuilder()
                            .setCell(cell)
                            .setMajor(1)
                            .setMinor(2)
                            .setExecutableId(LocalPekkoConfigTest.EXECUTABLE)
                            .setFingerprint(LocalPekkoConfigTest.FINGERPRINT)
                            .addAllFeatures(Arrays.asList(names))
                            .setMemberId(UUID.randomUUID().toString())
                            .setZone(i % 2 == 0 ? "az-a" : "az-b"));
        return reply.build();
    }

    @Test
    void cachedSecurityDeniesWhenRevocationFeedIsStale() throws Exception {
        var c = contract();
        var wall = new LocalGatewayInstallerTest.MutableClock(Instant.parse("2026-10-21T00:00:00Z"));
        var elapsed = new AtomicLong(1_000_000_000L);
        var r = refreshed(c, wall, elapsed::get);
        try {
            var business = r.ctx().getBean(NativeControlBusinessEnrollment.class);
            var security = r.ctx().getBean(NativeCachedSecurity.class);
            var allowed = principal("TEST_ONLY_allowed", wall.instant().plusSeconds(600));
            var revoked = principal("TEST_ONLY_revoked", wall.instant().plusSeconds(600));

            // No page yet: closed, never an always-allow function.
            assertThat(business.cachedSecurity().apply(allowed, wall.instant()))
                    .isEqualTo(AuthorizationStatus.FRESHNESS_UNKNOWN);
            assertThat(business.securityFresh().getAsBoolean()).isFalse();

            // A page not signed by the contract's source key is refused.
            var forged = java.security.KeyPairGenerator.getInstance("Ed25519").generateKeyPair().getPrivate();
            assertThat(security.apply(page(forged, wall.instant(), revoked.userId()), elapsed.get())).isFalse();
            assertThat(business.securityFresh().getAsBoolean()).isFalse();

            assertThat(security.apply(page(keys.source.getPrivate(), wall.instant(), revoked.userId()), elapsed.get()))
                    .isTrue();
            assertThat(business.securityFresh().getAsBoolean()).isTrue();
            assertThat(business.cachedSecurity().apply(allowed, wall.instant())).isEqualTo(AuthorizationStatus.ALLOWED);
            assertThat(business.cachedSecurity().apply(revoked, wall.instant())).isEqualTo(AuthorizationStatus.REVOKED);

            wall.advance(Duration.ofMillis(4999));
            elapsed.addAndGet(Duration.ofMillis(4999).toNanos());
            assertThat(business.cachedSecurity().apply(allowed, wall.instant())).isEqualTo(AuthorizationStatus.ALLOWED);
            wall.advance(Duration.ofMillis(1));
            elapsed.addAndGet(Duration.ofMillis(1).toNanos());
            assertThat(business.cachedSecurity().apply(allowed, wall.instant()))
                    .isEqualTo(AuthorizationStatus.FRESHNESS_UNKNOWN);
            assertThat(business.securityFresh().getAsBoolean()).isFalse();

            // A draining source closes freshness even over a fresh cache.
            assertThat(security.apply(page(keys.source.getPrivate(), wall.instant(), revoked.userId(), 1), elapsed.get()))
                    .isTrue();
            assertThat(business.securityFresh().getAsBoolean()).isTrue();
            retireSources(r.ctx());
            assertThat(business.securityFresh().getAsBoolean()).isFalse();
        } finally {
            r.ctx().close();
        }
    }

    @Test
    void refusesAContractIdentityThatDiffersFromTheEffectiveOne() throws Exception {
        var c = contract();
        assertThat(install(c).getBeanDefinitionCount()).as("matching twin").isEqualTo(REGISTERED.size());
        for (var edit :
                List.<Consumer<MockEnvironment>>of(
                        env -> env.setProperty("SIGNALING_IDENTITY_ISSUER", "TEST_ONLY_OTHER_ISSUER"),
                        env -> env.setProperty("SIGNALING_IDENTITY_AUDIENCE", "TEST_ONLY_OTHER_AUDIENCE"))) {
            var env = environment();
            edit.accept(env);
            var ctx = context(env);
            assertFailure(
                    () -> LocalControlInstaller.install(ctx, c, Clock.systemUTC(), System::nanoTime, NO_OPEN),
                    Role.IDENTITY,
                    Category.IDENTITY_MISMATCH);
            assertThat(ctx.getBeanDefinitionCount()).isZero();
        }
    }
}
