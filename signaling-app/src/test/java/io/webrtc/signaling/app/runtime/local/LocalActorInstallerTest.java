package io.webrtc.signaling.app.runtime.local;

import io.webrtc.signaling.actors.cluster.ClusterReadiness;
import io.webrtc.signaling.app.runtime.NativeActorBusinessEnrollment;
import io.webrtc.signaling.app.runtime.NativeActorIngressEnrollment;
import io.webrtc.signaling.app.runtime.NativeActorMaintenanceEnrollment;
import io.webrtc.signaling.app.runtime.NativeActorProcess;
import io.webrtc.signaling.app.runtime.NativeActorProcessEnrollment;
import io.webrtc.signaling.app.runtime.NativeActorSchedulingEnrollment;
import io.webrtc.signaling.app.runtime.NativeActorSourceEnrollment;
import io.webrtc.signaling.app.runtime.NativeCellRpcEnrollment;
import io.webrtc.signaling.app.runtime.NativeCompatibilityObservationEnrollment;
import io.webrtc.signaling.app.runtime.NativeControlDatabaseEnrollment;
import io.webrtc.signaling.app.runtime.NativeTelemetry;
import io.webrtc.signaling.app.runtime.NativeWalEnrollment;
import io.webrtc.signaling.app.runtime.NativeWorkerScheduler;
import io.webrtc.signaling.app.runtime.local.LocalRuntimeFailure.Category;
import io.webrtc.signaling.app.runtime.local.LocalRuntimeFailure.Role;
import io.webrtc.signaling.auth.BoundedTokenVerifier;
import io.webrtc.signaling.protocol.Identity.UserId;
import io.webrtc.signaling.rpc.CellRpcClient;
import io.webrtc.signaling.rpc.CellRpcServer;
import io.webrtc.signaling.rpc.GatewayRelayRpcClient;
import io.webrtc.signaling.rpc.NativeSessionHandler;
import io.webrtc.signaling.rpc.ProofBindings;
import io.webrtc.signaling.storage.DbBoundary;
import io.webrtc.signaling.storage.DbOperation;
import io.webrtc.signaling.storage.DbPools;
import io.webrtc.signaling.storage.SqlTransactions;
import org.apache.pekko.Done;
import org.apache.pekko.actor.typed.ActorSystem;
import org.assertj.core.api.ThrowableAssert.ThrowingCallable;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.beans.factory.support.AbstractBeanDefinition;
import org.springframework.context.support.GenericApplicationContext;

import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyPairGenerator;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Function;

import static io.webrtc.signaling.app.runtime.local.LocalActorFixture.BOOT_C001;
import static io.webrtc.signaling.app.runtime.local.LocalActorFixture.BOOT_C002;
import static io.webrtc.signaling.app.runtime.local.LocalTestPki.ACTOR_URI;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class LocalActorInstallerTest {
    /** Exactly the definitions the installer registers, one per type. */
    static final List<Class<?>> REGISTERED =
            List.of(
                    NativeActorProcessEnrollment.class,
                    NativeActorSourceEnrollment.class,
                    NativeWalEnrollment.class,
                    NativeActorMaintenanceEnrollment.class,
                    NativeCompatibilityObservationEnrollment.class,
                    NativeCellRpcEnrollment.class,
                    ClusterReadiness.class,
                    SqlTransactions.class,
                    DbBoundary.class,
                    DbPools.class,
                    BoundedTokenVerifier.class,
                    NativeActorBusinessEnrollment.class,
                    NativeActorSchedulingEnrollment.class,
                    NativeActorIngressEnrollment.class);

    @TempDir
    static Path pki;
    static LocalTestPki authority;
    static LocalTestPki.Leaf actor;

    @TempDir
    Path mount;

    @BeforeAll
    static void authority() throws Exception {
        authority = LocalTestPki.create(pki);
        actor = authority.leaf(pki, "actor", "URI:" + ACTOR_URI + ",DNS:localhost");
    }

    LocalRuntimeContract contract(
            Consumer<Map<String, Object>> profileEdit, Consumer<Map<String, Object>> snapshotEdit)
            throws Exception {
        authority.install(mount, actor);
        LocalTestKeys.install(mount);
        return LocalActorFixture.write(mount, profileEdit, snapshotEdit);
    }

    LocalRuntimeContract contract() throws Exception {
        return contract(p -> {
        }, s -> {
        });
    }

    static GenericApplicationContext context(LocalRuntimeContract c) {
        var ctx = new GenericApplicationContext();
        var env = LocalPekkoConfigTest.environment();
        env.setProperty("SIGNALING_CELL_ID", c.contract().cell());
        env.setProperty("SIGNALING_IDENTITY_ISSUER", "LOCAL_TEST_ONLY_ISSUER");
        env.setProperty("SIGNALING_IDENTITY_AUDIENCE", "LOCAL_TEST_ONLY_AUDIENCE");
        ctx.setEnvironment(env);
        return ctx;
    }

    @Test
    void refusesAContractIdentityThatDiffersFromTheEffectiveOne() throws Exception {
        var c = contract();
        assertThat(install(c).getBeanDefinitionCount()).as("matching twin").isEqualTo(REGISTERED.size());
        for (var name : List.of("SIGNALING_IDENTITY_ISSUER", "SIGNALING_IDENTITY_AUDIENCE")) {
            var ctx = context(c);
            ((org.springframework.mock.env.MockEnvironment) ctx.getEnvironment()).setProperty(name, "TEST_ONLY_OTHER");
            assertFailure(
                    () -> LocalActorInstaller.install(ctx, c, LocalPekkoConfigTest.osEnvironment()::get, d -> null),
                    Role.IDENTITY,
                    Category.IDENTITY_MISMATCH);
            assertThat(ctx.getBeanDefinitionCount()).as(name).isZero();
        }
        var missing = new GenericApplicationContext();
        missing.setEnvironment(LocalPekkoConfigTest.environment().withProperty("SIGNALING_CELL_ID", "c001"));
        assertFailure(
                () -> LocalActorInstaller.install(missing, c, LocalPekkoConfigTest.osEnvironment()::get, d -> null),
                Role.IDENTITY,
                Category.IDENTITY_MISMATCH);
        assertThat(missing.getBeanDefinitionCount()).isZero();
    }

    /** Installs into a fresh, unrefreshed context; nothing is instantiated. */
    static GenericApplicationContext install(
            LocalRuntimeContract c,
            Function<NativeControlDatabaseEnrollment.Database, SqlTransactions> opener) {
        var ctx = context(c);
        LocalActorInstaller.install(ctx, c, LocalPekkoConfigTest.osEnvironment()::get, opener);
        return ctx;
    }

    static GenericApplicationContext install(LocalRuntimeContract c) {
        return install(
                c,
                database -> {
                    throw new AssertionError("TEST_ONLY: no database is opened during install");
                });
    }

    static String name(GenericApplicationContext ctx, Class<?> type) {
        var names = ctx.getBeanFactory().getBeanNamesForType(type, true, false);
        assertThat(names).as(type.getName()).hasSize(1);
        return names[0];
    }

    /** Runs the definition's own supplier, outside any refresh. */
    static <T> T instance(GenericApplicationContext ctx, Class<T> type) {
        var definition = (AbstractBeanDefinition) ctx.getBeanFactory().getBeanDefinition(name(ctx, type));
        return type.cast(definition.getInstanceSupplier().get());
    }

    static void assertFailure(ThrowingCallable action, Role role, Category category) {
        assertThatThrownBy(action)
                .isInstanceOfSatisfying(
                        LocalRuntimeFailure.class,
                        failure -> {
                            assertThat(failure.role()).isEqualTo(role);
                            assertThat(failure.category()).isEqualTo(category);
                        });
    }

    /**
     * The installer's context refreshed with every supplier run. Only collaborators that would need
     * a cluster or a database are stood in by mocks: the process (its ActorSystem), the cell RPC
     * client, telemetry and the opened SQL owner.
     */
    record Refreshed(
            GenericApplicationContext ctx,
            List<NativeControlDatabaseEnrollment.Database> opened,
            ActorSystem<Void> system,
            CompletableFuture<Done> terminated,
            DbBoundary boundary,
            DbPools pools) {
    }

    @SuppressWarnings("unchecked")
    Refreshed refreshed(LocalRuntimeContract c) {
        var opened = new ArrayList<NativeControlDatabaseEnrollment.Database>();
        var boundary = mock(DbBoundary.class);
        var pools = mock(DbPools.class);
        var sql = mock(SqlTransactions.class);
        when(sql.boundary()).thenReturn(boundary);
        when(sql.pools()).thenReturn(pools);
        var ctx =
                install(
                        c,
                        database -> {
                            opened.add(database);
                            return sql;
                        });
        assertThat(opened).as("install itself opens nothing").isEmpty();
        ActorSystem<Void> system = mock(ActorSystem.class);
        var terminated = new CompletableFuture<Done>();
        when(system.getWhenTerminated()).thenReturn(terminated);
        var process = mock(NativeActorProcess.class);
        doReturn(system).when(process).system();
        var telemetry = mock(NativeTelemetry.class);
        when(telemetry.scrape()).thenReturn("TEST_ONLY_METRICS");
        var client = mock(CellRpcClient.class);
        Consumer<org.springframework.beans.factory.config.BeanDefinition> ownedElsewhere =
                definition -> definition.setDestroyMethodName("");
        ctx.registerBean(NativeActorProcess.class, () -> process, ownedElsewhere::accept);
        ctx.registerBean(NativeTelemetry.class, () -> telemetry, ownedElsewhere::accept);
        ctx.registerBean(CellRpcClient.class, () -> client, ownedElsewhere::accept);
        ctx.refresh();
        return new Refreshed(ctx, opened, system, terminated, boundary, pools);
    }

    static UserId userInBucket(int first, int last) {
        for (int i = 0; ; i++) {
            var user = new UserId("TEST_ONLY_user_" + i);
            int bucket = io.webrtc.signaling.storage.SessionRegistryService.bucket(user);
            if (bucket >= first && bucket <= last) return user;
        }
    }

    @Test
    void registersExactlyTheActorDefinitionsWithTheirOwners() throws Exception {
        var ctx = install(contract());
        var factory = ctx.getBeanFactory();
        assertThat(factory.getBeanDefinitionNames())
                .containsExactlyInAnyOrderElementsOf(REGISTERED.stream().map(Class::getName).toList());
        for (var type : REGISTERED) assertThat(factory.getType(name(ctx, type), false)).isEqualTo(type);

        // The verifier's worker pool is closed by the context; the native runtime hooks drain the
        // boundary and close the pools, so the context must not.
        assertThat(destroy(ctx, BoundedTokenVerifier.class)).isEqualTo("close");
        assertThat(destroy(ctx, DbBoundary.class)).isEmpty();
        assertThat(destroy(ctx, DbPools.class)).isEmpty();
        for (var type :
                List.of(
                        NativeActorBusinessEnrollment.class,
                        NativeActorSchedulingEnrollment.class,
                        NativeActorIngressEnrollment.class,
                        SqlTransactions.class))
            assertThat(destroy(ctx, type)).as(type.getName()).isNull();
    }

    static String destroy(GenericApplicationContext ctx, Class<?> type) {
        return ctx.getBeanFactory().getBeanDefinition(name(ctx, type)).getDestroyMethodName();
    }

    @Test
    void refreshesEveryActorBeanWithoutAnActorSystem() throws Exception {
        var c = contract();
        var r = refreshed(c);
        var ctx = r.ctx();
        var tokens = ctx.getBean(BoundedTokenVerifier.class);
        try {
            var business = ctx.getBean(NativeActorBusinessEnrollment.class);
            assertThat(business.system()).isSameAs(r.system());
            assertThat(business.tokens()).isSameAs(tokens);
            assertThat(business.cell()).isEqualTo("c001");
            assertThat(business.podUid()).isEqualTo(LocalRuntimeFixtures.POD_UID);
            assertThat(business.epochAdoption()).isSameAs(LocalActorInstaller.NO_EPOCH_ADOPTION);
            assertThat(business.homes().apply(userInBucket(8192, 16383)))
                    .isEqualTo(new ProofBindings.TrustedHome("c002", 3, 1));
            assertThat(business.callingPolicy()).as("Main's enrolled policy is used").isNull();

            var scheduling = ctx.getBean(NativeActorSchedulingEnrollment.class);
            assertThat(scheduling.system()).isSameAs(r.system());
            // The cell's enrolled zones, not only this actor's own: other-zone peers must count.
            assertThat(scheduling.azRoles()).containsExactlyInAnyOrder("az-a", "az-b");
            assertThat(scheduling.fingerprint()).isEqualTo(LocalPekkoConfigTest.FINGERPRINT);
            assertThat(scheduling.maintenance())
                    .extracting(NativeWorkerScheduler.Job::name)
                    .containsExactly(LocalZoneReport.JOB);
            assertThat(scheduling.healthAddress()).isEqualTo(new InetSocketAddress("192.0.2.10", 9090));
            assertThat(scheduling.healthAddress().isUnresolved()).isFalse();
            assertThat(scheduling.metrics().get()).isEqualTo("TEST_ONLY_METRICS");
            assertThat(scheduling.live().getAsBoolean()).isTrue();
            r.terminated().complete(Done.getInstance());
            assertThat(scheduling.live().getAsBoolean()).as("a terminated system is not live").isFalse();

            var ingress = ctx.getBean(NativeActorIngressEnrollment.class);
            try {
                assertThat(ingress.environment()).isEqualTo("local-test-only");
                assertThat(ingress.port()).isEqualTo(8443);
                assertThat(ingress.admission()).isEqualTo(LocalActorInstaller.ADMISSION);
                assertThat(ingress.relayCapacity()).isEqualTo(LocalActorInstaller.RELAY_CAPACITY);
                assertThat(ingress.delivery()).isNull();
                assertThat(ingress.network()).isNotNull();
                assertThat(ingress.gatewayWorkloads().test(peer("c001", "gateway", "local-c001"), identity("c001", "local-c001")))
                        .isTrue();
                assertThat(ingress.gatewayWorkloads().test(peer("c001", "actor", "local-c001"), identity("c001", "local-c001")))
                        .isFalse();
            } finally {
                ingress.gateways().drain().toCompletableFuture().get(5, TimeUnit.SECONDS);
            }

            assertThat(ctx.getBean(DbBoundary.class)).isSameAs(r.boundary());
            assertThat(ctx.getBean(DbPools.class)).isSameAs(r.pools());

            // The installer's own objects; stand-ins are the test's mocks only.
            for (var type :
                    List.of(
                            NativeActorProcessEnrollment.class,
                            NativeActorSourceEnrollment.class,
                            NativeWalEnrollment.class,
                            NativeActorMaintenanceEnrollment.class,
                            NativeCompatibilityObservationEnrollment.class,
                            NativeCellRpcEnrollment.class,
                            ClusterReadiness.class,
                            BoundedTokenVerifier.class,
                            NativeActorBusinessEnrollment.class,
                            NativeActorSchedulingEnrollment.class,
                            NativeActorIngressEnrollment.class))
                assertThat(ctx.getBean(type).getClass()).as(type.getName()).isEqualTo(type);
            assertThat(ingress.gateways().getClass()).isEqualTo(GatewayRelayRpcClient.class);
        } finally {
            ctx.close();
        }
        // Closing the context closes the verifier and nothing the native hooks own.
        assertThat(tokens.drain().toCompletableFuture()).isCompleted();
        assertThatThrownBy(() -> tokens.verify("a.b.c", Instant.now()).toCompletableFuture().get(1, TimeUnit.SECONDS))
                .hasCauseInstanceOf(java.util.concurrent.RejectedExecutionException.class);
        verify(r.boundary(), never()).close();
        verify(r.boundary(), never()).drain();
        verify(r.pools(), never()).close();
    }

    @Test
    void buildsNoSecondCellCredential() throws Exception {
        var c = contract();
        assertThat(c.contract().directoryDatabase().jdbcUrl()).isEqualTo(LocalActorFixture.DIRECTORY_URL);
        var r = refreshed(c);
        try {
            // Every supplier has run: the only database ever opened is this cell's.
            assertThat(r.opened()).containsExactly(c.contract().cellDatabase());
            assertThat(r.opened())
                    .noneMatch(d -> d.jdbcUrl().equals(LocalActorFixture.DIRECTORY_URL));
            assertThat(c.profile().transferCells()).isEmpty();
        } finally {
            r.ctx().close();
        }
        // An actor profile cannot carry other cells' credentials at all.
        assertFailure(
                () ->
                        contract(
                                p -> p.put("transferCells", List.of(LocalRuntimeFixtures.transferCell("c002", 1))),
                                s -> {
                                }),
                Role.PROFILE,
                Category.INVALID);
    }

    @Test
    void readinessIsTheSingleHostLocalOne() throws Exception {
        var ctx = install(contract());
        var readiness = instance(ctx, ClusterReadiness.class);
        var oneDomain = new ClusterReadiness.Snapshot(true, true, true, true, true, true, 4, 1, false);
        readiness.update(oneDomain);
        assertThat(readiness.businessReady()).as("one failure domain suffices locally").isTrue();
        var production = new ClusterReadiness();
        production.update(oneDomain);
        assertThat(production.businessReady()).isFalse();
        // Quorum and safety are still mandatory.
        readiness.update(new ClusterReadiness.Snapshot(true, true, true, true, true, true, 3, 1, false));
        assertThat(readiness.businessReady()).isFalse();
    }

    @Test
    void enrollmentsCarryTheContractAndProfileValues() throws Exception {
        var c = contract();
        var ctx = install(c);

        var wal = instance(ctx, NativeWalEnrollment.class);
        assertThat(wal.localSingleWriter()).isFalse();
        assertThat(wal.cell()).isEqualTo("c001");
        assertThat(wal.replicas()).containsExactly("replica-a");
        assertThat(wal.synchronousReplicas()).isEqualTo(1);

        var source = instance(ctx, NativeActorSourceEnrollment.class);
        assertThat(source.podUid()).isEqualTo(LocalRuntimeFixtures.POD_UID);
        assertThat(source.processBoot()).isEqualTo(c.processBoot());
        assertThat(source.clock().uri().toString()).isEqualTo("https://source.platform.svc/v1/c001/clock");
        assertThat(source.revocations().uri().toString())
                .isEqualTo("https://source.platform.svc/v1/c001/revocations");
        assertThat(source.clock().keys()).containsOnlyKeys("source").isEqualTo(source.revocations().keys());

        var cells = instance(ctx, NativeCellRpcEnrollment.class);
        assertThat(cells.environment()).isEqualTo("local-test-only");
        assertThat(cells.destinations()).containsOnlyKeys("c001", "c002");
        assertThat(cells.destinations().get("c002").tlsAuthority()).isEqualTo("actor.c002.svc");

        var observation = instance(ctx, NativeCompatibilityObservationEnrollment.class);
        assertThat(observation.cells()).containsExactlyInAnyOrder("c001", "c002");
        assertThat(observation.gateways())
                .containsExactlyInAnyOrder(
                        new GatewayRelayRpcClient.Target("c001", "local-c001", BOOT_C001),
                        new GatewayRelayRpcClient.Target("c002", "local-c002", BOOT_C002));
        assertThat(observation.installedBuild().fingerprint()).isEqualTo(LocalPekkoConfigTest.FINGERPRINT);

        var maintenance = instance(ctx, NativeActorMaintenanceEnrollment.class);
        assertThat(maintenance.privacyPseudonymKey()).isEqualTo(Files.readAllBytes(mount.resolve("privacy.key")));
        assertThat(maintenance.detachment()).isSameAs(LocalActorInstaller.UNENROLLED_DETACHMENT);

        assertThat(instance(ctx, NativeActorProcessEnrollment.class).systemName()).isEqualTo("signaling-c001");
    }

    static CellRpcServer.Peer peer(String cell, String role, String workload) {
        return new CellRpcServer.Peer(cell, role, workload);
    }

    static NativeSessionHandler.GatewayIdentity identity(String cell, String gateway) {
        return new NativeSessionHandler.GatewayIdentity(gateway, UUID.randomUUID(), cell, 1, "TEST_ONLY");
    }

    @Test
    void gatewayWorkloadAdmissionIsOwnIdentityOnly() throws Exception {
        var admits = LocalActorInstaller.workloads(contract().contract());
        assertThat(admits.test(peer("c001", "gateway", "local-c001"), identity("c001", "local-c001"))).isTrue();
        assertThat(admits.test(peer("c002", "gateway", "local-c002"), identity("c002", "local-c002"))).isTrue();

        assertThat(admits.test(peer("c001", "actor", "local-c001"), identity("c001", "local-c001")))
                .as("wrong role")
                .isFalse();
        assertThat(admits.test(peer("c001", "Gateway", "local-c001"), identity("c001", "local-c001")))
                .as("role is exact")
                .isFalse();
        assertThat(admits.test(peer("c002", "gateway", "local-c001"), identity("c001", "local-c001")))
                .as("other cell")
                .isFalse();
        assertThat(admits.test(peer("c001", "gateway", "local-c002"), identity("c001", "local-c001")))
                .as("another gateway's workload")
                .isFalse();
        assertThat(admits.test(peer("c001", "gateway", "local-c002"), identity("c001", "local-c002")))
                .as("a workload the contract does not enroll in this cell")
                .isFalse();
        assertThat(admits.test(peer("c001", "gateway", "local-x"), identity("c001", "local-x")))
                .as("unenrolled")
                .isFalse();
        assertThat(admits.test(null, identity("c001", "local-c001"))).as("no peer").isFalse();
        assertThat(admits.test(peer("c001", "gateway", "local-c001"), null)).as("no identity").isFalse();
    }

    @Test
    void relayDestinationsAreTheContractGatewaysAtTheirPinnedBoots() throws Exception {
        var destinations = LocalActorInstaller.relayDestinations(LocalDirectorySnapshot.load(contract()));
        assertThat(destinations)
                .isEqualTo(
                        Map.of(
                                new GatewayRelayRpcClient.Target("c001", "local-c001", BOOT_C001),
                                new CellRpcClient.Endpoint("gateway.c001.svc", 9443, "gateway.c001.svc"),
                                new GatewayRelayRpcClient.Target("c002", "local-c002", BOOT_C002),
                                new CellRpcClient.Endpoint("gateway.c002.svc", 9443, "gateway.c002.svc")));
        assertThatThrownBy(() -> destinations.put(null, null)).isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void epochAdoptionIsAlwaysRefused() {
        var verifier = LocalActorInstaller.NO_EPOCH_ADOPTION;
        assertThat(verifier.permits("c001", 1, 2)).isFalse();
        assertThat(verifier.permits("c001", 1, 1)).isFalse();
        assertThat(verifier.permits("c002", 5, 6)).isFalse();
        assertThat(verifier.permits("c001", 2, 1)).isFalse();
    }

    @Test
    void detachmentNeverLetsTheWorkerCompleteARequest() throws Exception {
        var operation = LocalActorInstaller.UNENROLLED_DETACHMENT.detach("00ff");
        // PrivacyDeletionWorker marks a request complete only in thenCompose on a successful
        // logical stage; a failed stage goes to exceptionally(false) and the row stays pending.
        var marked = new AtomicBoolean();
        var outcome =
                operation
                        .logical()
                        .thenCompose(
                                detached -> {
                                    marked.set(true);
                                    return CompletableFuture.completedFuture(true);
                                })
                        .exceptionally(error -> false)
                        .toCompletableFuture()
                        .get(1, TimeUnit.SECONDS);
        assertThat(outcome).isFalse();
        assertThat(marked).isFalse();
        assertThat(operation.logical().toCompletableFuture()).isCompletedExceptionally();
        assertThat(operation.physicalCompletion().toCompletableFuture().get(1, TimeUnit.SECONDS))
                .isEqualTo(DbOperation.PhysicalCompletion.NOT_STARTED);
    }

    @Test
    void workerOutcomesAreReportedWithoutFailing() {
        for (var status : NativeWorkerScheduler.Status.values())
            assertThatCode(
                            () ->
                                    LocalActorInstaller.workerEvent(
                                            new NativeWorkerScheduler.Event("control_outbox", status, 1)))
                    .doesNotThrowAnyException();
    }

    @Test
    void badJwtKeysFailBeforeAnyDefinitionIsRegistered() throws Exception {
        var c = contract();
        var small = KeyPairGenerator.getInstance("RSA");
        small.initialize(1024);
        LocalTestKeys.write(mount.resolve("rsa.pub"), "PUBLIC KEY", small.generateKeyPair().getPublic().getEncoded());
        var ctx = context(c);
        assertFailure(
                () ->
                        LocalActorInstaller.install(
                                ctx,
                                c,
                                LocalPekkoConfigTest.osEnvironment()::get,
                                d -> {
                                    throw new AssertionError();
                                }),
                Role.CONTRACT,
                Category.INVALID);
        assertThat(ctx.getBeanDefinitionCount()).isZero();

        var ed = KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
        LocalTestKeys.write(mount.resolve("rsa.pub"), "PUBLIC KEY", ed.getPublic().getEncoded());
        assertFailure(() -> install(c), Role.CONTRACT, Category.INVALID);
    }

    @Test
    void refusesASnapshotWhoseHashDiffersBeforeRegistering() throws Exception {
        var c = contract();
        Files.writeString(
                mount.resolve("directory.json"),
                LocalRuntimeFixtures.JSON.writeValueAsString(LocalActorFixture.snapshot()) + " ");
        var ctx = context(c);
        assertFailure(
                () -> LocalActorInstaller.install(ctx, c, LocalPekkoConfigTest.osEnvironment()::get, d -> null),
                Role.CONTRACT,
                Category.INVALID);
        assertThat(ctx.getBeanDefinitionCount()).isZero();
    }

    @Test
    void refusesAnActorWhoseCellZonesAreMissingOrOmitItsOwnZone() throws Exception {
        for (var zones :
                List.<List<String>>of(
                        List.of(), List.of("az-a"), List.of("az-a", "az-a"), List.of("az-b", "az-c"),
                        List.of("az-a", "az-b", "az-c", "az-d"), List.of("az-a", "AZ-B")))
            assertFailure(
                    () -> install(contract(p -> p.put("cellAzRoles", zones), s -> {
                    })),
                    Role.PROFILE,
                    Category.INVALID);
    }

    @Test
    void refusesWhenWalReplicaSetIsEmpty() throws Exception {
        assertFailure(
                () ->
                        install(
                                contract(
                                        p -> {
                                            @SuppressWarnings("unchecked")
                                            var wal = (Map<String, Object>) p.get("wal");
                                            wal.put("replicas", List.of());
                                            wal.put("synchronousReplicas", 0);
                                        },
                                        s -> {
                                        })),
                Role.PROFILE,
                Category.INVALID);
        assertFailure(
                () ->
                        install(
                                contract(
                                        p -> {
                                            @SuppressWarnings("unchecked")
                                            var wal = (Map<String, Object>) p.get("wal");
                                            wal.put("synchronousReplicas", 0);
                                        },
                                        s -> {
                                        })),
                Role.PROFILE,
                Category.INVALID);
    }

    @Test
    void privacyKeyDefectsAreProfileInvalidAndFileDefectsArePathFailures() throws Exception {
        assertFailure(
                () -> LocalActorInstaller.privacyKey(contract(p -> p.put("privacyPseudonymKey", null), s -> {
                })),
                Role.PROFILE,
                Category.INVALID);
        var c = contract();
        var key = mount.resolve("privacy.key");
        for (int length : new int[] {0, 31, 65, 4096}) {
            Files.write(key, new byte[length]);
            assertFailure(() -> LocalActorInstaller.privacyKey(c), Role.PROFILE, Category.INVALID);
        }
        for (int length : new int[] {32, 64}) {
            Files.write(key, new byte[length]);
            assertThat(LocalActorInstaller.privacyKey(c)).hasSize(length);
        }
        Files.write(key, new byte[31]);
        assertFailure(() -> install(c), Role.PROFILE, Category.INVALID);
        Files.delete(key);
        assertFailure(() -> LocalActorInstaller.privacyKey(c), Role.PATH, Category.MISSING);
    }
}
