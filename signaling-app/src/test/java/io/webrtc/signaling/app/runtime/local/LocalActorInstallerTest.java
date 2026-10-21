package io.webrtc.signaling.app.runtime.local;

import io.webrtc.signaling.actors.cluster.ClusterReadiness;
import io.webrtc.signaling.app.runtime.NativeActorBusinessEnrollment;
import io.webrtc.signaling.app.runtime.NativeActorIngressEnrollment;
import io.webrtc.signaling.app.runtime.NativeActorMaintenanceEnrollment;
import io.webrtc.signaling.app.runtime.NativeActorProcessEnrollment;
import io.webrtc.signaling.app.runtime.NativeActorSchedulingEnrollment;
import io.webrtc.signaling.app.runtime.NativeActorSourceEnrollment;
import io.webrtc.signaling.app.runtime.NativeCellRpcEnrollment;
import io.webrtc.signaling.app.runtime.NativeCompatibilityEnrollment;
import io.webrtc.signaling.app.runtime.NativeCompatibilityObservationEnrollment;
import io.webrtc.signaling.app.runtime.NativeControlDatabaseEnrollment;
import io.webrtc.signaling.app.runtime.NativeRecoveryEnrollment;
import io.webrtc.signaling.app.runtime.NativeWalEnrollment;
import io.webrtc.signaling.app.runtime.local.LocalRuntimeFailure.Category;
import io.webrtc.signaling.app.runtime.local.LocalRuntimeFailure.Role;
import io.webrtc.signaling.auth.BoundedTokenVerifier;
import io.webrtc.signaling.protocol.Identity.UserId;
import io.webrtc.signaling.rpc.GatewayRelayRpcClient;
import io.webrtc.signaling.rpc.ProofBindings;
import io.webrtc.signaling.storage.DbBoundary;
import io.webrtc.signaling.storage.DbPools;
import io.webrtc.signaling.storage.SessionRegistryService;
import io.webrtc.signaling.storage.SqlTransactions;
import org.assertj.core.api.ThrowableAssert.ThrowingCallable;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.beans.factory.support.AbstractBeanDefinition;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.mock.env.MockEnvironment;

import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.function.Function;

import static io.webrtc.signaling.app.runtime.local.LocalRuntimeFixtures.JSON;
import static io.webrtc.signaling.app.runtime.local.LocalTestPki.ACTOR_URI;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class LocalActorInstallerTest {
    static final UUID GATEWAY_BOOT = UUID.fromString("00000000-0000-4000-8000-0000000000b1");

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

    /** A complete actor mount; {@code profileEdit} and {@code snapshotEdit} adjust the documents. */
    LocalRuntimeContract contract(
            Consumer<Map<String, Object>> profileEdit, Consumer<Map<String, Object>> snapshotEdit)
            throws Exception {
        var env = LocalRuntimeFixtures.install(mount, "ACTOR");
        authority.install(mount, actor);
        LocalTestKeys.install(mount);
        var key = new byte[32];
        new java.security.SecureRandom().nextBytes(key);
        Files.write(mount.resolve("privacy.key"), key);

        var profile = LocalRuntimeFixtures.profile(mount, "ACTOR");
        @SuppressWarnings("unchecked")
        var compatibility = (Map<String, Object>) profile.get("compatibility");
        compatibility.put("major", 1);
        compatibility.put("minor", 2);
        compatibility.put("executableId", LocalPekkoConfigTest.EXECUTABLE);
        compatibility.put("features", List.of("session-s1", "full-connection-active"));
        compatibility.put("lowerMinor", 1);
        compatibility.put("upperMinor", 2);
        profile.put("privacyPseudonymKey", mount.resolve("privacy.key").toString());
        profileEdit.accept(profile);
        Files.writeString(mount.resolve("process.json"), JSON.writeValueAsString(profile));

        var snapshot = snapshot();
        snapshotEdit.accept(snapshot);
        var bytes = JSON.writeValueAsBytes(snapshot);
        Files.write(mount.resolve("directory.json"), bytes);

        var contract = LocalRuntimeFixtures.contract(mount);
        // A distinct directory credential, so a test can tell the two databases apart.
        var directory = LocalRuntimeFixtures.database("c001");
        directory.put(
                "jdbcUrl", "jdbc:postgresql://directory.platform.svc:5432/directory?sslmode=verify-full");
        directory.put("username", "directory");
        contract.put("directoryDatabase", directory);
        contract.put(
                "cells",
                List.of(
                        cell("c001", "actor.c001.svc"),
                        cell("c002", "actor.c002.svc")));
        contract.put(
                "directorySnapshotSha256",
                HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes)));
        Files.writeString(mount.resolve("provider.json"), JSON.writeValueAsString(contract));
        return LocalRuntimeContract.load(env);
    }

    LocalRuntimeContract contract() throws Exception {
        return contract(p -> {
        }, s -> {
        });
    }

    static Map<String, Object> cell(String cell, String host) {
        return Map.of("cell", cell, "host", host, "port", 8443, "tlsAuthority", host, "storageEpoch", 1);
    }

    static Map<String, Object> snapshot() {
        var document = new LinkedHashMap<String, Object>();
        document.put("schemaVersion", 1);
        document.put("qualification", "LOCAL_TEST_ONLY");
        document.put(
                "buckets",
                new ArrayList<>(
                        List.of(
                                range(0, 8191, "c001"),
                                range(8192, 16383, "c002"))));
        document.put(
                "gatewayBoots",
                new ArrayList<>(
                        List.of(
                                Map.of(
                                        "cell", "c001",
                                        "gatewayId", "local-c001",
                                        "bootId", GATEWAY_BOOT.toString()))));
        return document;
    }

    static Map<String, Object> range(int first, int last, String cell) {
        return Map.of(
                "first", first, "last", last, "cell", cell, "directoryEpoch", 3, "storageEpoch", 1);
    }

    static MockEnvironment environment(LocalRuntimeContract c) {
        var env = LocalPekkoConfigTest.environment();
        env.setProperty("SIGNALING_CELL_ID", c.contract().cell());
        return env;
    }

    /** Installs into a fresh, unrefreshed context; nothing is instantiated. */
    GenericApplicationContext install(
            LocalRuntimeContract c,
            Function<NativeControlDatabaseEnrollment.Database, SqlTransactions> opener) {
        var ctx = new GenericApplicationContext();
        ctx.setEnvironment(environment(c));
        LocalActorInstaller.install(ctx, c, LocalPekkoConfigTest.osEnvironment()::get, opener);
        return ctx;
    }

    GenericApplicationContext install(LocalRuntimeContract c) {
        return install(
                c,
                database -> {
                    throw new AssertionError("TEST_ONLY: no database is opened during install");
                });
    }

    static <T> T instance(GenericApplicationContext ctx, Class<T> type) {
        var names = ctx.getBeanFactory().getBeanNamesForType(type, true, false);
        assertThat(names).hasSize(1);
        var definition = (AbstractBeanDefinition) ctx.getBeanFactory().getBeanDefinition(names[0]);
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

    static UserId userInBucket(int first, int last) {
        for (int i = 0; ; i++) {
            var user = new UserId("TEST_ONLY_user_" + i);
            int bucket = SessionRegistryService.bucket(user);
            if (bucket >= first && bucket <= last) return user;
        }
    }

    @Test
    void registersEveryActorEnrollmentExactlyOnce() throws Exception {
        var ctx = install(contract());
        var factory = ctx.getBeanFactory();
        for (var type :
                List.of(
                        NativeActorProcessEnrollment.class,
                        NativeActorSourceEnrollment.class,
                        NativeActorBusinessEnrollment.class,
                        NativeActorIngressEnrollment.class,
                        NativeActorSchedulingEnrollment.class,
                        NativeActorMaintenanceEnrollment.class,
                        NativeWalEnrollment.class,
                        NativeCellRpcEnrollment.class,
                        NativeCompatibilityObservationEnrollment.class,
                        ClusterReadiness.class,
                        SqlTransactions.class,
                        DbBoundary.class,
                        DbPools.class,
                        BoundedTokenVerifier.class))
            assertThat(factory.getBeanNamesForType(type, true, false)).as(type.getName()).hasSize(1);
        // Built by the existing collectors from the observation enrollment, never from constants.
        assertThat(factory.getBeanNamesForType(NativeCompatibilityEnrollment.class, true, false)).isEmpty();
        // Recovery accounting is out of scope.
        assertThat(factory.getBeanNamesForType(NativeRecoveryEnrollment.class, true, false)).isEmpty();
    }

    @Test
    void ownersAreClosedByTheirNativeOwnerOrByTheContext() throws Exception {
        var ctx = install(contract());
        var factory = ctx.getBeanFactory();
        assertThat(destroy(ctx, BoundedTokenVerifier.class)).isEqualTo("close");
        // The native runtime hooks drain the boundary and close the pools, in order.
        assertThat(destroy(ctx, DbBoundary.class)).isEmpty();
        assertThat(destroy(ctx, DbPools.class)).isEmpty();
        assertThat(factory.getBeanDefinitionCount()).isPositive();
    }

    static String destroy(GenericApplicationContext ctx, Class<?> type) {
        var name = ctx.getBeanFactory().getBeanNamesForType(type, true, false)[0];
        return ctx.getBeanFactory().getBeanDefinition(name).getDestroyMethodName();
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

        var cells = instance(ctx, NativeCellRpcEnrollment.class);
        assertThat(cells.environment()).isEqualTo("local-test-only");
        assertThat(cells.destinations()).containsOnlyKeys("c001", "c002");
        assertThat(cells.destinations().get("c002").tlsAuthority()).isEqualTo("actor.c002.svc");

        var observation = instance(ctx, NativeCompatibilityObservationEnrollment.class);
        assertThat(observation.cells()).containsExactlyInAnyOrder("c001", "c002");
        assertThat(observation.gateways())
                .containsExactly(new GatewayRelayRpcClient.Target("c001", "local-c001", GATEWAY_BOOT));
        assertThat(observation.installedBuild().fingerprint()).isEqualTo(LocalPekkoConfigTest.FINGERPRINT);

        var maintenance = instance(ctx, NativeActorMaintenanceEnrollment.class);
        assertThat(maintenance.privacyPseudonymKey())
                .isEqualTo(Files.readAllBytes(mount.resolve("privacy.key")));

        assertThat(instance(ctx, NativeActorProcessEnrollment.class).systemName()).isEqualTo("signaling-c001");
    }

    @Test
    void buildsNoSecondCellCredential() throws Exception {
        var c = contract();
        var opened = new ArrayList<NativeControlDatabaseEnrollment.Database>();
        var ctx =
                install(
                        c,
                        database -> {
                            opened.add(database);
                            throw new IllegalStateException("TEST_ONLY_RECORDED");
                        });
        assertThat(opened).as("install itself opens nothing").isEmpty();

        assertThatThrownBy(() -> instance(ctx, SqlTransactions.class)).hasMessage("TEST_ONLY_RECORDED");
        assertThat(opened).containsExactly(c.contract().cellDatabase());
        assertThat(opened).doesNotContain(c.contract().directoryDatabase());
        assertThat(c.contract().cellDatabase()).isNotEqualTo(c.contract().directoryDatabase());
    }

    @Test
    void homesResolveFromSnapshotAndRejectUnknownBucket() throws Exception {
        var c =
                contract(
                        p -> {
                        },
                        s -> s.put("buckets", List.of(range(0, 8191, "c001"), range(8192, 12000, "c002"))));
        var homes = LocalDirectorySnapshot.load(c);

        assertThat(homes.home(userInBucket(0, 8191)))
                .isEqualTo(new ProofBindings.TrustedHome("c001", 3, 1));
        assertThat(homes.home(userInBucket(8192, 12000)))
                .isEqualTo(new ProofBindings.TrustedHome("c002", 3, 1));
        assertThat(homes.home(userInBucket(12001, 16383))).as("unknown bucket fails closed").isNull();
        // The installer itself accepts the snapshot.
        install(c);
    }

    @Test
    void refusesASnapshotWhoseHashDiffers() throws Exception {
        var c = contract();
        Files.writeString(mount.resolve("directory.json"), JSON.writeValueAsString(snapshot()) + " ");
        assertFailure(() -> LocalDirectorySnapshot.load(c), Role.CONTRACT, Category.INVALID);
        assertFailure(() -> install(c), Role.CONTRACT, Category.INVALID);
    }

    @Test
    void refusesSnapshotsThatDoNotMatchTheContract() throws Exception {
        assertFailure(
                () -> install(contract(p -> {
                }, s -> s.put("buckets", List.of(range(0, 100, "c999"))))),
                Role.CONTRACT,
                Category.INVALID);
        assertFailure(
                () -> install(contract(p -> {
                }, s -> s.put("buckets", List.of(range(0, 100, "c001"), range(100, 200, "c002"))))),
                Role.CONTRACT,
                Category.INVALID);
        assertFailure(
                () -> install(contract(p -> {
                }, s -> s.put("buckets", List.of(range(0, 16384, "c001"))))),
                Role.CONTRACT,
                Category.INVALID);
        assertFailure(
                () -> install(contract(p -> {
                }, s -> s.put("gatewayBoots", List.of()))),
                Role.CONTRACT,
                Category.INVALID);
        assertFailure(
                () -> install(contract(p -> {
                }, s -> s.put("unexpected", true))),
                Role.CONTRACT,
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
    void refusesAMissingOrShortPrivacyKey() throws Exception {
        assertFailure(
                () -> install(contract(p -> p.put("privacyPseudonymKey", null), s -> {
                })),
                Role.PROFILE,
                Category.INVALID);
        var c = contract();
        Files.write(mount.resolve("privacy.key"), new byte[31]);
        assertFailure(() -> install(c), Role.PROFILE, Category.INVALID);
    }

    @Test
    void noBeanClassNameContainsTestOnly() throws Exception {
        var ctx = install(contract());
        var factory = ctx.getBeanFactory();
        for (var name : factory.getBeanDefinitionNames()) {
            var type = factory.getType(name, false);
            assertThat(type).as(name).isNotNull();
            assertThat(type.getName()).doesNotContainIgnoringCase("TestOnly");
        }
        // Eagerly built values are installer-owned, never TEST_ONLY capability fixtures.
        for (var type :
                List.of(
                        NativeWalEnrollment.class,
                        NativeActorSourceEnrollment.class,
                        NativeCellRpcEnrollment.class,
                        NativeActorMaintenanceEnrollment.class,
                        ClusterReadiness.class))
            assertThat(instance(ctx, type).getClass().getName()).doesNotContainIgnoringCase("TestOnly");
    }
}
