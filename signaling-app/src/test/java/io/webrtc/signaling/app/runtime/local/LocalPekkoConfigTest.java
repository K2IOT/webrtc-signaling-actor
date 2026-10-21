package io.webrtc.signaling.app.runtime.local;

import com.typesafe.config.Config;
import io.webrtc.signaling.actors.cluster.ShardingBootstrap;
import io.webrtc.signaling.app.runtime.NativeCompatibility;
import io.webrtc.signaling.app.runtime.local.LocalRuntimeFailure.Category;
import io.webrtc.signaling.app.runtime.local.LocalRuntimeFailure.Role;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.mock.env.MockEnvironment;

import javax.net.ssl.SSLContext;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static io.webrtc.signaling.app.runtime.local.LocalTestPki.ACTOR_URI;
import org.assertj.core.api.ThrowableAssert.ThrowingCallable;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class LocalPekkoConfigTest {
    static final String FINGERPRINT = "a".repeat(64);
    static final String EXECUTABLE = "b".repeat(64);
    static final String PASSWORD = LocalTestPki.PASSWORD;
    static final List<String> REQUIRED =
            List.of(
                    "SIGNALING_CLUSTER_FINGERPRINT",
                    "SIGNALING_REMOTING_KEYSTORE_PASSWORD",
                    "SIGNALING_REMOTING_KEY_PASSWORD",
                    "SIGNALING_REMOTING_TRUSTSTORE_PASSWORD");

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

    /** An actor mount whose process profile is the fixture's, adjusted by {@code edit}. */
    LocalRuntimeContract contract(Consumer<Map<String, Object>> edit) throws Exception {
        var env = LocalRuntimeFixtures.install(mount, "ACTOR");
        authority.install(mount, actor);
        var profile = LocalRuntimeFixtures.profile(mount, "ACTOR");
        @SuppressWarnings("unchecked")
        var compatibility = (Map<String, Object>) profile.get("compatibility");
        compatibility.put("major", 1);
        compatibility.put("minor", 2);
        compatibility.put("executableId", EXECUTABLE);
        compatibility.put("features", List.of("session-s1", "full-connection-active"));
        compatibility.put("lowerMinor", 1);
        compatibility.put("upperMinor", 2);
        edit.accept(profile);
        Files.writeString(mount.resolve("process.json"), LocalRuntimeFixtures.JSON.writeValueAsString(profile));
        return LocalRuntimeContract.load(env);
    }

    LocalRuntimeContract contract() throws Exception {
        return contract(profile -> {
        });
    }

    static MockEnvironment environment() {
        return new MockEnvironment()
                .withProperty("SIGNALING_CLUSTER_FINGERPRINT", FINGERPRINT)
                .withProperty("SIGNALING_REMOTING_KEYSTORE_PASSWORD", PASSWORD)
                .withProperty("SIGNALING_REMOTING_KEY_PASSWORD", PASSWORD)
                .withProperty("SIGNALING_REMOTING_TRUSTSTORE_PASSWORD", PASSWORD);
    }

    /** The OS process environment the fixture profile's Kubernetes API address requires. */
    static Map<String, String> osEnvironment() {
        return Map.of(
                "SIGNALING_KUBE_API_HOST", "kubernetes.default.svc", "SIGNALING_KUBE_API_PORT", "443");
    }

    static Config build(LocalRuntimeContract c, MockEnvironment env) {
        return LocalPekkoConfig.build(c, env, osEnvironment()::get);
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

    @Test
    void passesProductionValidation() throws Exception {
        var config = build(contract(), environment());

        assertThatCode(() -> ShardingBootstrap.validateProduction(config)).doesNotThrowAnyException();
        assertThat(config.getString("signaling.cell-id")).isEqualTo("c001");
        assertThat(config.getString("signaling.cluster-fingerprint")).isEqualTo(FINGERPRINT);
        assertThat(config.getString("pekko.discovery.kubernetes-api.pod-namespace")).isEqualTo("signaling");
        assertThat(config.getString("pekko.discovery.kubernetes-api.pod-label-selector"))
                .isEqualTo("app=webrtc-signaling,plane=actor,cell=c001");
        assertThat(config.getString("pekko.management.cluster.bootstrap.contact-point-discovery.service-name"))
                .isEqualTo("signaling-c001");
    }

    @Test
    void takesKubeAndRemotingFilesFromTheConfinedMount() throws Exception {
        var c = contract();
        var config = build(c, environment());

        assertThat(config.getString("pekko.discovery.kubernetes-api.api-ca-path"))
                .isEqualTo(c.resolve(mount.resolve("kube-ca.crt").toString()).toString());
        assertThat(config.getString("pekko.discovery.kubernetes-api.api-token-path"))
                .isEqualTo(c.resolve(mount.resolve("kube-token").toString()).toString());
        var tls = config.getConfig("pekko.remote.artery.ssl.config-ssl-engine");
        assertThat(tls.getString("key-store")).isEqualTo(c.resolve(mount.resolve("pod/remoting.p12").toString()).toString());
        assertThat(tls.getString("trust-store")).isEqualTo(c.resolve(mount.resolve("trust.p12").toString()).toString());
        assertThat(tls.getString("key-store-password")).isEqualTo(PASSWORD);
        assertThat(tls.getBoolean("require-mutual-authentication")).isTrue();
        assertThat(config.getString("pekko.discovery.kubernetes-api.api-service-host-env-name"))
                .isEqualTo("SIGNALING_KUBE_API_HOST");
        assertThat(config.getString("pekko.discovery.kubernetes-api.api-service-port-env-name"))
                .isEqualTo("SIGNALING_KUBE_API_PORT");
    }

    @Test
    void exposesCompatibilityRolesForOwnBuild() throws Exception {
        var config = build(contract(), environment());

        // The process derives compat-* roles itself and refuses a config that already has them.
        assertThat(config.getStringList("pekko.cluster.roles")).containsExactly("signaling-actor", "az-a");
        var roles = NativeCompatibility.enrolledBuildRoles(config).getStringList("pekko.cluster.roles");
        assertThat(roles)
                .containsExactlyInAnyOrder(
                        "signaling-actor",
                        "az-a",
                        "compat-major-1",
                        "compat-minor-2",
                        "compat-executable-" + EXECUTABLE,
                        "compat-cell-c001",
                        "compat-fingerprint-" + FINGERPRINT,
                        "compat-feature-session-s1",
                        "compat-feature-full-connection-active");
        assertThat(config.getInt("signaling.compatibility.window.major")).isEqualTo(1);
        assertThat(config.getInt("signaling.compatibility.window.lower-minor")).isEqualTo(1);
        assertThat(config.getInt("signaling.compatibility.window.upper-minor")).isEqualTo(2);
    }

    @Test
    void initialSixEnablesControlledFormationOnly() throws Exception {
        var c = contract();
        var initial = build(c, environment().withProperty("SIGNALING_FORMATION_PROFILE", "initial-six"));
        assertThat(initial.getBoolean("pekko.management.cluster.bootstrap.new-cluster-enabled")).isTrue();
        assertThat(initial.getBoolean("signaling.controlled-initial-formation")).isTrue();
        assertThatCode(() -> ShardingBootstrap.validateProduction(initial)).doesNotThrowAnyException();

        for (var other : new String[]{null, "", "steady", "INITIAL-SIX", "initial-six "}) {
            var env = environment();
            if (other != null) env.withProperty("SIGNALING_FORMATION_PROFILE", other);
            var config = build(c, env);
            assertThat(config.getBoolean("pekko.management.cluster.bootstrap.new-cluster-enabled")).isFalse();
            assertThat(config.hasPath("signaling.controlled-initial-formation")).isFalse();
        }
    }

    @Test
    void rejectsConfigWithoutAzRole() throws Exception {
        for (var bad : new String[]{null, "zone-a", "az-", "az-A", "az-_a"}) {
            var c = contract(profile -> profile.put("azRole", bad));
            assertFailure(() -> build(c, environment()), Role.PROFILE, Category.INVALID);
        }
    }

    @Test
    void canonicalHostnameEqualsPodIp() throws Exception {
        var config = build(contract(), environment());

        assertThat(config.getString("pekko.remote.artery.canonical.hostname")).isEqualTo("192.0.2.10");
        assertThat(config.getInt("pekko.remote.artery.canonical.port")).isEqualTo(25520);
        assertThat(config.getString("pekko.management.http.hostname")).isEqualTo("192.0.2.10");
    }

    @Test
    void rejectsUnusableBuildIdentity() throws Exception {
        List<Consumer<Map<String, Object>>> edits =
                List.of(
                        p -> compatibility(p).put("executableId", "TEST_ONLY_EXECUTABLE"),
                        p -> compatibility(p).put("features", List.of("not-a-feature")),
                        p -> compatibility(p).put("upperMinor", 3),
                        p -> compatibility(p).put("major", 0),
                        // The build minor must be the lower or the upper bound of the window.
                        p -> compatibility(p).put("minor", 5),
                        p -> compatibility(p).put("minor", 0),
                        p -> p.put("compatibility", null),
                        p -> p.put("kube", null),
                        p -> p.put("plane", "GATEWAY"));
        for (var edit : edits) {
            var c = contract(edit);
            assertFailure(() -> build(c, environment()), Role.PROFILE, Category.INVALID);
        }
    }

    @Test
    void rejectsMissingOrMalformedEnvironment() throws Exception {
        var c = contract();
        for (var name : REQUIRED) {
            var without = new MockEnvironment();
            for (var keep : REQUIRED)
                if (!keep.equals(name)) without.withProperty(keep, environment().getProperty(keep));
            boolean fingerprint = name.equals("SIGNALING_CLUSTER_FINGERPRINT");
            assertFailure(
                    () -> build(c, without),
                    fingerprint ? Role.IDENTITY : Role.CONTRACT,
                    Category.MISSING);
            if (!fingerprint)
                assertFailure(
                        () -> build(c, environment().withProperty(name, "")), Role.CONTRACT, Category.MISSING);
        }
        assertFailure(
                () -> build(c, environment().withProperty("SIGNALING_CLUSTER_FINGERPRINT", "A".repeat(64))),
                Role.IDENTITY,
                Category.INVALID);
        assertFailure(
                () -> build(c, environment().withProperty("SIGNALING_CLUSTER_FINGERPRINT", "a".repeat(63))),
                Role.IDENTITY,
                Category.INVALID);
    }

    @Test
    void requiresTheProfileKubeAddressInTheOsEnvironment() throws Exception {
        var c = contract();
        var env = environment();
        var host = "SIGNALING_KUBE_API_HOST";
        var port = "SIGNALING_KUBE_API_PORT";

        assertThat(LocalPekkoConfig.build(c, env, osEnvironment()::get)).isNotNull();
        assertFailure(
                () -> LocalPekkoConfig.build(c, env, Map.of(host, "other.example.test", port, "443")::get),
                Role.PROFILE,
                Category.IDENTITY_MISMATCH);
        assertFailure(
                () -> LocalPekkoConfig.build(c, env, Map.of(host, "kubernetes.default.svc", port, "444")::get),
                Role.PROFILE,
                Category.IDENTITY_MISMATCH);
        assertFailure(
                () -> LocalPekkoConfig.build(c, env, Map.of(port, "443")::get),
                Role.PROFILE,
                Category.IDENTITY_MISMATCH);
        assertFailure(
                () -> LocalPekkoConfig.build(c, env, Map.of(host, "kubernetes.default.svc")::get),
                Role.PROFILE,
                Category.IDENTITY_MISMATCH);
        assertFailure(
                () -> LocalPekkoConfig.build(c, env, Map.<String, String>of()::get),
                Role.PROFILE,
                Category.IDENTITY_MISMATCH);
    }

    @Test
    void springAndSystemPropertiesNeverSatisfyTheKubeAddress() throws Exception {
        var c = contract();
        // Pekko reads the OS environment; a Spring property or -D property holding the right value
        // must not pass while discovery would find nothing.
        var env =
                environment()
                        .withProperty("SIGNALING_KUBE_API_HOST", "kubernetes.default.svc")
                        .withProperty("SIGNALING_KUBE_API_PORT", "443");
        var previousHost = System.getProperty("SIGNALING_KUBE_API_HOST");
        var previousPort = System.getProperty("SIGNALING_KUBE_API_PORT");
        System.setProperty("SIGNALING_KUBE_API_HOST", "kubernetes.default.svc");
        System.setProperty("SIGNALING_KUBE_API_PORT", "443");
        try {
            assertFailure(
                    () -> LocalPekkoConfig.build(c, env, Map.<String, String>of()::get),
                    Role.PROFILE,
                    Category.IDENTITY_MISMATCH);
            assertFailure(
                    () -> LocalPekkoConfig.build(c, env, name -> System.getProperty(name + ".absent")),
                    Role.PROFILE,
                    Category.IDENTITY_MISMATCH);
            assertThat(LocalPekkoConfig.build(c, environment(), osEnvironment()::get)).isNotNull();
        } finally {
            restore("SIGNALING_KUBE_API_HOST", previousHost);
            restore("SIGNALING_KUBE_API_PORT", previousPort);
        }
    }

    @Test
    void failureNeverEchoesInputs() throws Exception {
        var sentinelFingerprint = "c".repeat(64);
        var sentinelPassword = "SENTINEL_PASSWORD_9f3a";
        var env =
                new MockEnvironment()
                        .withProperty("SIGNALING_CLUSTER_FINGERPRINT", sentinelFingerprint)
                        .withProperty("SIGNALING_REMOTING_KEYSTORE_PASSWORD", sentinelPassword)
                        .withProperty("SIGNALING_REMOTING_KEY_PASSWORD", sentinelPassword)
                        .withProperty("SIGNALING_REMOTING_TRUSTSTORE_PASSWORD", sentinelPassword);
        // Both failures occur after the fingerprint and passwords have been read; the second one
        // after they are already inside the built config.
        var addressMismatch = contract();
        var failures =
                new LocalRuntimeFailure[]{
                        failureOf(
                                () ->
                                        LocalPekkoConfig.build(
                                                addressMismatch,
                                                env,
                                                Map.of(
                                                        "SIGNALING_KUBE_API_HOST", "sentinel-host.example.test",
                                                        "SIGNALING_KUBE_API_PORT", "4444")::get)),
                        failureOf(
                                () ->
                                        LocalPekkoConfig.build(
                                                contract(p -> compatibility(p).put("features", List.of("sentinel-feature"))),
                                                env,
                                                osEnvironment()::get))
                };
        for (var failure : failures) {
            assertThat(failure.getMessage())
                    .doesNotContain(
                            sentinelFingerprint,
                            sentinelPassword,
                            "sentinel-host",
                            "4444",
                            "sentinel-feature",
                            mount.toString());
            assertThat(failure.getCause()).isNull();
            assertThat(failure.getSuppressed()).isEmpty();
        }
    }

    @Test
    void processNamesTheSystemAfterTheCell() throws Exception {
        var c = contract();
        var config = build(c, environment());
        var tls = SSLContext.getDefault();

        var enrollment = LocalPekkoConfig.process(c, config, tls, tls);

        assertThat(enrollment.systemName()).isEqualTo("signaling-c001");
        assertThat(enrollment.config().getString("signaling.cell-id")).isEqualTo("c001");
    }

    @Test
    void processRefusesAConfigForAnotherCell() throws Exception {
        var c = contract();
        var config =
                build(c, environment())
                        .withValue("signaling.cell-id", com.typesafe.config.ConfigValueFactory.fromAnyRef("c002"));
        var tls = SSLContext.getDefault();

        assertThatThrownBy(() -> LocalPekkoConfig.process(c, config, tls, tls))
                .isInstanceOfSatisfying(
                        LocalRuntimeFailure.class,
                        failure -> assertThat(failure.role()).isEqualTo(Role.IDENTITY));
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> compatibility(Map<String, Object> profile) {
        return (Map<String, Object>) profile.get("compatibility");
    }

    private static LocalRuntimeFailure failureOf(ThrowingCallable action) {
        try {
            action.call();
        } catch (LocalRuntimeFailure failure) {
            return failure;
        } catch (Throwable other) {
            throw new AssertionError("expected a LocalRuntimeFailure", other);
        }
        throw new AssertionError("expected a failure");
    }

    private static void restore(String name, String value) {
        if (value == null) System.clearProperty(name);
        else System.setProperty(name, value);
    }
}
