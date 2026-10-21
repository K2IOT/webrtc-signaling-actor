package io.webrtc.signaling.app.runtime.local;

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
import java.util.function.Consumer;

import static io.webrtc.signaling.app.runtime.local.LocalTestPki.ACTOR_URI;
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
                    "SIGNALING_REMOTING_TRUSTSTORE_PASSWORD",
                    "SIGNALING_KUBE_API_HOST",
                    "SIGNALING_KUBE_API_PORT");

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
    LocalRuntimeContract contract(Consumer<java.util.Map<String, Object>> edit) throws Exception {
        var env = LocalRuntimeFixtures.install(mount, "ACTOR");
        authority.install(mount, actor);
        var profile = LocalRuntimeFixtures.profile(mount, "ACTOR");
        @SuppressWarnings("unchecked")
        var compatibility = (java.util.Map<String, Object>) profile.get("compatibility");
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
                .withProperty("SIGNALING_REMOTING_TRUSTSTORE_PASSWORD", PASSWORD)
                .withProperty("SIGNALING_KUBE_API_HOST", "kubernetes.default.svc")
                .withProperty("SIGNALING_KUBE_API_PORT", "443");
    }

    @Test
    void passesProductionValidation() throws Exception {
        var config = LocalPekkoConfig.build(contract(), environment());

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
        var config = LocalPekkoConfig.build(c, environment());

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
        var config = LocalPekkoConfig.build(contract(), environment());

        // The process derives compat-* roles itself and refuses a config that already has them.
        assertThat(config.getStringList("pekko.cluster.roles")).containsExactly("signaling-actor", "az-a");
        var roles = NativeCompatibility.enrolledBuildRoles(config).getStringList("pekko.cluster.roles");
        assertThat(roles)
                .contains(
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
        var initial = LocalPekkoConfig.build(c, environment().withProperty("SIGNALING_FORMATION_PROFILE", "initial-six"));
        assertThat(initial.getBoolean("pekko.management.cluster.bootstrap.new-cluster-enabled")).isTrue();
        assertThat(initial.getBoolean("signaling.controlled-initial-formation")).isTrue();
        assertThatCode(() -> ShardingBootstrap.validateProduction(initial)).doesNotThrowAnyException();

        for (var other : new String[]{null, "", "steady", "INITIAL-SIX", "initial-six "}) {
            var env = environment();
            if (other != null) env.withProperty("SIGNALING_FORMATION_PROFILE", other);
            var config = LocalPekkoConfig.build(c, env);
            assertThat(config.getBoolean("pekko.management.cluster.bootstrap.new-cluster-enabled")).isFalse();
            assertThat(config.hasPath("signaling.controlled-initial-formation")).isFalse();
        }
    }

    @Test
    void rejectsConfigWithoutAzRole() throws Exception {
        for (var bad : new String[]{null, "zone-a", "az-", "az-A", "az-_a"}) {
            var c = contract(profile -> profile.put("azRole", bad));
            assertThatThrownBy(() -> LocalPekkoConfig.build(c, environment()))
                    .isInstanceOfSatisfying(
                            LocalRuntimeFailure.class,
                            failure -> {
                                assertThat(failure.role()).isEqualTo(Role.PROFILE);
                                assertThat(failure.category()).isEqualTo(Category.INVALID);
                            });
        }
    }

    @Test
    void canonicalHostnameEqualsPodIp() throws Exception {
        var config = LocalPekkoConfig.build(contract(), environment());

        assertThat(config.getString("pekko.remote.artery.canonical.hostname")).isEqualTo("192.0.2.10");
        assertThat(config.getInt("pekko.remote.artery.canonical.port")).isEqualTo(25520);
        assertThat(config.getString("pekko.management.http.hostname")).isEqualTo("192.0.2.10");
    }

    @Test
    void rejectsUnusableBuildIdentity() throws Exception {
        List<Consumer<java.util.Map<String, Object>>> edits =
                List.of(
                        p -> compatibility(p).put("executableId", "TEST_ONLY_EXECUTABLE"),
                        p -> compatibility(p).put("features", List.of("not-a-feature")),
                        p -> compatibility(p).put("upperMinor", 3),
                        p -> compatibility(p).put("major", 0),
                        p -> p.put("compatibility", null),
                        p -> p.put("kube", null),
                        p -> p.put("plane", "GATEWAY"));
        for (var edit : edits) {
            var c = contract(edit);
            assertThatThrownBy(() -> LocalPekkoConfig.build(c, environment()))
                    .isInstanceOfSatisfying(
                            LocalRuntimeFailure.class,
                            failure -> assertThat(failure.role()).isEqualTo(Role.PROFILE));
        }
    }

    @Test
    void rejectsMissingOrMismatchedEnvironment() throws Exception {
        var c = contract();
        for (var name : REQUIRED) {
            var without = new MockEnvironment();
            for (var keep : REQUIRED)
                if (!keep.equals(name)) without.withProperty(keep, environment().getProperty(keep));
            assertThatThrownBy(() -> LocalPekkoConfig.build(c, without))
                    .isInstanceOf(LocalRuntimeFailure.class);
        }
        assertThatThrownBy(
                () -> LocalPekkoConfig.build(c, environment().withProperty("SIGNALING_CLUSTER_FINGERPRINT", "A".repeat(64))))
                .isInstanceOf(LocalRuntimeFailure.class);
        assertThatThrownBy(
                () -> LocalPekkoConfig.build(c, environment().withProperty("SIGNALING_KUBE_API_PORT", "444")))
                .isInstanceOf(LocalRuntimeFailure.class);
    }

    @Test
    void failureNeverEchoesSecrets() throws Exception {
        var c = contract();
        var failure =
                catchFailure(() -> LocalPekkoConfig.build(c, environment().withProperty("SIGNALING_KUBE_API_PORT", "444")));
        assertThat(failure.getMessage()).doesNotContain("444", PASSWORD, FINGERPRINT);
        assertThat(failure.getCause()).isNull();
    }

    @Test
    void processNamesTheSystemAfterTheCell() throws Exception {
        var c = contract();
        var config = LocalPekkoConfig.build(c, environment());
        var tls = SSLContext.getDefault();

        var enrollment = LocalPekkoConfig.process(c, config, tls, tls);

        assertThat(enrollment.systemName()).isEqualTo("signaling-c001");
        assertThat(enrollment.config().getString("signaling.cell-id")).isEqualTo("c001");
    }

    @Test
    void processRefusesAConfigForAnotherCell() throws Exception {
        var c = contract();
        var config =
                LocalPekkoConfig.build(c, environment())
                        .withValue("signaling.cell-id", com.typesafe.config.ConfigValueFactory.fromAnyRef("c002"));
        var tls = SSLContext.getDefault();

        assertThatThrownBy(() -> LocalPekkoConfig.process(c, config, tls, tls))
                .isInstanceOfSatisfying(
                        LocalRuntimeFailure.class,
                        failure -> assertThat(failure.role()).isEqualTo(Role.IDENTITY));
    }

    @SuppressWarnings("unchecked")
    private static java.util.Map<String, Object> compatibility(java.util.Map<String, Object> profile) {
        return (java.util.Map<String, Object>) profile.get("compatibility");
    }

    private static Throwable catchFailure(Runnable action) {
        try {
            action.run();
        } catch (RuntimeException failure) {
            return failure;
        }
        throw new AssertionError("expected a failure");
    }
}
