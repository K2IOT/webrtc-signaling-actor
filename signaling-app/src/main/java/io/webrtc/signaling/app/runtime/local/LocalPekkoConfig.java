package io.webrtc.signaling.app.runtime.local;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;
import io.webrtc.signaling.actors.cluster.ShardingBootstrap;
import io.webrtc.signaling.app.CompatibilityWindow.Feature;
import io.webrtc.signaling.app.SignalingApplication.Plane;
import io.webrtc.signaling.app.runtime.NativeActorProcessEnrollment;
import io.webrtc.signaling.app.runtime.NativeCompatibility;
import io.webrtc.signaling.app.runtime.local.LocalRuntimeFailure.Category;
import io.webrtc.signaling.app.runtime.local.LocalRuntimeFailure.Role;
import org.springframework.core.env.Environment;

import javax.net.ssl.SSLContext;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Pekko {@link Config} and process enrollment for one LOCAL_TEST_ONLY actor container. The packaged
 * {@code application-actor.conf} stays the base; only values the mounted contract, the process
 * profile and the process environment decide are layered over it. Every file path goes through
 * {@link LocalRuntimeContract#resolve(String)}. Failures are {@link LocalRuntimeFailure}s that name
 * a role and a category and never carry a value, path or cause.
 *
 * <p>The returned config carries the remoting PKCS12 passwords, which Pekko reads from
 * configuration; it must never be logged or rendered.
 */
public final class LocalPekkoConfig {
    static final String FINGERPRINT_VARIABLE = "SIGNALING_CLUSTER_FINGERPRINT";
    static final String FORMATION_VARIABLE = "SIGNALING_FORMATION_PROFILE";
    static final String INITIAL_FORMATION = "initial-six";
    static final String KEYSTORE_PASSWORD_VARIABLE = "SIGNALING_REMOTING_KEYSTORE_PASSWORD";
    static final String KEY_PASSWORD_VARIABLE = "SIGNALING_REMOTING_KEY_PASSWORD";
    static final String TRUSTSTORE_PASSWORD_VARIABLE = "SIGNALING_REMOTING_TRUSTSTORE_PASSWORD";
    /**
     * Pekko takes the Kubernetes API address from environment variables whose <em>names</em> are
     * configuration. The process must export these two with exactly the profile's values.
     */
    static final String KUBE_HOST_VARIABLE = "SIGNALING_KUBE_API_HOST";
    static final String KUBE_PORT_VARIABLE = "SIGNALING_KUBE_API_PORT";
    private static final String DISCOVERY = "pekko.discovery.kubernetes-api.";
    private static final String TLS = "pekko.remote.artery.ssl.config-ssl-engine.";
    private static final String COMPATIBILITY = "signaling.compatibility.";

    private LocalPekkoConfig() {
    }

    /** The resolved, production-validated-shape actor config for this container. */
    public static Config build(LocalRuntimeContract c, Environment env) {
        var profile = c.profile();
        if (profile.plane() != Plane.ACTOR) throw profileInvalid();
        var cell = c.contract().cell();
        var kube = profile.kube();
        var compatibility = profile.compatibility();
        if (kube == null || compatibility == null || !validAzRole(profile.azRole())) throw profileInvalid();
        var tls = c.contract().tls();

        var values = new HashMap<String, Object>();
        values.put("signaling.cell-id", cell);
        values.put("signaling.cluster-fingerprint", fingerprint(env));
        values.put("pekko.cluster.roles", List.of("signaling-actor", profile.azRole()));
        values.put("pekko.remote.artery.canonical.hostname", profile.podIp());
        values.put("pekko.management.http.hostname", profile.podIp());
        values.put(
                "pekko.management.cluster.bootstrap.contact-point-discovery.service-name", "signaling-" + cell);

        values.put(TLS + "key-store", c.resolve(tls.remotingKeyStore()).toString());
        values.put(TLS + "trust-store", c.resolve(tls.remotingTrustStore()).toString());
        values.put(TLS + "key-store-password", secret(env, KEYSTORE_PASSWORD_VARIABLE));
        values.put(TLS + "key-password", secret(env, KEY_PASSWORD_VARIABLE));
        values.put(TLS + "trust-store-password", secret(env, TRUSTSTORE_PASSWORD_VARIABLE));

        values.put(DISCOVERY + "pod-namespace", kube.namespace());
        values.put(DISCOVERY + "pod-label-selector", "app=webrtc-signaling,plane=actor,cell=" + cell);
        values.put(DISCOVERY + "api-ca-path", c.resolve(kube.caPath()).toString());
        values.put(DISCOVERY + "api-token-path", c.resolve(kube.tokenPath()).toString());
        values.put(DISCOVERY + "api-service-host-env-name", KUBE_HOST_VARIABLE);
        values.put(DISCOVERY + "api-service-port-env-name", KUBE_PORT_VARIABLE);
        kubeAddress(env, kube.apiHost(), kube.apiPort());

        compatibility(values, compatibility);
        if (INITIAL_FORMATION.equals(env.getProperty(FORMATION_VARIABLE))) {
            values.put("pekko.management.cluster.bootstrap.new-cluster-enabled", true);
            values.put("signaling.controlled-initial-formation", true);
        }

        var overrides = ConfigFactory.empty();
        for (var entry : values.entrySet())
            overrides = overrides.withValue(entry.getKey(), ConfigValueFactory.fromAnyRef(entry.getValue()));
        try {
            return overrides.withFallback(ShardingBootstrap.baseConfig()).resolve();
        } catch (RuntimeException unresolved) {
            // Config diagnostics can quote values; keep none of them.
            throw profileInvalid();
        }
    }

    /**
     * The enrollment for this container: system name {@code signaling-<cell>}. {@code config} must
     * be this container's cell config; the enrollment constructor then re-runs the production
     * validation.
     */
    public static NativeActorProcessEnrollment process(
            LocalRuntimeContract c, Config config, SSLContext server, SSLContext client) {
        var cell = c.contract().cell();
        if (!config.hasPath("signaling.cell-id") || !cell.equals(config.getString("signaling.cell-id")))
            throw LocalJson.failure(Role.IDENTITY, Category.IDENTITY_MISMATCH);
        try {
            return new NativeActorProcessEnrollment("signaling-" + cell, config, server, client);
        } catch (RuntimeException rejected) {
            throw profileInvalid();
        }
    }

    private static void compatibility(Map<String, Object> values, LocalProcessProfile.Compatibility build) {
        // The native process needs the exact 64-hex executable id to admit peers, and the adjacent
        // window [lower, lower + 1] must contain this build; fail here, not as a closed gate later.
        if (build.major() < 1
                || !build.executableId().matches("[a-f0-9]{64}")
                || build.upperMinor() != (long) build.lowerMinor() + 1
                || (build.minor() != build.lowerMinor() && build.minor() != build.upperMinor()))
            throw profileInvalid();
        var features = new ArrayList<String>();
        for (var name : build.features()) {
            try {
                var feature = Feature.valueOf(name.replace('-', '_').toUpperCase(Locale.ROOT));
                if (!NativeCompatibility.IMPLEMENTED.contains(feature)) throw new IllegalArgumentException();
            } catch (IllegalArgumentException unsupported) {
                throw profileInvalid();
            }
            features.add(name);
        }
        features.sort(String::compareTo);
        values.put(COMPATIBILITY + "major", build.major());
        values.put(COMPATIBILITY + "minor", build.minor());
        values.put(COMPATIBILITY + "executable-id", build.executableId());
        values.put(COMPATIBILITY + "features", features);
        values.put(COMPATIBILITY + "window.major", build.major());
        values.put(COMPATIBILITY + "window.lower-minor", build.lowerMinor());
        values.put(COMPATIBILITY + "window.upper-minor", build.upperMinor());
    }

    private static String fingerprint(Environment env) {
        var value = env.getProperty(FINGERPRINT_VARIABLE);
        if (value == null) throw LocalJson.failure(Role.IDENTITY, Category.MISSING);
        if (!value.matches("[a-f0-9]{64}")) throw LocalJson.failure(Role.IDENTITY, Category.INVALID);
        return value;
    }

    private static String secret(Environment env, String variable) {
        var value = env.getProperty(variable);
        if (value == null || value.isEmpty() || value.length() > 512)
            throw LocalJson.failure(Role.CONTRACT, Category.MISSING);
        return value;
    }

    /** Pekko reads the address from the process environment; it has to be the profile's. */
    private static void kubeAddress(Environment env, String host, int port) {
        if (!host.equals(env.getProperty(KUBE_HOST_VARIABLE))
                || !Integer.toString(port).equals(env.getProperty(KUBE_PORT_VARIABLE)))
            throw LocalJson.failure(Role.PROFILE, Category.IDENTITY_MISMATCH);
    }

    private static boolean validAzRole(String role) {
        return role != null && role.matches("az-[a-z0-9][a-z0-9-]{0,62}");
    }

    private static LocalRuntimeFailure profileInvalid() {
        return LocalJson.failure(Role.PROFILE, Category.INVALID);
    }
}
