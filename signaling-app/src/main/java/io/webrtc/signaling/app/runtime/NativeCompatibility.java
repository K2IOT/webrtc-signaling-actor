package io.webrtc.signaling.app.runtime;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.webrtc.signaling.actors.cluster.ClusterReadiness;
import io.webrtc.signaling.actors.cluster.NativeClusterMembership;
import io.webrtc.signaling.app.CompatibilityWindow;
import io.webrtc.signaling.app.CompatibilityWindow.Feature;
import io.webrtc.signaling.protocol.RuntimeCapabilities;

import java.time.Instant;
import java.util.*;
import java.util.function.Supplier;

/**
 * Feature admission re-reads reachable authenticated membership before every new wire producer.
 */
public final class NativeCompatibility implements RuntimeCapabilities {
    // Wire features implemented by this source; executable release adjacency must be enrolled.
    public static final Set<Feature> IMPLEMENTED =
            Set.of(
                    Feature.SESSION_S1,
                    Feature.FULL_CONNECTION_ACTIVE,
                    Feature.CAUSAL_ROUTE_LOSS,
                    Feature.NATIVE_RENEWAL_V1,
                    Feature.NATIVE_RECOVERY_RECEIPT_V1,
                    Feature.NATIVE_WAKE_RECEIPT_V1,
                    Feature.NATIVE_CONTROL_RECEIPT_V1,
                    Feature.HOME_PROOF_HIGH_WATER_V1,
                    Feature.STORAGE_DOMAIN_ADOPTION_V1,
                    Feature.NATIVE_ACTOR_TRACE_V1,
                    Feature.NATIVE_INITIAL_ICE_RECEIPT_V1);
    private final String cell, fingerprint;
    private final Supplier<NativeActorBusinessEnrollment> local;
    private final Supplier<NativeCompatibilityEnrollment> enrollment;
    private final Supplier<NativeActorSchedulingEnrollment> scheduling;
    private boolean admissionDisabled;
    private UUID admissionFence;
    private volatile NativeCompatibilityEnrollment.SchemaObservation localSchema;

    public NativeCompatibility(
            String cell,
            String fingerprint,
            Supplier<NativeActorBusinessEnrollment> local,
            Supplier<NativeActorSchedulingEnrollment> scheduling,
            Supplier<NativeCompatibilityEnrollment> enrollment) {
        this.cell = Objects.requireNonNull(cell);
        this.fingerprint = Objects.requireNonNull(fingerprint);
        this.local = Objects.requireNonNull(local);
        this.scheduling = Objects.requireNonNull(scheduling);
        this.enrollment = Objects.requireNonNull(enrollment);
    }

    public static Config enrolledBuildRoles(Config config) {
        var roles = new HashSet<>(config.getStringList("pekko.cluster.roles"));
        if (roles.stream().anyMatch(r -> r.startsWith("compat-")))
            throw new IllegalArgumentException("Compatibility roles belong to the native build");
        var build = configuredBuild(config);
        // Unenrolled processes may form the transport; feature admission and readiness stay closed.
        if (build == null) return config;
        roles.add("compat-major-" + build.major());
        roles.add("compat-minor-" + build.minor());
        roles.add("compat-executable-" + build.executableId());
        roles.add("compat-cell-" + config.getString("signaling.cell-id"));
        roles.add("compat-fingerprint-" + config.getString("signaling.cluster-fingerprint"));
        for (var feature : build.implementedFeatures())
            roles.add(
                    (feature == Feature.NATIVE_ACTOR_TRACE_V1 ? "compat-observation-" : "compat-feature-")
                            + wire(feature));
        return ConfigFactory.parseMap(Map.of("pekko.cluster.roles", List.copyOf(roles)))
                .withFallback(config);
    }

    private static int migration16Checksum() throws java.io.IOException {
        var crc = new java.util.zip.CRC32();
        try (var stream =
                     NativeCompatibility.class.getResourceAsStream(
                             "/db/migration/V016__storage_domain_renewal_adoption.sql")) {
            if (stream == null) throw new java.io.IOException("Migration identity unavailable");
            try (var reader =
                         new java.io.BufferedReader(
                                 new java.io.InputStreamReader(stream, java.nio.charset.StandardCharsets.UTF_8))) {
                String line;
                boolean first = true;
                while ((line = reader.readLine()) != null) {
                    if (first && line.startsWith("\uFEFF")) line = line.substring(1);
                    first = false;
                    crc.update(line.getBytes(java.nio.charset.StandardCharsets.UTF_8));
                }
            }
        }
        return (int) crc.getValue();
    }

    private static NativeCompatibilityEnrollment.BuildIdentity configuredBuild(Config config) {
        String prefix = "signaling.compatibility.";
        if (!config.hasPath(prefix + "major")
                || !config.hasPath(prefix + "minor")
                || !config.hasPath(prefix + "executable-id")
                || !config.hasPath(prefix + "features")) return null;
        var features = EnumSet.noneOf(Feature.class);
        for (var name : config.getStringList(prefix + "features"))
            features.add(Feature.valueOf(name.replace('-', '_').toUpperCase(Locale.ROOT)));
        if (!IMPLEMENTED.containsAll(features))
            throw new IllegalArgumentException("Unsupported local native feature");
        return new NativeCompatibilityEnrollment.BuildIdentity(
                config.getInt(prefix + "major"),
                config.getInt(prefix + "minor"),
                config.getString(prefix + "executable-id"),
                config.getString("signaling.cluster-fingerprint"),
                features);
    }

    /**
     * The effective key is {@code ssl.config-ssl-engine}, which {@code application-actor.conf} and
     * {@code ShardingBootstrap.validateProduction} use. The older {@code ssl.config} path is read
     * only when the effective one is absent. Pekko's reference.conf always defines the effective
     * key, so on a real ActorSystem the fallback never applies; it only matters for synthetic
     * configs. Absence of both, or any non-boolean value, fails closed.
     */
    static boolean mutualAuthenticationRequired(Config config) {
        String key = ".require-mutual-authentication";
        String effective = "pekko.remote.artery.ssl.config-ssl-engine" + key;
        return config.hasPath(effective)
                ? config.getBoolean(effective)
                : config.getBoolean("pekko.remote.artery.ssl.config" + key);
    }

    private static boolean fresh(Instant until) {
        var now = Instant.now();
        return until.isAfter(now) && !until.isAfter(now.plusSeconds(5));
    }

    private static String single(Set<String> roles, String prefix) {
        var values = roles.stream().filter(r -> r.startsWith(prefix)).toList();
        if (values.size() != 1) throw new IllegalArgumentException("Missing authenticated descriptor");
        return values.getFirst().substring(prefix.length());
    }

    private static String wire(Feature feature) {
        return feature.name().toLowerCase(Locale.ROOT).replace('_', '-');
    }

    @Override
    public synchronized boolean permits(String target, String capability) {
        final Feature feature;
        try {
            feature = Feature.valueOf(capability.replace('-', '_').toUpperCase(Locale.ROOT));
        } catch (RuntimeException invalid) {
            return false;
        }
        try {
            if ((feature == Feature.NATIVE_RENEWAL_V1 || feature == Feature.STORAGE_DOMAIN_ADOPTION_V1)
                    && !schema16(target)) return false;
            var peers = peers(target);
            if (peers == null || peers.isEmpty()) return false;
            var build = build();
            if (build == null || !build.implementedFeatures().contains(feature)) return false;
            var approved = approvedWindow();
            if (approved == null || !approved.contains(build.major(), build.minor())) return false;
            // Independent feature windows prevent optional observability from latching authority gates.
            var window =
                    new CompatibilityWindow(
                            target, approved.major(), approved.upperMinor(), build.fingerprint());
            return window.enable(feature, peers) && peers.stream().allMatch(window::accepts);
        } catch (RuntimeException unavailable) {
            return false;
        }
    }

    private List<CompatibilityWindow.Peer> peers(String target) {
        if (cell.equals(target)) {
            var inputs = local.get();
            if (inputs != null) {
                var config = inputs.system().settings().config();
                if (!mutualAuthenticationRequired(config)) return null;
                var schedule = scheduling.get();
                if (schedule == null || schedule.system() != inputs.system()) return null;
                var membership =
                        new NativeClusterMembership(
                                inputs.system(), new ClusterReadiness(), schedule.azRoles());
                var result = new ArrayList<CompatibilityWindow.Peer>();
                for (var peer : membership.authenticatedUpPeers()) {
                    if (peer.zone().equals("INVALID")) return null;
                    var roles = peer.roles();
                    if (!single(roles, "compat-executable-").matches("[a-f0-9]{64}")) return null;
                    String memberCell = single(roles, "compat-cell-");
                    int major = Integer.parseInt(single(roles, "compat-major-"));
                    int minor = Integer.parseInt(single(roles, "compat-minor-"));
                    var features = EnumSet.noneOf(Feature.class);
                    for (String role : roles)
                        if (role.startsWith("compat-feature-"))
                            features.add(
                                    Feature.valueOf(
                                            role.substring("compat-feature-".length())
                                                    .replace('-', '_')
                                                    .toUpperCase(Locale.ROOT)));
                    if (roles.contains("compat-observation-native-actor-trace-v1"))
                        features.add(Feature.NATIVE_ACTOR_TRACE_V1);
                    result.add(
                            new CompatibilityWindow.Peer(
                                    peer.memberId(),
                                    peer.zone(),
                                    memberCell,
                                    major,
                                    minor,
                                    single(roles, "compat-fingerprint-"),
                                    features));
                }
                return List.copyOf(result);
            }
        }
        var inputs = enrollment.get();
        if (inputs == null) return null;
        var snapshots = inputs.authenticatedRemoteActors().get();
        if (snapshots == null || snapshots.size() > 50) return null;
        var snapshot = snapshots.get(target);
        return snapshot != null && fresh(snapshot.validUntil()) ? snapshot.peers() : null;
    }

    @Override
    public synchronized boolean permitsGateway(
            String target, String gatewayId, UUID bootId, String capability) {
        try {
            var inputs = enrollment.get();
            if (inputs == null) return false;
            var snapshots = inputs.authenticatedGatewayBoots().get();
            if (snapshots == null || snapshots.size() > 128) return false;
            var peer =
                    snapshots.get(new NativeCompatibilityEnrollment.GatewayBoot(target, gatewayId, bootId));
            var feature = Feature.valueOf(capability.replace('-', '_').toUpperCase(Locale.ROOT));
            var build = build();
            var approved = approvedWindow();
            return build != null
                    && approved != null
                    && approved.contains(build.major(), build.minor())
                    && peer != null
                    && fresh(peer.validUntil())
                    && approved.contains(peer.major(), peer.minor())
                    && build.implementedFeatures().contains(feature)
                    && peer.fingerprint().equals(build.fingerprint())
                    && peer.features().contains(feature);
        } catch (RuntimeException unavailable) {
            return false;
        }
    }

    /**
     * Native operator call; no unauthenticated administrative endpoint is installed.
     */
    public synchronized void disableFeatureAdmission() {
        admissionDisabled = true;
        if (admissionFence == null) admissionFence = UUID.randomUUID();
    }

    @Override
    public synchronized boolean permitsNew(String target, String capability) {
        return !admissionDisabled && permits(target, capability);
    }

    public synchronized UUID admissionFence() {
        return admissionFence;
    }

    public synchronized boolean admissionOpen() {
        return !admissionDisabled;
    }

    public synchronized boolean rollbackEligible() {
        if (!admissionDisabled) return false;
        try {
            var inputs = enrollment.get();
            if (inputs == null) return false;
            var observation = inputs.rollbackObservation().get();
            return observation != null
                    && fresh(observation.validUntil())
                    && observation.incompatibleDurableWork().isPresent()
                    && observation.incompatibleDurableWork().getAsLong() == 0
                    && observation.originalPhysicalOwnersSettled().orElse(false)
                    && observation.expandContractSchemaRetained().orElse(false);
        } catch (RuntimeException unavailable) {
            return false;
        }
    }

    public NativeWorkerScheduler.Job schemaJob(io.webrtc.signaling.storage.SqlTransactions sql) {
        return new NativeWorkerScheduler.Job(
                "schema_compatibility",
                NativeWorkerScheduler.Priority.SAFETY,
                java.time.Duration.ofSeconds(1),
                budget -> {
                    var operation =
                            sql.submitTracked(
                                    io.webrtc.signaling.storage.DbClass.RECOVERY,
                                    budget,
                                    connection -> {
                                        try (var statement =
                                                     connection.prepareStatement(
                                                             "SELECT checksum FROM flyway_schema_history WHERE success AND (version='16' OR version='016')")) {
                                            try (var rows = statement.executeQuery()) {
                                                if (!rows.next()) return false;
                                                int actual = rows.getInt(1);
                                                if (rows.wasNull() || rows.next()) return false;
                                                return actual == migration16Checksum();
                                            }
                                        }
                                    });
                    var logical =
                            operation
                                    .logical()
                                    .thenApply(
                                            applied -> {
                                                localSchema =
                                                        new NativeCompatibilityEnrollment.SchemaObservation(
                                                                Instant.now().plusSeconds(5), applied);
                                                return applied;
                                            });
                    return new io.webrtc.signaling.rpc.RpcOperation<>(
                            logical, operation.physicalCompletion().thenApply(v -> null));
                });
    }

    public NativeCompatibilityEnrollment.SchemaObservation localSchemaObservation() {
        var snapshot = localSchema;
        return snapshot != null && fresh(snapshot.validUntil()) ? snapshot : null;
    }

    private boolean schema16(String target) {
        if (cell.equals(target) && localSchema != null && fresh(localSchema.validUntil()))
            return localSchema.exactMigration16Applied();
        var inputs = enrollment.get();
        if (inputs == null) return false;
        var facts = inputs.appliedSchemas().get();
        if (facts == null || facts.size() > 50) return false;
        var fact = facts.get(target);
        return fact != null && fresh(fact.validUntil()) && fact.exactMigration16Applied();
    }

    public boolean controlReady() {
        return permitsNew(cell, SESSION_S1);
    }

    public boolean gatewayReady(String gatewayId, java.util.UUID bootId) {
        return permitsNew(cell, SESSION_S1)
                && permits(cell, FULL_CONNECTION_ACTIVE)
                && permitsGateway(cell, gatewayId, bootId, NATIVE_CONTROL_RECEIPT_V1);
    }

    public boolean actorReady() {
        return permits(cell, SESSION_S1)
                && permits(cell, FULL_CONNECTION_ACTIVE)
                && permits(cell, NATIVE_WAKE_RECEIPT_V1)
                && permits(cell, NATIVE_RECOVERY_RECEIPT_V1)
                && permits(cell, HOME_PROOF_HIGH_WATER_V1)
                && permits(cell, NATIVE_RENEWAL_V1);
    }

    private NativeCompatibilityEnrollment.BuildIdentity build() {
        var actor = local.get();
        var inputs = actor == null ? enrollment.get() : null;
        var identity =
                actor != null
                        ? configuredBuild(actor.system().settings().config())
                        : inputs == null ? null : inputs.localBuild();
        return identity != null
                && fingerprint.equals(identity.fingerprint())
                && IMPLEMENTED.containsAll(identity.implementedFeatures())
                ? identity
                : null;
    }

    private NativeCompatibilityEnrollment.ApprovedWindow approvedWindow() {
        var actor = local.get();
        if (actor == null) {
            var inputs = enrollment.get();
            return inputs == null ? null : inputs.approvedWindow();
        }
        var config = actor.system().settings().config();
        String prefix = "signaling.compatibility.window.";
        if (!config.hasPath(prefix + "major")
                || !config.hasPath(prefix + "lower-minor")
                || !config.hasPath(prefix + "upper-minor")) return null;
        return new NativeCompatibilityEnrollment.ApprovedWindow(
                config.getInt(prefix + "major"),
                config.getInt(prefix + "lower-minor"),
                config.getInt(prefix + "upper-minor"));
    }
}
