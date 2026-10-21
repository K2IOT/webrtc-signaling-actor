package io.webrtc.signaling.app.runtime.local;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.webrtc.signaling.app.SignalingApplication.Plane;
import io.webrtc.signaling.app.runtime.NativeControlDatabaseEnrollment;
import io.webrtc.signaling.storage.DbClass;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;

/**
 * Process-level facts for one LOCAL_TEST_ONLY container. It sits beside the mounted native contract
 * (schema v1, unchanged) and is parsed with the same bounded, strict reader: unknown, duplicate,
 * missing and trailing content is rejected. Optional sections are explicit {@code null}.
 */
public record LocalProcessProfile(
        int schemaVersion,
        Plane plane,
        UUID podUid,
        String podIp,
        int businessPort,
        int healthPort,
        int relayPort,
        String azRole,
        String gatewayId,
        UUID gatewayBootId,
        Wal wal,
        Compatibility compatibility,
        Kube kube,
        List<TransferCell> transferCells,
        String privacyPseudonymKey,
        List<String> allowedOrigins) {
    public LocalProcessProfile {
        if (schemaVersion != 1) throw new IllegalArgumentException();
        Objects.requireNonNull(plane);
        Objects.requireNonNull(podUid);
        if (podIp == null || !ipv4(podIp)) throw new IllegalArgumentException();
        validPort(businessPort);
        validPort(healthPort);
        validPort(relayPort);
        if (businessPort == healthPort) throw new IllegalArgumentException();
        if (azRole != null && !azRole.matches("[A-Za-z0-9_.-]{1,64}"))
            throw new IllegalArgumentException();
        if ((gatewayId == null) != (gatewayBootId == null)) throw new IllegalArgumentException();
        if (gatewayId != null && !gatewayId.matches("[A-Za-z0-9_.-]{1,128}"))
            throw new IllegalArgumentException();
        transferCells = transferCells == null ? List.of() : List.copyOf(transferCells);
        // Only the control profile may hold credentials for more than one cell database.
        if (plane != Plane.CONTROL && !transferCells.isEmpty()) throw new IllegalArgumentException();
        if (transferCells.size() > 8
                || transferCells.stream().map(TransferCell::cell).distinct().count()
                != transferCells.size()) throw new IllegalArgumentException();
        if (privacyPseudonymKey != null
                && (privacyPseudonymKey.isBlank() || privacyPseudonymKey.length() > 512))
            throw new IllegalArgumentException();
        allowedOrigins = allowedOrigins == null ? List.of() : List.copyOf(allowedOrigins);
        if (allowedOrigins.size() > 32
                || allowedOrigins.stream()
                .anyMatch(o -> !o.matches("https?://[A-Za-z0-9.-]{1,253}(:[0-9]{1,5})?")))
            throw new IllegalArgumentException();
    }

    /**
     * Parses already size-checked UTF-8 text. Failures carry only a category, never the input.
     */
    static LocalProcessProfile parse(String text) {
        var document = LocalJson.tree(text, LocalRuntimeFailure.Role.PROFILE);
        try {
            // Durations are ISO-8601 text; Jackson would also accept a bare number of seconds.
            for (var name : List.of("freshness", "maximumReplyAge")) {
                var value = document.path("wal").path(name);
                if (!value.isMissingNode() && !value.isTextual()) throw new IllegalArgumentException();
            }
            return LocalJson.MAPPER.treeToValue(document, LocalProcessProfile.class);
        } catch (Exception invalid) {
            // Jackson diagnostics may include private input; retain neither fragments nor causes.
            throw LocalJson.failure(
                    LocalRuntimeFailure.Role.PROFILE, LocalRuntimeFailure.Category.INVALID);
        }
    }

    private static void validPort(int port) {
        if (port < 1 || port > 65535) throw new IllegalArgumentException();
    }

    /** Literal dotted quad only, so validation never reaches name resolution. */
    private static boolean ipv4(String value) {
        if (!value.matches("[0-9]{1,3}(\\.[0-9]{1,3}){3}")) return false;
        for (var octet : value.split("\\.")) if (Integer.parseInt(octet) > 255) return false;
        return true;
    }

    @Override
    public String toString() {
        return "LocalProcessProfile[LOCAL_TEST_ONLY,plane=" + plane + ",credentials=redacted]";
    }

    public record Wal(
            String systemIdentifier,
            long timeline,
            Set<String> replicas,
            int synchronousReplicas,
            long maximumLagBytes,
            Duration freshness,
            Duration maximumReplyAge) {
        public Wal {
            if (systemIdentifier == null || !systemIdentifier.matches("[0-9]{1,20}"))
                throw new IllegalArgumentException();
            if (timeline < 1 || maximumLagBytes < 0) throw new IllegalArgumentException();
            replicas = Set.copyOf(replicas);
            if (replicas.size() > 16
                    || replicas.stream().anyMatch(r -> r == null || !r.matches("[A-Za-z0-9_.-]{1,128}"))
                    || synchronousReplicas < 0
                    || synchronousReplicas > replicas.size()) throw new IllegalArgumentException();
            positive(freshness);
            positive(maximumReplyAge);
        }
    }

    public record Compatibility(
            int major,
            int minor,
            String executableId,
            Set<String> features,
            int lowerMinor,
            int upperMinor) {
        public Compatibility {
            if (major < 0 || minor < 0 || lowerMinor < 0 || upperMinor < lowerMinor)
                throw new IllegalArgumentException();
            if (executableId == null || !executableId.matches("[A-Za-z0-9_.:-]{1,128}"))
                throw new IllegalArgumentException();
            features = Set.copyOf(features);
            if (features.size() > 64
                    || features.stream().anyMatch(f -> !f.matches("[A-Za-z0-9_.:-]{1,128}")))
                throw new IllegalArgumentException();
        }
    }

    /**
     * Pekko discovery inputs. Both paths are absolute here and are confined under the contract
     * directory by {@link LocalRuntimeContract#load}, like every other path.
     */
    public record Kube(
            String apiHost, int apiPort, String caPath, String tokenPath, String namespace) {
        public Kube {
            if (apiHost == null || apiHost.length() > 253 || !apiHost.matches("[A-Za-z0-9.-]+"))
                throw new IllegalArgumentException();
            validPort(apiPort);
            absolute(caPath);
            absolute(tokenPath);
            if (namespace == null || !namespace.matches("[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?"))
                throw new IllegalArgumentException();
        }

        private static void absolute(String path) {
            if (path == null || path.length() > 512 || !path.startsWith("/"))
                throw new IllegalArgumentException();
        }
    }

    public record TransferCell(
            String cell, long storageEpoch, NativeControlDatabaseEnrollment.Database database) {
        public TransferCell {
            if (cell == null || !cell.matches("c[0-9]{3}") || storageEpoch < 1)
                throw new IllegalArgumentException();
            Objects.requireNonNull(database);
            if (!verifyFull(database.jdbcUrl())) throw new IllegalArgumentException();
        }

        /**
         * The file carries counts only. The local database enrollment constructor derives the
         * LOCAL_TEST_ONLY resource profiles, as for the mounted contract's own databases.
         */
        @JsonCreator
        static TransferCell fromFile(
                @JsonProperty("cell") String cell,
                @JsonProperty("storageEpoch") long storageEpoch,
                @JsonProperty("database") DatabaseFile database) {
            Objects.requireNonNull(database);
            return new TransferCell(
                    cell,
                    storageEpoch,
                    new NativeControlDatabaseEnrollment.Database(
                            database.jdbcUrl(),
                            database.username(),
                            database.password(),
                            database.quotas(),
                            database.safetyMax(),
                            database.controlMax()));
        }

        /** Exactly one {@code sslmode} URL parameter, with the value {@code verify-full}. */
        static boolean verifyFull(String jdbcUrl) {
            int query = jdbcUrl.indexOf('?');
            if (query < 0) return false;
            int found = 0;
            for (var parameter : jdbcUrl.substring(query + 1).split("&", -1)) {
                int equals = parameter.indexOf('=');
                if (equals < 0 || !parameter.substring(0, equals).equals("sslmode")) continue;
                if (!parameter.substring(equals + 1).equals("verify-full")) return false;
                found++;
            }
            return found == 1;
        }
    }

    /** Wire shape of a transfer cell database: counts only, no resource profiles. */
    record DatabaseFile(
            String jdbcUrl,
            String username,
            String password,
            Map<DbClass, Integer> quotas,
            int safetyMax,
            int controlMax) {
        @Override
        public String toString() {
            return "DatabaseFile[credentials=redacted]";
        }
    }

    private static void positive(Duration value) {
        if (value == null || value.isZero() || value.isNegative()) throw new IllegalArgumentException();
    }
}
