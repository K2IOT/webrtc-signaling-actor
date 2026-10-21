package io.webrtc.signaling.app.runtime.local;

import io.webrtc.signaling.app.runtime.local.LocalRuntimeFailure.Category;
import io.webrtc.signaling.app.runtime.local.LocalRuntimeFailure.Role;
import io.webrtc.signaling.storage.DbClass;
import io.webrtc.signaling.storage.DbResourceProfile;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static io.webrtc.signaling.app.runtime.local.LocalRuntimeFixtures.JSON;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Field validation of the process profile, driven through the same parser the loader uses. */
class LocalRuntimeProfileTest {
    @TempDir
    Path mount;

    @Test
    void validProfilesParseForEveryPlane() throws Exception {
        for (var plane : List.of("ACTOR", "GATEWAY", "CONTROL")) {
            var document = LocalRuntimeFixtures.profile(mount, plane);
            if (plane.equals("GATEWAY")) {
                document.put("gatewayId", "local-c001");
                document.put("gatewayBootId", "00000000-0000-4000-8000-0000000000aa");
            }
            assertThat(parse(document).plane().name()).isEqualTo(plane);
        }
    }

    @Test
    void controlTransferCellsGetLocalResourceProfilesFromTheDatabaseEnrollment() throws Exception {
        var document = LocalRuntimeFixtures.profile(mount, "CONTROL");
        document.put(
                "transferCells",
                List.of(
                        LocalRuntimeFixtures.transferCell("c001", 1),
                        LocalRuntimeFixtures.transferCell("c002", 2)));
        var profile = parse(document);
        assertThat(profile.transferCells()).extracting(t -> t.cell()).containsExactly("c001", "c002");
        var database = profile.transferCells().get(1).database();
        assertThat(profile.transferCells().get(1).storageEpoch()).isEqualTo(2);
        assertThat(database.resourceProfiles())
                .isEqualTo(DbResourceProfile.localTestOnly(database.quotas()))
                .containsKey(DbClass.CRITICAL);
        assertThat(profile.toString()).doesNotContain("TEST_ONLY_PRIVATE");
        assertThat(database.toString()).doesNotContain("TEST_ONLY_PRIVATE");
    }

    @Test
    void onlyTheControlProfileMayCarryTransferCells() throws Exception {
        for (var plane : List.of("ACTOR", "GATEWAY")) {
            var document = LocalRuntimeFixtures.profile(mount, plane);
            if (plane.equals("GATEWAY")) {
                document.put("gatewayId", "local-c001");
                document.put("gatewayBootId", "00000000-0000-4000-8000-0000000000aa");
            }
            document.put("transferCells", List.of(LocalRuntimeFixtures.transferCell("c002", 2)));
            assertInvalid(document);
        }
    }

    @ParameterizedTest
    @ValueSource(
            strings = {
                    "jdbc:postgresql://postgres.c002.svc:5432/signaling",
                    "jdbc:postgresql://postgres.c002.svc:5432/signaling?sslmode=require",
                    "jdbc:postgresql://postgres.c002.svc:5432/signaling?sslmode=verify-ca",
                    "jdbc:postgresql://postgres.c002.svc:5432/signaling?sslmode=verify-full-not",
                    "jdbc:postgresql://postgres.c002.svc:5432/signaling?sslmode=verify-full&sslmode=disable",
                    "jdbc:postgresql://postgres.c002.svc:5432/signaling?sslmode=disable&sslmode=verify-full",
                    "jdbc:postgresql://postgres.c002.svc:5432/signaling?xsslmode=verify-full",
                    "jdbc:postgresql://postgres.c002.svc:5432/signaling?application_name=sslmode=verify-full",
                    "jdbc:postgresql://postgres.c002.svc:5432/sslmode=verify-full",
                    "jdbc:postgresql://postgres.c002.svc:5432/signaling#sslmode=verify-full"
            })
    void transferCellDatabaseRequiresExactlyOneVerifyFullParameter(String url) throws Exception {
        var cell = LocalRuntimeFixtures.transferCell("c002", 2);
        @SuppressWarnings("unchecked")
        var database = (Map<String, Object>) cell.get("database");
        database.put("jdbcUrl", url);
        var document = LocalRuntimeFixtures.profile(mount, "CONTROL");
        document.put("transferCells", List.of(cell));
        assertInvalid(document);
    }

    @Test
    void transferCellDatabaseAcceptsVerifyFullAmongOtherParameters() throws Exception {
        var cell = LocalRuntimeFixtures.transferCell("c002", 2);
        @SuppressWarnings("unchecked")
        var database = (Map<String, Object>) cell.get("database");
        database.put(
                "jdbcUrl",
                "jdbc:postgresql://postgres.c002.svc:5432/signaling?ApplicationName=a&sslmode=verify-full&x=1");
        var document = LocalRuntimeFixtures.profile(mount, "CONTROL");
        document.put("transferCells", List.of(cell));
        assertThat(parse(document).transferCells()).hasSize(1);
    }

    @Test
    void transferCellsMustBeDistinctBoundedAndWellFormed() throws Exception {
        var document = LocalRuntimeFixtures.profile(mount, "CONTROL");
        document.put(
                "transferCells",
                List.of(
                        LocalRuntimeFixtures.transferCell("c002", 2),
                        LocalRuntimeFixtures.transferCell("c002", 3)));
        assertInvalid(document);

        var eight = new ArrayList<Map<String, Object>>();
        for (int i = 1; i <= 8; i++)
            eight.add(LocalRuntimeFixtures.transferCell(String.format("c%03d", i), i));
        document.put("transferCells", eight);
        assertThat(parse(document).transferCells()).hasSize(8);
        eight.add(LocalRuntimeFixtures.transferCell("c009", 9));
        document.put("transferCells", eight);
        assertInvalid(document);

        for (var bad :
                List.<Consumer<Map<String, Object>>>of(
                        cell -> cell.put("cell", "c2"),
                        cell -> cell.put("cell", null),
                        cell -> cell.put("storageEpoch", 0),
                        cell -> cell.put("database", null),
                        cell -> cell.put("extra", 1),
                        cell -> cell.remove("storageEpoch"),
                        cell -> database(cell).put("resourceProfiles", Map.of()),
                        cell -> database(cell).put("extra", 1),
                        cell -> database(cell).remove("quotas"),
                        cell -> database(cell).put("safetyMax", 0),
                        cell -> database(cell).put("username", ""))) {
            var cell = LocalRuntimeFixtures.transferCell("c002", 2);
            bad.accept(cell);
            document.put("transferCells", List.of(cell));
            assertInvalid(document);
        }
    }

    @Test
    void gatewayIdentityIsAllOrNothingAndWellFormed() throws Exception {
        var document = LocalRuntimeFixtures.profile(mount, "GATEWAY");
        document.put("gatewayId", "local-c001");
        assertInvalid(document);
        document.put("gatewayId", null);
        document.put("gatewayBootId", "00000000-0000-4000-8000-0000000000aa");
        assertInvalid(document);
        document.put("gatewayBootId", "not-a-uuid");
        document.put("gatewayId", "local-c001");
        assertInvalid(document);
        for (var id : List.of("", "has space", "a".repeat(129), "slash/x")) {
            document.put("gatewayId", id);
            document.put("gatewayBootId", "00000000-0000-4000-8000-0000000000aa");
            assertInvalid(document);
        }
        document.put("gatewayId", null);
        document.put("gatewayBootId", null);
        assertThat(parse(document).gatewayId()).isNull();
    }

    @ParameterizedTest
    @ValueSource(
            strings = {
                    "",
                    "localhost",
                    "192.0.2",
                    "192.0.2.256",
                    "192.0.2.1.5",
                    "192.0.2.-1",
                    " 192.0.2.1",
                    "::1",
                    "fe80::1",
                    "192.0.2.1/24",
                    "postgres.c001.svc",
                    "0x7f.0.0.1"
            })
    void podIpMustBeADottedQuadLiteral(String ip) throws Exception {
        var document = LocalRuntimeFixtures.profile(mount, "ACTOR");
        document.put("podIp", ip);
        assertInvalid(document);
        document.put("podIp", null);
        assertInvalid(document);
    }

    @ParameterizedTest
    @CsvSource({"192.0.2.1", "198.51.100.254", "203.0.113.0", "10.0.0.255"})
    void podIpAcceptsDottedQuads(String ip) throws Exception {
        var document = LocalRuntimeFixtures.profile(mount, "ACTOR");
        document.put("podIp", ip);
        assertThat(parse(document).podIp()).isEqualTo(ip);
    }

    @Test
    void walRulesAreEnforced() throws Exception {
        for (var bad :
                List.<Consumer<Map<String, Object>>>of(
                        wal -> wal.put("systemIdentifier", ""),
                        wal -> wal.put("systemIdentifier", "abc"),
                        wal -> wal.put("systemIdentifier", "1".repeat(21)),
                        wal -> wal.put("timeline", 0),
                        wal -> wal.put("maximumLagBytes", -1),
                        wal -> wal.put("replicas", List.of("")),
                        wal -> wal.put("replicas", List.of("bad replica")),
                        wal -> wal.put("replicas", numbered(17)),
                        wal -> wal.put("synchronousReplicas", -1),
                        wal -> wal.put("synchronousReplicas", 2),
                        wal -> wal.put("freshness", "PT0S"),
                        wal -> wal.put("freshness", "-PT1S"),
                        wal -> wal.put("freshness", "1s"),
                        wal -> wal.put("maximumReplyAge", "PT0S"),
                        wal -> wal.put("maximumReplyAge", 2),
                        wal -> wal.put("extra", true))) {
            var document = LocalRuntimeFixtures.profile(mount, "ACTOR");
            bad.accept(wal(document));
            assertInvalid(document);
        }
        var document = LocalRuntimeFixtures.profile(mount, "ACTOR");
        wal(document).put("synchronousReplicas", 0);
        wal(document).put("replicas", List.of());
        assertThat(parse(document).wal().replicas()).isEmpty();
    }

    @Test
    void compatibilityRulesAreEnforced() throws Exception {
        for (var bad :
                List.<Consumer<Map<String, Object>>>of(
                        c -> c.put("major", -1),
                        c -> c.put("minor", -1),
                        c -> c.put("lowerMinor", -1),
                        c -> {
                            c.put("lowerMinor", 3);
                            c.put("upperMinor", 2);
                        },
                        c -> c.put("executableId", ""),
                        c -> c.put("executableId", "has space"),
                        c -> c.put("executableId", "x".repeat(129)),
                        c -> c.put("features", List.of("")),
                        c -> c.put("features", List.of("bad feature")),
                        c -> c.put("features", numbered(65)),
                        c -> c.put("extra", 1))) {
            var document = LocalRuntimeFixtures.profile(mount, "ACTOR");
            bad.accept(section(document, "compatibility"));
            assertInvalid(document);
        }
    }

    @Test
    void kubeRulesAreEnforced() throws Exception {
        for (var bad :
                List.<Consumer<Map<String, Object>>>of(
                        k -> k.put("apiHost", ""),
                        k -> k.put("apiHost", "bad host"),
                        k -> k.put("apiHost", "h".repeat(254)),
                        k -> k.put("apiPort", 0),
                        k -> k.put("apiPort", 65536),
                        k -> k.put("caPath", "relative/ca.crt"),
                        k -> k.put("tokenPath", "token"),
                        k -> k.put("caPath", "/" + "a".repeat(512)),
                        k -> k.put("tokenPath", null),
                        k -> k.put("namespace", "Upper"),
                        k -> k.put("namespace", "-lead"),
                        k -> k.put("namespace", "trail-"),
                        k -> k.put("namespace", "a".repeat(64)),
                        k -> k.put("namespace", ""),
                        k -> k.put("extra", 1))) {
            var document = LocalRuntimeFixtures.profile(mount, "ACTOR");
            bad.accept(section(document, "kube"));
            assertInvalid(document);
        }
    }

    @Test
    void cellZonesAreTwoToThreeDistinctAzRolesOnActorProfilesOnly() throws Exception {
        var document = LocalRuntimeFixtures.profile(mount, "ACTOR");
        assertThat(parse(document).cellAzRoles()).containsExactlyInAnyOrder("az-a", "az-b");
        document.put("cellAzRoles", List.of("az-a", "az-b", "az-c"));
        assertThat(parse(document).cellAzRoles()).hasSize(3);
        document.put("cellAzRoles", null);
        assertThat(parse(document).cellAzRoles()).isEmpty();
        for (var bad :
                List.<List<String>>of(
                        List.of("az-a"),
                        List.of("az-a", "az-a"),
                        List.of("az-a", "az-b", "az-c", "az-d"),
                        List.of("az-a", "zone-b"),
                        List.of("az-a", "az-B"),
                        List.of("az-a", "az-"),
                        List.of("az-a", "az-" + "b".repeat(64)))) {
            var invalid = LocalRuntimeFixtures.profile(mount, "ACTOR");
            invalid.put("cellAzRoles", bad);
            assertInvalid(invalid);
        }
        var absent = LocalRuntimeFixtures.profile(mount, "ACTOR");
        absent.remove("cellAzRoles");
        assertInvalid(absent);
        for (var plane : List.of("GATEWAY", "CONTROL")) {
            var other = LocalRuntimeFixtures.profile(mount, plane);
            if (plane.equals("GATEWAY")) {
                other.put("gatewayId", "local-c001");
                other.put("gatewayBootId", "00000000-0000-4000-8000-0000000000aa");
            }
            assertThat(parse(other).cellAzRoles()).isEmpty();
            other.put("cellAzRoles", List.of("az-a", "az-b"));
            assertInvalid(other);
        }
    }

    // helpers

    private static List<String> numbered(int count) {
        var names = new ArrayList<String>();
        for (int i = 0; i < count; i++) names.add("n" + i);
        return names;
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> database(Map<String, Object> cell) {
        return (Map<String, Object>) cell.get("database");
    }

    private static Map<String, Object> wal(Map<String, Object> document) {
        return section(document, "wal");
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> section(Map<String, Object> document, String name) {
        return (Map<String, Object>) document.get(name);
    }

    private static LocalProcessProfile parse(Map<String, Object> document) throws Exception {
        return LocalProcessProfile.parse(JSON.writeValueAsString(document));
    }

    private static void assertInvalid(Map<String, Object> document) throws Exception {
        String text = JSON.writeValueAsString(document);
        assertThatThrownBy(() -> LocalProcessProfile.parse(text))
                .isInstanceOfSatisfying(
                        LocalRuntimeFailure.class,
                        failure -> {
                            assertThat(failure.role()).isEqualTo(Role.PROFILE);
                            assertThat(failure.category()).isEqualTo(Category.INVALID);
                            assertThat(failure).hasNoCause();
                        });
    }
}
