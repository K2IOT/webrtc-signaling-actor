package io.webrtc.signaling.app.runtime.local;

import io.webrtc.signaling.app.SignalingApplication.Plane;
import io.webrtc.signaling.app.runtime.local.LocalRuntimeFailure.Category;
import io.webrtc.signaling.app.runtime.local.LocalRuntimeFailure.Role;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static io.webrtc.signaling.app.runtime.local.LocalRuntimeFixtures.JSON;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class LocalRuntimeContractTest {
    static final String SECRET = "TEST_ONLY_PRIVATE_PASSWORD";

    @TempDir
    Path mount;

    static org.springframework.mock.env.MockEnvironment identity(String issuer, String audience) {
        var env = new org.springframework.mock.env.MockEnvironment();
        if (issuer != null) env.setProperty("SIGNALING_IDENTITY_ISSUER", issuer);
        if (audience != null) env.setProperty("SIGNALING_IDENTITY_AUDIENCE", audience);
        return env;
    }

    @Test
    void contractIdentityMustEqualTheEffectiveSpringIdentity() throws Exception {
        var c = LocalRuntimeContract.load(LocalRuntimeFixtures.install(mount, "ACTOR"));
        c.requireIdentity(identity("LOCAL_TEST_ONLY_ISSUER", "LOCAL_TEST_ONLY_AUDIENCE"));
        // The property names SignalingProperties binds are equally authoritative.
        c.requireIdentity(
                new org.springframework.mock.env.MockEnvironment()
                        .withProperty("signaling.identity.issuer", "LOCAL_TEST_ONLY_ISSUER")
                        .withProperty("signaling.identity.audience", "LOCAL_TEST_ONLY_AUDIENCE"));

        for (var env :
                List.of(
                        identity("TEST_ONLY_SECRET_ISSUER", "LOCAL_TEST_ONLY_AUDIENCE"),
                        identity("LOCAL_TEST_ONLY_ISSUER", "TEST_ONLY_SECRET_AUDIENCE"),
                        identity(null, "LOCAL_TEST_ONLY_AUDIENCE"),
                        identity("LOCAL_TEST_ONLY_ISSUER", null),
                        identity("", "LOCAL_TEST_ONLY_AUDIENCE"),
                        identity("local_test_only_issuer", "LOCAL_TEST_ONLY_AUDIENCE"),
                        // The variable and the bound property disagree: neither may be ignored.
                        identity("LOCAL_TEST_ONLY_ISSUER", "LOCAL_TEST_ONLY_AUDIENCE")
                                .withProperty("signaling.identity.issuer", "TEST_ONLY_SECRET_ISSUER")))
            assertFailure(() -> c.requireIdentity(env), Role.IDENTITY, Category.IDENTITY_MISMATCH);
    }

    @Test
    void loadsContractAndProfileFromTheMountedDirectory() throws Exception {
        var loaded = LocalRuntimeContract.load(LocalRuntimeFixtures.install(mount, "ACTOR"));
        assertThat(loaded.contract().cell()).isEqualTo("c001");
        assertThat(loaded.profile().plane()).isEqualTo(Plane.ACTOR);
        assertThat(loaded.profile().wal().freshness()).isEqualTo(Duration.ofSeconds(1));
        assertThat(loaded.profile().transferCells()).isEmpty();
        assertThat(loaded.directory()).isEqualTo(mount.toRealPath());
        assertThat(loaded.toString()).doesNotContain("TEST_ONLY_PRIVATE");
        assertThat(loaded.profile().toString()).doesNotContain("TEST_ONLY_PRIVATE");
    }

    @Test
    void rejectsPodUidDifferentFromEnvironment() throws Exception {
        var env = LocalRuntimeFixtures.install(mount, "ACTOR");
        env.setProperty("SIGNALING_POD_UID", "00000000-0000-4000-8000-000000000002");
        assertFailure(() -> LocalRuntimeContract.load(env), Role.IDENTITY, Category.IDENTITY_MISMATCH);
        var absent = LocalRuntimeFixtures.environment(mount);
        ((java.util.Properties) absent.getPropertySources().get("mockProperties").getSource())
                .remove("SIGNALING_POD_UID");
        assertFailure(() -> LocalRuntimeContract.load(absent), Role.IDENTITY, Category.IDENTITY_MISMATCH);
    }

    @ParameterizedTest
    @ValueSource(
            strings = {
                    // Parses to the profile's own UUID but is not canonical text.
                    "0-0-4000-8000-1",
                    "00000000-0000-4000-8000-00000000000001",
                    " 00000000-0000-4000-8000-000000000001",
                    "{00000000-0000-4000-8000-000000000001}",
                    "00000000-0000-4000-8000-000000000001 ",
                    ""
            })
    void rejectsNonCanonicalPodUid(String text) throws Exception {
        var env = LocalRuntimeFixtures.install(mount, "ACTOR");
        env.setProperty("SIGNALING_POD_UID", text);
        assertFailure(() -> LocalRuntimeContract.load(env), Role.IDENTITY, Category.IDENTITY_MISMATCH);
    }

    @Test
    void rejectsUpperCasePodUid() throws Exception {
        var profile = LocalRuntimeFixtures.profile(mount, "ACTOR");
        profile.put("podUid", "0000000a-0000-4000-8000-00000000000b");
        var env = LocalRuntimeFixtures.install(mount, "ACTOR");
        Files.writeString(mount.resolve("process.json"), JSON.writeValueAsString(profile));
        env.setProperty("SIGNALING_POD_UID", "0000000a-0000-4000-8000-00000000000b");
        assertThat(LocalRuntimeContract.load(env).profile().podUid().toString())
                .isEqualTo("0000000a-0000-4000-8000-00000000000b");
        env.setProperty("SIGNALING_POD_UID", "0000000A-0000-4000-8000-00000000000B");
        assertFailure(() -> LocalRuntimeContract.load(env), Role.IDENTITY, Category.IDENTITY_MISMATCH);
    }

    @Test
    void rejectsCellDifferentFromEnvironment() throws Exception {
        var env = LocalRuntimeFixtures.install(mount, "ACTOR");
        env.setProperty("SIGNALING_CELL_ID", "c002");
        assertFailure(() -> LocalRuntimeContract.load(env), Role.IDENTITY, Category.IDENTITY_MISMATCH);
    }

    @Test
    void rejectsRelativeEscapingAndSymlinkPaths() throws Exception {
        var loaded = LocalRuntimeContract.load(LocalRuntimeFixtures.install(mount, "ACTOR"));
        var outside = Files.createTempDirectory("local-runtime-outside");
        try {
            Files.writeString(outside.resolve("secret"), "x");
            Files.writeString(mount.resolve("inside"), "x");
            Files.createSymbolicLink(mount.resolve("escape"), outside.resolve("secret"));
            Files.createSymbolicLink(mount.resolve("escapedir"), outside);
            assertThat(loaded.resolve(mount.resolve("inside").toString()))
                    .isEqualTo(mount.resolve("inside").toRealPath());
            for (var bad :
                    List.of(
                            "inside",
                            "./inside",
                            mount.resolve("../" + outside.getFileName() + "/secret").toString(),
                            mount.resolve("escape").toString(),
                            mount.resolve("escapedir/secret").toString(),
                            outside.resolve("secret").toString(),
                            mount + "/" + "a".repeat(600))) {
                var failure = catchFailure(() -> loaded.resolve(bad));
                assertThat(failure.role()).as(bad).isEqualTo(Role.PATH);
                assertThat(failure.category()).as(bad).isEqualTo(Category.PATH_ESCAPE);
            }
            assertThat(catchFailure(() -> loaded.resolve(mount.resolve("missing").toString())).category())
                    .isEqualTo(Category.MISSING);
            assertThat(catchFailure(() -> loaded.resolve(null)).category()).isEqualTo(Category.PATH_ESCAPE);
        } finally {
            Files.deleteIfExists(outside.resolve("secret"));
            Files.deleteIfExists(outside);
        }
    }

    @Test
    void kubePathsAreConfinedLikeEveryOtherPath() throws Exception {
        var env = LocalRuntimeFixtures.install(mount, "ACTOR");
        var outside = Files.createTempDirectory("local-runtime-kube");
        try {
            Files.writeString(outside.resolve("token"), "x");
            for (var field : List.of("caPath", "tokenPath")) {
                for (var bad :
                        List.of(
                                outside.resolve("token").toString(),
                                "/var/run/secrets/kubernetes.io/serviceaccount/token")) {
                    writeProfile(profileWithKube(field, bad));
                    var failure = catchFailure(() -> LocalRuntimeContract.load(env));
                    assertThat(failure.role()).as(field + bad).isEqualTo(Role.PROFILE);
                    assertThat(failure.category())
                            .as(field + bad)
                            .isIn(Category.PATH_ESCAPE, Category.MISSING);
                }
            }
            // A symlink inside the directory that leaves it is refused too.
            Files.createSymbolicLink(mount.resolve("kube-link"), outside.resolve("token"));
            writeProfile(profileWithKube("tokenPath", mount.resolve("kube-link").toString()));
            assertFailure(() -> LocalRuntimeContract.load(env), Role.PROFILE, Category.PATH_ESCAPE);
            writeProfile(profileWithKube("caPath", mount.resolve("kube-missing").toString()));
            assertFailure(() -> LocalRuntimeContract.load(env), Role.PROFILE, Category.MISSING);
            writeProfile(LocalRuntimeFixtures.profile(mount, "ACTOR"));
            assertThat(LocalRuntimeContract.load(env).profile().kube()).isNotNull();
        } finally {
            Files.deleteIfExists(outside.resolve("token"));
            Files.deleteIfExists(outside);
        }
    }

    @Test
    void rejectsProfileOutsideTheContractDirectoryOrThroughAnEscapingSymlink() throws Exception {
        var env = LocalRuntimeFixtures.install(mount, "ACTOR");
        var other = Files.createTempDirectory("local-runtime-other");
        try {
            Files.writeString(
                    other.resolve("process.json"),
                    JSON.writeValueAsString(LocalRuntimeFixtures.profile(mount, "ACTOR")));
            env.setProperty(
                    "SIGNALING_LOCAL_PROCESS_PROFILE", other.resolve("process.json").toString());
            assertFailure(() -> LocalRuntimeContract.load(env), Role.PROFILE, Category.PATH_ESCAPE);
            Files.delete(mount.resolve("process.json"));
            Files.createSymbolicLink(mount.resolve("process.json"), other.resolve("process.json"));
            env.setProperty(
                    "SIGNALING_LOCAL_PROCESS_PROFILE", mount.resolve("process.json").toString());
            assertFailure(() -> LocalRuntimeContract.load(env), Role.PROFILE, Category.PATH_ESCAPE);
        } finally {
            Files.deleteIfExists(mount.resolve("process.json"));
            Files.deleteIfExists(other.resolve("process.json"));
            Files.deleteIfExists(other);
        }
    }

    @Test
    void projectedSecretSymlinkInsideTheDirectoryIsSupported() throws Exception {
        var env = LocalRuntimeFixtures.install(mount, "ACTOR");
        var data = Files.createDirectory(mount.resolve("..data"));
        Files.move(mount.resolve("process.json"), data.resolve("process.json"));
        Files.createSymbolicLink(mount.resolve("process.json"), Path.of("..data/process.json"));
        assertThat(LocalRuntimeContract.load(env).profile().podUid())
                .isEqualTo(LocalRuntimeFixtures.POD_UID);
    }

    @Test
    void namesTheCategoryOfAnUnusableProfile() throws Exception {
        var env = LocalRuntimeFixtures.install(mount, "ACTOR");
        String valid = JSON.writeValueAsString(LocalRuntimeFixtures.profile(mount, "ACTOR"));
        var cases = new LinkedHashMap<String, Category>();
        cases.put(valid.substring(0, valid.length() - 1) + ",\"password\":\"" + SECRET + "\"}", Category.INVALID);
        cases.put(valid.substring(0, valid.length() - 1) + ",\"plane\":\"GATEWAY\"}", Category.DUPLICATE_KEY);
        cases.put(
                valid.replace("\"healthPort\":9090", "\"healthPort\":9090,\"healthPort\":9091"),
                Category.DUPLICATE_KEY);
        cases.put(valid.replace("\"schemaVersion\":1", "\"schemaVersion\":2"), Category.INVALID);
        cases.put(valid.replace("\"schemaVersion\":1", "\"schemaVersion\":\"1\""), Category.INVALID);
        cases.put(valid.replace("\"freshness\":\"PT1S\"", "\"freshness\":1"), Category.INVALID);
        cases.put(valid.replace("\"freshness\":\"PT1S\"", "\"freshness\":\"PT0S\""), Category.INVALID);
        cases.put(valid.replace("\"podIp\":\"192.0.2.10\",", ""), Category.INVALID);
        cases.put(valid.replace("\"plane\":\"ACTOR\"", "\"plane\":\"actor\""), Category.INVALID);
        cases.put(valid.replace("\"wal\":{", "\"wal\":{\"extra\":1,"), Category.INVALID);
        cases.put(valid + "{}", Category.MALFORMED);
        cases.put(valid.substring(0, valid.length() - 1), Category.MALFORMED);
        cases.put("not json", Category.MALFORMED);
        cases.put("   ", Category.MALFORMED);
        cases.put("{\"a\":{\"b\":{\"c\":{\"d\":{\"e\":{\"f\":{\"g\":{\"h\":{\"i\":1}}}}}}}}}", Category.MALFORMED);
        cases.put(" ".repeat(65537), Category.TOO_LARGE);
        for (var entry : cases.entrySet()) {
            Files.writeString(mount.resolve("process.json"), entry.getKey());
            var failure = catchFailure(() -> LocalRuntimeContract.load(env));
            assertThat(failure.role()).isEqualTo(Role.PROFILE);
            assertThat(failure.category())
                    .as(entry.getKey().length() > 120 ? entry.getKey().substring(0, 120) : entry.getKey())
                    .isEqualTo(entry.getValue());
            assertThat(failure).hasMessageNotContaining(SECRET).hasNoCause();
        }
        Files.write(mount.resolve("process.json"), new byte[] {(byte) 0xff, (byte) 0xfe, '{', '}'});
        assertFailure(() -> LocalRuntimeContract.load(env), Role.PROFILE, Category.MALFORMED);
        Files.delete(mount.resolve("process.json"));
        assertFailure(() -> LocalRuntimeContract.load(env), Role.PROFILE, Category.MISSING);
    }

    @Test
    void namesTheCategoryOfAnUnusableContractWithoutEchoingIt() throws Exception {
        var env = LocalRuntimeFixtures.install(mount, "ACTOR");
        String valid = JSON.writeValueAsString(LocalRuntimeFixtures.contract(mount));
        var cases = new LinkedHashMap<String, Category>();
        cases.put(valid.substring(0, valid.length() - 1) + ",\"cell\":\"c002\"}", Category.DUPLICATE_KEY);
        cases.put(valid.substring(0, valid.length() - 1), Category.MALFORMED);
        cases.put("{", Category.MALFORMED);
        cases.put(valid + "{}", Category.MALFORMED);
        cases.put(valid.replace("\"storageEpoch\":1,\"routingEpoch\":1", "\"storageEpoch\":0,\"routingEpoch\":1"), Category.INVALID);
        cases.put(valid.substring(0, valid.length() - 1) + ",\"password\":\"" + SECRET + "\"}", Category.INVALID);
        cases.put(valid.replace("\"local-test-only\"", "\"production\""), Category.ENVIRONMENT_MISMATCH);
        cases.put(" ".repeat(65537), Category.TOO_LARGE);
        for (var entry : cases.entrySet()) {
            Files.writeString(mount.resolve("provider.json"), entry.getKey());
            var failure = catchFailure(() -> LocalRuntimeContract.load(env));
            assertThat(failure.role()).isEqualTo(Role.CONTRACT);
            assertThat(failure.category())
                    .as(entry.getKey().length() > 120 ? entry.getKey().substring(0, 120) : entry.getKey())
                    .isEqualTo(entry.getValue());
            assertThat(failure).hasMessageNotContaining(SECRET).hasNoCause();
        }
        Files.write(mount.resolve("provider.json"), new byte[] {(byte) 0xff, (byte) 0xfe, '{', '}'});
        assertFailure(() -> LocalRuntimeContract.load(env), Role.CONTRACT, Category.MALFORMED);
        Files.delete(mount.resolve("provider.json"));
        assertFailure(() -> LocalRuntimeContract.load(env), Role.CONTRACT, Category.MISSING);
    }

    @ParameterizedTest
    @ValueSource(strings = {"businessPort", "healthPort", "relayPort"})
    void rejectsOutOfRangePorts(String port) throws Exception {
        var env = LocalRuntimeFixtures.install(mount, "ACTOR");
        for (int bad : new int[] {0, 65536, -1}) {
            var profile = LocalRuntimeFixtures.profile(mount, "ACTOR");
            profile.put(port, bad);
            writeProfile(profile);
            assertFailure(() -> LocalRuntimeContract.load(env), Role.PROFILE, Category.INVALID);
        }
    }

    @Test
    void generatesFreshProcessBootEachLoad() throws Exception {
        var env = LocalRuntimeFixtures.install(mount, "ACTOR");
        var first = LocalRuntimeContract.load(env);
        var second = LocalRuntimeContract.load(env);
        assertThat(first.processBoot()).isNotNull().isNotEqualTo(second.processBoot());
        var profile = LocalRuntimeFixtures.profile(mount, "ACTOR");
        profile.put("processBoot", first.processBoot().toString());
        writeProfile(profile);
        assertFailure(() -> LocalRuntimeContract.load(env), Role.PROFILE, Category.INVALID);
    }

    private Map<String, Object> profileWithKube(String field, String path) {
        var profile = LocalRuntimeFixtures.profile(mount, "ACTOR");
        @SuppressWarnings("unchecked")
        var kube = (Map<String, Object>) profile.get("kube");
        kube.put(field, path);
        return profile;
    }

    private void writeProfile(Map<String, Object> profile) throws Exception {
        Files.writeString(mount.resolve("process.json"), JSON.writeValueAsString(profile));
    }

    static LocalRuntimeFailure catchFailure(Runnable action) {
        var thrown = new ArrayList<LocalRuntimeFailure>();
        try {
            action.run();
        } catch (LocalRuntimeFailure failure) {
            thrown.add(failure);
        }
        assertThat(thrown).as("expected LocalRuntimeFailure").hasSize(1);
        return thrown.getFirst();
    }

    static void assertFailure(Runnable action, Role role, Category category) {
        var failure = catchFailure(action);
        assertThat(failure.role()).isEqualTo(role);
        assertThat(failure.category()).isEqualTo(category);
        assertThat(failure).hasNoCause();
    }
}
