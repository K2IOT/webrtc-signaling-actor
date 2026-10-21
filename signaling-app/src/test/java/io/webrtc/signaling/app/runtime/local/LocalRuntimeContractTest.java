package io.webrtc.signaling.app.runtime.local;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.mock.env.MockEnvironment;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static io.webrtc.signaling.app.runtime.local.LocalRuntimeFixtures.JSON;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class LocalRuntimeContractTest {
    @TempDir
    Path mount;

    @Test
    void loadsContractAndProfileFromTheMountedDirectory() throws Exception {
        var loaded = LocalRuntimeContract.load(LocalRuntimeFixtures.install(mount, "ACTOR"));
        assertThat(loaded.contract().cell()).isEqualTo("c001");
        assertThat(loaded.profile().plane()).isEqualTo(io.webrtc.signaling.app.SignalingApplication.Plane.ACTOR);
        assertThat(loaded.profile().wal().freshness()).isEqualTo(java.time.Duration.ofSeconds(1));
        assertThat(loaded.profile().transferCells()).isEmpty();
        assertThat(loaded.directory()).isEqualTo(mount.toRealPath());
        assertThat(loaded.toString()).doesNotContain("TEST_ONLY_PRIVATE");
        assertThat(loaded.profile().toString()).doesNotContain("TEST_ONLY_PRIVATE");
    }

    @Test
    void rejectsPodUidDifferentFromEnvironment() throws Exception {
        var env = LocalRuntimeFixtures.install(mount, "ACTOR");
        env.setProperty("SIGNALING_POD_UID", "00000000-0000-4000-8000-000000000002");
        assertThatThrownBy(() -> LocalRuntimeContract.load(env))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Local runtime identity mismatch");
        var absent = new MockEnvironment();
        copyPaths(env, absent);
        absent.setProperty("SIGNALING_CELL_ID", "c001");
        assertThatThrownBy(() -> LocalRuntimeContract.load(absent))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void rejectsCellDifferentFromEnvironment() throws Exception {
        var env = LocalRuntimeFixtures.install(mount, "ACTOR");
        env.setProperty("SIGNALING_CELL_ID", "c002");
        assertThatThrownBy(() -> LocalRuntimeContract.load(env))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Local runtime identity mismatch");
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
                            mount.resolve("missing").toString(),
                            outside.resolve("secret").toString(),
                            mount + "/" + "a".repeat(600))) {
                assertThatThrownBy(() -> loaded.resolve(bad))
                        .as(bad)
                        .isInstanceOf(IllegalArgumentException.class);
            }
            assertThatThrownBy(() -> loaded.resolve(null)).isInstanceOf(IllegalArgumentException.class);
        } finally {
            Files.deleteIfExists(outside.resolve("secret"));
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
                    JSON.writeValueAsString(LocalRuntimeFixtures.profile("ACTOR")));
            env.setProperty(
                    "SIGNALING_LOCAL_PROCESS_PROFILE", other.resolve("process.json").toString());
            assertThatThrownBy(() -> LocalRuntimeContract.load(env))
                    .isInstanceOf(IllegalArgumentException.class);
            Files.delete(mount.resolve("process.json"));
            Files.createSymbolicLink(mount.resolve("process.json"), other.resolve("process.json"));
            env.setProperty(
                    "SIGNALING_LOCAL_PROCESS_PROFILE", mount.resolve("process.json").toString());
            assertThatThrownBy(() -> LocalRuntimeContract.load(env))
                    .isInstanceOf(IllegalArgumentException.class);
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
    void rejectsUnknownAndDuplicateProfileFields() throws Exception {
        var env = LocalRuntimeFixtures.install(mount, "ACTOR");
        String valid = JSON.writeValueAsString(LocalRuntimeFixtures.profile("ACTOR"));
        String secret = "TEST_ONLY_PRIVATE_PASSWORD";
        var accepted = new java.util.ArrayList<String>();
        for (String invalid :
                List.of(
                        valid.substring(0, valid.length() - 1) + ",\"password\":\"" + secret + "\"}",
                        valid.substring(0, valid.length() - 1) + ",\"plane\":\"GATEWAY\"}",
                        valid.replace("\"healthPort\":9090", "\"healthPort\":9090,\"healthPort\":9091"),
                        valid.replace("\"schemaVersion\":1", "\"schemaVersion\":2"),
                        valid.replace("\"schemaVersion\":1", "\"schemaVersion\":\"1\""),
                        valid.replace("\"freshness\":\"PT1S\"", "\"freshness\":1"),
                        valid.replace("\"freshness\":\"PT1S\"", "\"freshness\":\"PT0S\""),
                        valid.replace("\"podIp\":\"192.0.2.10\",", ""),
                        valid.replace("\"plane\":\"ACTOR\"", "\"plane\":\"actor\""),
                        valid.replace("\"wal\":{", "\"wal\":{\"extra\":1,"),
                        valid + "{}",
                        "{\"a\":{\"b\":{\"c\":{\"d\":{\"e\":{\"f\":{\"g\":{\"h\":{\"i\":1}}}}}}}}}",
                        " ".repeat(65537))) {
            Files.writeString(mount.resolve("process.json"), invalid);
            try {
                LocalRuntimeContract.load(env);
            } catch (IllegalArgumentException expected) {
                assertThat(expected).hasMessageNotContaining(secret).hasNoCause();
                continue;
            }
            accepted.add(invalid.contains("\"freshness\":1,") ? "numeric-freshness" : invalid.contains("\"plane\":\"actor\"") ? "lowercase-plane" : invalid.length() > 100 ? invalid.substring(0, 100) : invalid);
        }
        assertThat(accepted).isEmpty();
    }

    @ParameterizedTest
    @ValueSource(strings = {"businessPort", "healthPort", "relayPort"})
    void rejectsOutOfRangePorts(String port) throws Exception {
        var env = LocalRuntimeFixtures.install(mount, "ACTOR");
        for (int bad : new int[] {0, 65536, -1}) {
            Map<String, Object> profile = LocalRuntimeFixtures.profile("ACTOR");
            profile.put(port, bad);
            Files.writeString(mount.resolve("process.json"), JSON.writeValueAsString(profile));
            assertThatThrownBy(() -> LocalRuntimeContract.load(env))
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test
    void generatesFreshProcessBootEachLoad() throws Exception {
        var env = LocalRuntimeFixtures.install(mount, "ACTOR");
        var first = LocalRuntimeContract.load(env);
        var second = LocalRuntimeContract.load(env);
        assertThat(first.processBoot()).isNotNull().isNotEqualTo(second.processBoot());
        Map<String, Object> profile = LocalRuntimeFixtures.profile("ACTOR");
        profile.put("processBoot", first.processBoot().toString());
        Files.writeString(mount.resolve("process.json"), JSON.writeValueAsString(profile));
        assertThatThrownBy(() -> LocalRuntimeContract.load(env))
                .isInstanceOf(IllegalArgumentException.class);
    }

    private static void copyPaths(MockEnvironment from, MockEnvironment to) {
        for (var name :
                List.of("SIGNALING_NATIVE_PROVIDER_CONTRACT", "SIGNALING_LOCAL_PROCESS_PROFILE")) {
            to.setProperty(name, from.getProperty(name));
        }
    }
}
