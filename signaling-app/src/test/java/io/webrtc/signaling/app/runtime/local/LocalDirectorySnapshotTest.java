package io.webrtc.signaling.app.runtime.local;

import io.webrtc.signaling.app.runtime.local.LocalRuntimeFailure.Category;
import io.webrtc.signaling.app.runtime.local.LocalRuntimeFailure.Role;
import io.webrtc.signaling.protocol.Identity.UserId;
import io.webrtc.signaling.rpc.GatewayRelayRpcClient;
import io.webrtc.signaling.rpc.ProofBindings;
import io.webrtc.signaling.storage.SessionRegistryService;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static io.webrtc.signaling.app.runtime.local.LocalActorFixture.BOOT_C001;
import static io.webrtc.signaling.app.runtime.local.LocalActorFixture.BOOT_C002;
import static io.webrtc.signaling.app.runtime.local.LocalActorFixture.boot;
import static io.webrtc.signaling.app.runtime.local.LocalActorFixture.range;
import static io.webrtc.signaling.app.runtime.local.LocalRuntimeFixtures.JSON;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Every rejection is paired with a valid twin that differs from it in exactly that one rule, so
 * each case is shown to fail for its own reason even where the category is the shared INVALID.
 */
class LocalDirectorySnapshotTest {
    @TempDir
    Path mount;

    LocalDirectorySnapshot load(byte[] bytes) throws Exception {
        return LocalDirectorySnapshot.load(LocalActorFixture.write(mount, p -> {
        }, bytes));
    }

    static byte[] json(Consumer<Map<String, Object>> edit) throws Exception {
        var snapshot = LocalActorFixture.snapshot();
        edit.accept(snapshot);
        return JSON.writeValueAsBytes(snapshot);
    }

    static byte[] base() throws Exception {
        return json(s -> {
        });
    }

    void assertRule(byte[] valid, byte[] invalid, Category category) throws Exception {
        assertThatCode(() -> load(valid)).as("valid twin").doesNotThrowAnyException();
        assertThatThrownBy(() -> load(invalid))
                .isInstanceOfSatisfying(
                        LocalRuntimeFailure.class,
                        failure -> {
                            assertThat(failure.role()).isEqualTo(Role.CONTRACT);
                            assertThat(failure.category()).isEqualTo(category);
                        });
    }

    void assertRule(Consumer<Map<String, Object>> valid, Consumer<Map<String, Object>> invalid)
            throws Exception {
        assertRule(json(valid), json(invalid), Category.INVALID);
    }

    static Consumer<Map<String, Object>> buckets(List<Map<String, Object>> ranges) {
        return s -> s.put("buckets", ranges);
    }

    static Consumer<Map<String, Object>> boots(List<Map<String, Object>> boots) {
        return s -> s.put("gatewayBoots", boots);
    }

    static Map<String, Object> with(Map<String, Object> range, String key, Object value) {
        var copy = new LinkedHashMap<>(range);
        copy.put(key, value);
        return copy;
    }

    static UserId userInBucket(int first, int last) {
        for (int i = 0; ; i++) {
            var user = new UserId("TEST_ONLY_user_" + i);
            int bucket = SessionRegistryService.bucket(user);
            if (bucket >= first && bucket <= last) return user;
        }
    }

    @Test
    void homesResolveFromSnapshotAndUnknownBucketsHaveNoHome() throws Exception {
        var snapshot = load(json(buckets(List.of(range(0, 8191, "c001"), range(8192, 12000, "c002")))));

        assertThat(snapshot.home(userInBucket(0, 8191))).isEqualTo(new ProofBindings.TrustedHome("c001", 3, 1));
        assertThat(snapshot.home(userInBucket(8192, 12000)))
                .isEqualTo(new ProofBindings.TrustedHome("c002", 3, 1));
        assertThat(snapshot.home(userInBucket(12001, 16383))).isNull();
        assertThat(snapshot.targets())
                .containsExactlyInAnyOrder(
                        new GatewayRelayRpcClient.Target("c001", "local-c001", BOOT_C001),
                        new GatewayRelayRpcClient.Target("c002", "local-c002", BOOT_C002));
    }

    @Test
    void digestMustMatchTheExactBytes() throws Exception {
        var c = LocalActorFixture.write(mount);
        assertThatCode(() -> LocalDirectorySnapshot.load(c)).doesNotThrowAnyException();
        // Same document, one more byte of whitespace: semantically equal, digest differs.
        var padded = new ByteArrayOutputStream();
        padded.write(base());
        padded.write(' ');
        Files.write(mount.resolve("directory.json"), padded.toByteArray());
        assertThatThrownBy(() -> LocalDirectorySnapshot.load(c))
                .isInstanceOfSatisfying(
                        LocalRuntimeFailure.class,
                        failure -> assertThat(failure.category()).isEqualTo(Category.INVALID));
    }

    @Test
    void duplicateKeysAreDuplicateKey() throws Exception {
        var text = new String(base(), StandardCharsets.UTF_8);
        var duplicated = text.replaceFirst("\\{", "{\"schemaVersion\":1,");
        assertRule(base(), duplicated.getBytes(StandardCharsets.UTF_8), Category.DUPLICATE_KEY);
    }

    @Test
    void trailingContentIsMalformed() throws Exception {
        var text = new String(base(), StandardCharsets.UTF_8);
        assertRule(
                (text + " ").getBytes(StandardCharsets.UTF_8),
                (text + " {}").getBytes(StandardCharsets.UTF_8),
                Category.MALFORMED);
    }

    @Test
    void nonUtf8IsMalformed() throws Exception {
        var valid = new ByteArrayOutputStream();
        valid.write(base());
        valid.write(' ');
        var invalid = new ByteArrayOutputStream();
        invalid.write(base());
        invalid.write(0xFF);
        assertRule(valid.toByteArray(), invalid.toByteArray(), Category.MALFORMED);
    }

    @Test
    void schemaVersionMustBeOne() throws Exception {
        assertRule(s -> s.put("schemaVersion", 1), s -> s.put("schemaVersion", 2));
    }

    @Test
    void qualificationMustBeLocalTestOnly() throws Exception {
        assertRule(s -> s.put("qualification", "LOCAL_TEST_ONLY"), s -> s.put("qualification", "PRODUCTION"));
    }

    @Test
    void unknownAndMissingFieldsAreRejected() throws Exception {
        assertRule(s -> {
        }, s -> s.put("unexpected", true));
        assertRule(s -> {
        }, s -> s.remove("gatewayBoots"));
        assertRule(
                buckets(List.of(range(0, 16383, "c001"))),
                buckets(List.of(with(range(0, 16383, "c001"), "unexpected", 1))));
    }

    @Test
    void fractionalNumbersAreNotTruncated() throws Exception {
        assertRule(
                buckets(List.of(with(range(0, 16383, "c001"), "first", 1))),
                buckets(List.of(with(range(0, 16383, "c001"), "first", 1.9))));
        assertRule(
                buckets(List.of(with(range(0, 16383, "c001"), "directoryEpoch", 1))),
                buckets(List.of(with(range(0, 16383, "c001"), "directoryEpoch", 1.5))));
    }

    @Test
    void directoryEpochMustBePositive() throws Exception {
        assertRule(
                buckets(List.of(with(range(0, 16383, "c001"), "directoryEpoch", 1))),
                buckets(List.of(with(range(0, 16383, "c001"), "directoryEpoch", 0))));
    }

    @Test
    void storageEpochMustBePositive() throws Exception {
        assertRule(
                buckets(List.of(with(range(0, 16383, "c001"), "storageEpoch", 1))),
                buckets(List.of(with(range(0, 16383, "c001"), "storageEpoch", 0))));
    }

    @Test
    void storageEpochMustEqualTheContractCells() throws Exception {
        assertRule(
                buckets(List.of(with(range(0, 16383, "c001"), "storageEpoch", 1))),
                buckets(List.of(with(range(0, 16383, "c001"), "storageEpoch", 2))));
    }

    @Test
    void cellMustBeEnrolledInTheContract() throws Exception {
        assertRule(buckets(List.of(range(0, 16383, "c002"))), buckets(List.of(range(0, 16383, "c999"))));
    }

    @Test
    void rangesMustNotOverlap() throws Exception {
        assertRule(
                buckets(List.of(range(0, 100, "c001"), range(101, 200, "c002"))),
                buckets(List.of(range(0, 100, "c001"), range(100, 200, "c002"))));
    }

    @Test
    void rangesMustLieWithinTheBucketSpace() throws Exception {
        assertRule(buckets(List.of(range(0, 16383, "c001"))), buckets(List.of(range(-1, 16383, "c001"))));
        assertRule(buckets(List.of(range(0, 16383, "c001"))), buckets(List.of(range(0, 16384, "c001"))));
        assertRule(buckets(List.of(range(10, 10, "c001"))), buckets(List.of(range(10, 9, "c001"))));
    }

    @Test
    void rangeCountIsBounded() throws Exception {
        assertRule(buckets(singles(LocalDirectorySnapshot.MAX_RANGES)), buckets(singles(LocalDirectorySnapshot.MAX_RANGES + 1)));
        assertRule(buckets(singles(1)), buckets(List.of()));
    }

    static List<Map<String, Object>> singles(int count) {
        var ranges = new ArrayList<Map<String, Object>>();
        for (int bucket = 0; bucket < count; bucket++) ranges.add(range(bucket, bucket, "c001"));
        return ranges;
    }

    @Test
    void everyEnrolledGatewayHasExactlyOnePinnedBoot() throws Exception {
        var c001 = boot("c001", BOOT_C001);
        var c002 = boot("c002", BOOT_C002);
        assertRule(boots(List.of(c001, c002)), boots(List.of(c001, c002, c001)));
        assertRule(boots(List.of(c001, c002)), boots(List.of(c001)));
        var stranger =
                Map.<String, Object>of("cell", "c001", "gatewayId", "local-other", "bootId", BOOT_C002.toString());
        assertRule(boots(List.of(c001, c002)), boots(List.of(c001, c002, stranger)));
    }
}
