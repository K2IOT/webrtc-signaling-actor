package io.webrtc.signaling.app.runtime.local;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectReader;
import io.webrtc.signaling.app.runtime.MountedNativeContract;
import io.webrtc.signaling.app.runtime.local.LocalRuntimeFailure.Category;
import io.webrtc.signaling.app.runtime.local.LocalRuntimeFailure.Role;
import io.webrtc.signaling.protocol.Identity.UserId;
import io.webrtc.signaling.rpc.GatewayRelayRpcClient;
import io.webrtc.signaling.rpc.ProofBindings;
import io.webrtc.signaling.storage.SessionRegistryService;

import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;

/**
 * The directory snapshot a LOCAL_TEST_ONLY container is pinned to by {@code directorySnapshot} and
 * {@code directorySnapshotSha256} (format: {@code config/native-sources.md}, "Local process
 * profile"). It is read once, bounded, hash-checked over the exact bytes and parsed strictly; the
 * resulting lookups do no I/O. A bucket the snapshot does not name has no trusted home: {@link
 * #home} returns {@code null}, which every home verifier treats as a rejection.
 */
final class LocalDirectorySnapshot {
    static final int BUCKETS = 16384;
    /** Reachable within the 64 KiB read bound (about 70 bytes per range). */
    static final int MAX_RANGES = 512;
    /**
     * The shared strict mapper, minus Jackson's default float-to-int truncation: {@code 1.9} is not
     * a bucket.
     */
    private static final ObjectReader READER =
            LocalJson.MAPPER
                    .readerFor(Document.class)
                    .without(DeserializationFeature.ACCEPT_FLOAT_AS_INT)
                    .with(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);

    private final ProofBindings.TrustedHome[] homes;
    private final Map<GatewayRelayRpcClient.Target, MountedNativeContract.GatewayTarget> gateways;

    private LocalDirectorySnapshot(
            ProofBindings.TrustedHome[] homes,
            Map<GatewayRelayRpcClient.Target, MountedNativeContract.GatewayTarget> gateways) {
        this.homes = homes;
        this.gateways = Map.copyOf(gateways);
    }

    /**
     * Fails with {@code CONTRACT/INVALID} for a hash mismatch or a schema or content defect; text
     * that is not UTF-8 JSON is {@code CONTRACT/MALFORMED} and a repeated key {@code
     * CONTRACT/DUPLICATE_KEY}, as for the other mounted documents.
     */
    static LocalDirectorySnapshot load(LocalRuntimeContract c) {
        var contract = c.contract();
        var bytes = LocalFiles.read(c.resolve(contract.directorySnapshot()), LocalJson.MAX_BYTES);
        if (!MessageDigest.isEqual(
                sha256(bytes), HexFormat.of().parseHex(contract.directorySnapshotSha256())))
            throw invalid();
        Document document;
        try {
            document =
                    READER.readValue(LocalJson.tree(utf8(bytes), Role.CONTRACT));
        } catch (LocalRuntimeFailure malformed) {
            throw malformed;
        } catch (Exception malformed) {
            // Jackson diagnostics may quote input; keep none of it.
            throw invalid();
        }
        try {
            return new LocalDirectorySnapshot(homes(document, contract), gateways(document, contract));
        } catch (RuntimeException rejected) {
            throw invalid();
        }
    }

    /** The trusted home of {@code user}'s bucket, or {@code null} when the snapshot has none. */
    ProofBindings.TrustedHome home(UserId user) {
        return homes[SessionRegistryService.bucket(user)];
    }

    /** Every contract gateway paired with its pinned boot; keys are the relay targets. */
    Map<GatewayRelayRpcClient.Target, MountedNativeContract.GatewayTarget> gateways() {
        return gateways;
    }

    /** The pinned relay targets, for the compatibility observation enrollment. */
    Set<GatewayRelayRpcClient.Target> targets() {
        return Set.copyOf(gateways.keySet());
    }

    private static ProofBindings.TrustedHome[] homes(
            Document document, MountedNativeContract contract) {
        var epochs = new HashMap<String, Long>();
        contract.cells().forEach(cell -> epochs.put(cell.cell(), cell.storageEpoch()));
        var homes = new ProofBindings.TrustedHome[BUCKETS];
        for (var range : document.buckets()) {
            var epoch = epochs.get(range.cell());
            if (epoch == null || epoch != range.storageEpoch()) throw new IllegalArgumentException();
            var home = new ProofBindings.TrustedHome(range.cell(), range.directoryEpoch(), range.storageEpoch());
            for (int bucket = range.first(); bucket <= range.last(); bucket++) {
                if (homes[bucket] != null) throw new IllegalArgumentException();
                homes[bucket] = home;
            }
        }
        return homes;
    }

    private static Map<GatewayRelayRpcClient.Target, MountedNativeContract.GatewayTarget> gateways(
            Document document, MountedNativeContract contract) {
        var enrolled = new HashMap<String, MountedNativeContract.GatewayTarget>();
        contract.gateways().forEach(g -> enrolled.put(g.cell() + "/" + g.gatewayId(), g));
        var result = new HashMap<GatewayRelayRpcClient.Target, MountedNativeContract.GatewayTarget>();
        var seen = new HashSet<String>();
        for (var boot : document.gatewayBoots()) {
            var key = boot.cell() + "/" + boot.gatewayId();
            var gateway = enrolled.get(key);
            if (gateway == null || !seen.add(key)) throw new IllegalArgumentException();
            result.put(new GatewayRelayRpcClient.Target(boot.cell(), boot.gatewayId(), boot.bootId()), gateway);
        }
        // Exactly one pinned boot for every enrolled gateway.
        if (!seen.equals(enrolled.keySet())) throw new IllegalArgumentException();
        return result;
    }

    private static byte[] sha256(byte[] bytes) {
        try {
            return MessageDigest.getInstance("SHA-256").digest(bytes);
        } catch (NoSuchAlgorithmException unavailable) {
            throw new IllegalStateException("SHA-256 unavailable");
        }
    }

    private static String utf8(byte[] bytes) {
        try {
            return StandardCharsets.UTF_8
                    .newDecoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .decode(ByteBuffer.wrap(bytes))
                    .toString();
        } catch (CharacterCodingException notUtf8) {
            throw LocalJson.failure(Role.CONTRACT, Category.MALFORMED);
        }
    }

    private static LocalRuntimeFailure invalid() {
        return LocalJson.failure(Role.CONTRACT, Category.INVALID);
    }

    record Document(
            int schemaVersion, String qualification, List<Range> buckets, List<GatewayBoot> gatewayBoots) {
        Document {
            if (schemaVersion != 1 || !"LOCAL_TEST_ONLY".equals(qualification))
                throw new IllegalArgumentException();
            buckets = List.copyOf(buckets);
            gatewayBoots = List.copyOf(gatewayBoots);
            if (buckets.isEmpty() || buckets.size() > MAX_RANGES || gatewayBoots.size() > 128)
                throw new IllegalArgumentException();
        }
    }

    record Range(int first, int last, String cell, long directoryEpoch, long storageEpoch) {
        Range {
            if (first < 0 || last < first || last >= BUCKETS) throw new IllegalArgumentException();
            if (cell == null || !cell.matches("c[0-9]{3}")) throw new IllegalArgumentException();
            if (directoryEpoch < 1 || storageEpoch < 1) throw new IllegalArgumentException();
        }
    }

    record GatewayBoot(String cell, String gatewayId, UUID bootId) {
        GatewayBoot {
            Objects.requireNonNull(cell);
            Objects.requireNonNull(gatewayId);
            Objects.requireNonNull(bootId);
        }
    }
}
