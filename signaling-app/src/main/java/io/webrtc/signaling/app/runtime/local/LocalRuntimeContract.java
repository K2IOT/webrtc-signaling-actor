package io.webrtc.signaling.app.runtime.local;

import io.webrtc.signaling.app.runtime.MountedNativeContract;
import org.springframework.core.env.Environment;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;
import java.util.UUID;

/**
 * The mounted native contract plus the per-container process profile, bound to this JVM. Every file
 * the adapter later opens must go through {@link #resolve(String)}.
 *
 * @param processBoot generated fresh for each {@link #load}; never read from a file
 */
public record LocalRuntimeContract(
        MountedNativeContract contract,
        LocalProcessProfile profile,
        Path directory,
        UUID processBoot) {
    static final String PROFILE_VARIABLE = "SIGNALING_LOCAL_PROCESS_PROFILE";
    static final String PROFILE_DEFAULT = "/run/signaling/process.json";
    static final String CONTRACT_VARIABLE = "SIGNALING_NATIVE_PROVIDER_CONTRACT";
    static final String CONTRACT_DEFAULT = "/run/signaling/provider.json";
    private static final int MAX_BYTES = 65536;
    private static final int MAX_PATH = 512;

    public LocalRuntimeContract {
        Objects.requireNonNull(contract);
        Objects.requireNonNull(profile);
        Objects.requireNonNull(directory);
        Objects.requireNonNull(processBoot);
    }

    /**
     * Loads both mounts and fails closed unless this container is the one the profile describes.
     */
    public static LocalRuntimeContract load(Environment env) {
        try {
            var contractFile = absolute(env.getProperty(CONTRACT_VARIABLE, CONTRACT_DEFAULT));
            var profileFile = absolute(env.getProperty(PROFILE_VARIABLE, PROFILE_DEFAULT));
            var contract = MountedNativeContract.load(contractFile, "local-minikube", "LOCAL_TEST_ONLY");
            // The directory is the real parent; projected-secret symlinks stay inside it.
            var directory = contractFile.getParent().toRealPath();
            if (!directory.equals(profileFile.getParent().toRealPath()))
                throw new IllegalArgumentException();
            var profile = LocalProcessProfile.parse(read(confine(directory, profileFile.toString())));
            if (!profile.podUid().equals(uuid(env.getProperty("SIGNALING_POD_UID")))
                    || !contract.cell().equals(env.getProperty("SIGNALING_CELL_ID")))
                throw new IllegalStateException("Local runtime identity mismatch");
            return new LocalRuntimeContract(contract, profile, directory, UUID.randomUUID());
        } catch (IllegalStateException mismatch) {
            throw mismatch;
        } catch (Exception invalid) {
            // Parser diagnostics may carry private input; retain neither source fragments nor causes.
            throw new IllegalArgumentException("Invalid local runtime contract");
        }
    }

    /**
     * An absolute path of at most 512 characters whose real location, after following every
     * symbolic link, is inside the contract directory.
     */
    public Path resolve(String path) {
        try {
            return confine(directory, path);
        } catch (Exception invalid) {
            throw new IllegalArgumentException("Invalid local runtime path");
        }
    }

    @Override
    public String toString() {
        return "LocalRuntimeContract[LOCAL_TEST_ONLY,credentials=redacted]";
    }

    /**
     * Bounded read of the declared contract environment, used only by the activation gate. Any
     * unreadable or malformed input yields {@code null}: the gate then stays closed.
     */
    static String declaredEnvironment(Environment env) {
        try {
            var file = absolute(env.getProperty(CONTRACT_VARIABLE, CONTRACT_DEFAULT));
            var node = LocalProcessProfile.JSON.readTree(read(file));
            return node.path("environment").isTextual() ? node.path("environment").asText() : null;
        } catch (Exception unreadable) {
            return null;
        }
    }

    private static Path confine(Path directory, String name) throws IOException {
        if (name == null || name.length() > MAX_PATH) throw new IllegalArgumentException();
        var path = Path.of(name);
        if (!path.isAbsolute()) throw new IllegalArgumentException();
        var real = path.toRealPath();
        if (!real.startsWith(directory)) throw new IllegalArgumentException();
        return real;
    }

    private static Path absolute(String name) {
        if (name == null || name.isBlank() || name.length() > MAX_PATH) throw new IllegalArgumentException();
        var path = Path.of(name);
        if (!path.isAbsolute() || path.getParent() == null) throw new IllegalArgumentException();
        return path;
    }

    private static UUID uuid(String text) {
        if (text == null) throw new IllegalArgumentException();
        return UUID.fromString(text.trim());
    }

    private static String read(Path file) throws IOException {
        if (!Files.isRegularFile(file)) throw new IllegalArgumentException();
        byte[] bytes;
        try (var input = Files.newInputStream(file)) {
            bytes = input.readNBytes(MAX_BYTES + 1);
        }
        if (bytes.length == 0 || bytes.length > MAX_BYTES) throw new IllegalArgumentException();
        return StandardCharsets.UTF_8
                .newDecoder()
                .onMalformedInput(CodingErrorAction.REPORT)
                .onUnmappableCharacter(CodingErrorAction.REPORT)
                .decode(ByteBuffer.wrap(bytes))
                .toString();
    }
}
