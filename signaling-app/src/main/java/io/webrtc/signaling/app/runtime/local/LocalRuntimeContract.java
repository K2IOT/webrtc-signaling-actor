package io.webrtc.signaling.app.runtime.local;

import io.webrtc.signaling.app.runtime.MountedNativeContract;
import io.webrtc.signaling.app.runtime.local.LocalRuntimeFailure.Category;
import io.webrtc.signaling.app.runtime.local.LocalRuntimeFailure.Role;
import org.springframework.core.env.Environment;

import java.io.IOException;
import java.nio.file.NoSuchFileException;
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
    private static final int MAX_PATH = 512;

    public LocalRuntimeContract {
        Objects.requireNonNull(contract);
        Objects.requireNonNull(profile);
        Objects.requireNonNull(directory);
        Objects.requireNonNull(processBoot);
    }

    /**
     * Loads both mounts and fails closed with a {@link LocalRuntimeFailure} naming the input role
     * and failure category. The contract is read once by {@link MountedNativeContract}; only a
     * rejected contract is re-inspected, and only to name the category.
     */
    public static LocalRuntimeContract load(Environment env) {
        var contractFile = absolute(env.getProperty(CONTRACT_VARIABLE, CONTRACT_DEFAULT), Role.CONTRACT);
        var profileFile = absolute(env.getProperty(PROFILE_VARIABLE, PROFILE_DEFAULT), Role.PROFILE);
        // The directory is the real parent; projected-secret symlinks stay inside it.
        var directory = realParent(contractFile, Role.CONTRACT);
        MountedNativeContract contract;
        try {
            contract = MountedNativeContract.load(contractFile, "local-minikube", "LOCAL_TEST_ONLY");
        } catch (RuntimeException rejected) {
            throw diagnose(contractFile);
        }
        if (!directory.equals(realParent(profileFile, Role.PROFILE)))
            throw LocalJson.failure(Role.PROFILE, Category.PATH_ESCAPE);
        var profile =
                LocalProcessProfile.parse(
                        LocalJson.read(confine(directory, profileFile.toString(), Role.PROFILE), Role.PROFILE));
        if (!profile.podUid().equals(canonicalUuid(env.getProperty("SIGNALING_POD_UID")))
                || !contract.cell().equals(env.getProperty("SIGNALING_CELL_ID")))
            throw LocalJson.failure(Role.IDENTITY, Category.IDENTITY_MISMATCH);
        if (profile.kube() != null) {
            confine(directory, profile.kube().caPath(), Role.PROFILE);
            confine(directory, profile.kube().tokenPath(), Role.PROFILE);
        }
        return new LocalRuntimeContract(contract, profile, directory, UUID.randomUUID());
    }

    /**
     * An absolute path of at most 512 characters whose real location, after following every
     * symbolic link, is inside the contract directory.
     */
    public Path resolve(String path) {
        return confine(directory, path, Role.PATH);
    }

    @Override
    public String toString() {
        return "LocalRuntimeContract[LOCAL_TEST_ONLY,credentials=redacted]";
    }

    /** Names why the mounted contract was rejected; always throws. */
    private static LocalRuntimeFailure diagnose(Path file) {
        var document = LocalJson.tree(LocalJson.read(file, Role.CONTRACT), Role.CONTRACT);
        var environment = document.path("environment");
        return LocalJson.failure(
                Role.CONTRACT,
                environment.isTextual() && !"local-test-only".equals(environment.asText())
                        ? Category.ENVIRONMENT_MISMATCH
                        : Category.INVALID);
    }

    private static Path confine(Path directory, String name, Role role) {
        try {
            if (name == null || name.length() > MAX_PATH) throw new IllegalArgumentException();
            var path = Path.of(name);
            if (!path.isAbsolute()) throw new IllegalArgumentException();
            var real = path.toRealPath();
            if (!real.startsWith(directory)) throw new IllegalArgumentException();
            return real;
        } catch (NoSuchFileException absent) {
            throw LocalJson.failure(role, Category.MISSING);
        } catch (IOException | RuntimeException escaped) {
            throw LocalJson.failure(role, Category.PATH_ESCAPE);
        }
    }

    private static Path realParent(Path file, Role role) {
        try {
            return file.getParent().toRealPath();
        } catch (IOException absent) {
            throw LocalJson.failure(role, Category.MISSING);
        }
    }

    private static Path absolute(String name, Role role) {
        try {
            if (name == null || name.isBlank() || name.length() > MAX_PATH)
                throw new IllegalArgumentException();
            var path = Path.of(name);
            if (!path.isAbsolute() || path.getParent() == null) throw new IllegalArgumentException();
            return path;
        } catch (RuntimeException invalid) {
            throw LocalJson.failure(role, Category.PATH_ESCAPE);
        }
    }

    /** Lower-case canonical text only: {@code 1-1-1-1-1} parses as a UUID but is not accepted. */
    private static UUID canonicalUuid(String text) {
        try {
            var parsed = UUID.fromString(text);
            if (!parsed.toString().equals(text)) throw new IllegalArgumentException();
            return parsed;
        } catch (RuntimeException invalid) {
            throw LocalJson.failure(Role.IDENTITY, Category.IDENTITY_MISMATCH);
        }
    }
}
