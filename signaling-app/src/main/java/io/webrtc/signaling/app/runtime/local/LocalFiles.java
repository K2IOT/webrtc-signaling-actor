package io.webrtc.signaling.app.runtime.local;

import io.webrtc.signaling.app.runtime.local.LocalRuntimeFailure.Category;
import io.webrtc.signaling.app.runtime.local.LocalRuntimeFailure.Role;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * The one bounded read of mounted key material. An unreadable file is {@code PATH/MISSING}, an
 * empty one {@code CONTRACT/INVALID} and an oversized one {@code CONTRACT/TOO_LARGE}, whichever
 * owner asks; no path, content or cause is carried.
 */
final class LocalFiles {
    /** PEM keys, one block each. */
    static final int KEY_MAX = 16384;
    /** Certificate chains and CA bundles. */
    static final int CERTIFICATE_MAX = 65536;
    /** PKCS12 key and trust stores. */
    static final int STORE_MAX = 65536;

    private LocalFiles() {
    }

    static byte[] read(Path file, int maximum) {
        byte[] bytes;
        try (var input = Files.newInputStream(file)) {
            bytes = input.readNBytes(maximum + 1);
        } catch (IOException unreadable) {
            throw LocalJson.failure(Role.PATH, Category.MISSING);
        }
        if (bytes.length > maximum) throw LocalJson.failure(Role.CONTRACT, Category.TOO_LARGE);
        if (bytes.length == 0) throw LocalJson.failure(Role.CONTRACT, Category.INVALID);
        return bytes;
    }
}
