package io.webrtc.signaling.app.runtime.local;

import io.webrtc.signaling.app.runtime.local.LocalRuntimeFailure.Category;
import io.webrtc.signaling.app.runtime.local.LocalRuntimeFailure.Role;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Base64;

/**
 * One PEM block of an expected label, read from an already-confined path. Failures carry a
 * category only: never the path, the content or a cause.
 */
final class LocalPem {
    private static final int MAX_BYTES = 16384;

    private LocalPem() {
    }

    static byte[] der(Path file, String label) {
        byte[] bytes;
        try (var input = Files.newInputStream(file)) {
            bytes = input.readNBytes(MAX_BYTES + 1);
        } catch (IOException unreadable) {
            throw LocalJson.failure(Role.PATH, Category.MISSING);
        }
        if (bytes.length == 0 || bytes.length > MAX_BYTES)
            throw LocalJson.failure(Role.CONTRACT, Category.INVALID);
        var text = new String(bytes, StandardCharsets.US_ASCII).strip();
        var begin = "-----BEGIN " + label + "-----";
        var end = "-----END " + label + "-----";
        if (!text.startsWith(begin) || !text.endsWith(end) || text.indexOf(begin, 1) >= 0)
            throw LocalJson.failure(Role.CONTRACT, Category.INVALID);
        try {
            return Base64.getDecoder()
                    .decode(text.substring(begin.length(), text.length() - end.length()).replaceAll("\\s", ""));
        } catch (IllegalArgumentException malformed) {
            throw LocalJson.failure(Role.CONTRACT, Category.INVALID);
        }
    }
}
