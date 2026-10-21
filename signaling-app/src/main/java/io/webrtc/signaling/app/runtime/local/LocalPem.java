package io.webrtc.signaling.app.runtime.local;

import io.webrtc.signaling.app.runtime.local.LocalRuntimeFailure.Category;
import io.webrtc.signaling.app.runtime.local.LocalRuntimeFailure.Role;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Base64;

/**
 * Exactly one PEM block of the expected label, read from an already-confined path. Anything else is
 * refused: another label (so {@code ENCRYPTED PRIVATE KEY} and legacy {@code RSA PRIVATE KEY} are
 * never guessed at), a second block, leading or trailing text, a truncated block, encapsulated
 * headers and bad base64. Failures carry a category only: never the path, the content or a cause.
 */
final class LocalPem {
    private LocalPem() {
    }

    static byte[] der(Path file, String label) {
        var text = new String(LocalFiles.read(file, LocalFiles.KEY_MAX), StandardCharsets.US_ASCII).strip();
        var begin = "-----BEGIN " + label + "-----";
        var end = "-----END " + label + "-----";
        if (!text.startsWith(begin) || !text.endsWith(end) || text.indexOf(begin, 1) >= 0)
            throw LocalJson.failure(Role.CONTRACT, Category.INVALID);
        if (text.length() < begin.length() + end.length())
            throw LocalJson.failure(Role.CONTRACT, Category.INVALID);
        try {
            var body = text.substring(begin.length(), text.length() - end.length());
            if (body.contains("-----")) throw new IllegalArgumentException();
            return Base64.getDecoder().decode(body.replaceAll("\\s", ""));
        } catch (IllegalArgumentException malformed) {
            throw LocalJson.failure(Role.CONTRACT, Category.INVALID);
        }
    }
}
