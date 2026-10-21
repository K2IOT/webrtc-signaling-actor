package io.webrtc.signaling.app.runtime.local;

import io.webrtc.signaling.app.runtime.local.LocalRuntimeFailure.Category;
import io.webrtc.signaling.app.runtime.local.LocalRuntimeFailure.Role;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class LocalPemTest {
    static final byte[] DER = {1, 2, 3, 4, 5, 6, 7, 8, 9};

    @TempDir
    Path dir;

    Path file(String text) throws Exception {
        var file = dir.resolve("key.pem");
        Files.writeString(file, text, StandardCharsets.US_ASCII);
        return file;
    }

    static void assertRejected(Runnable read, Role role, Category category) {
        assertThatThrownBy(read::run)
                .isInstanceOfSatisfying(
                        LocalRuntimeFailure.class,
                        failure -> {
                            assertThat(failure.role()).isEqualTo(role);
                            assertThat(failure.category()).isEqualTo(category);
                            assertThat(failure.getCause()).isNull();
                            assertThat(failure.getMessage()).doesNotContain("TEST_ONLY_SECRET");
                        });
    }

    @Test
    void readsExactlyOneBlockOfTheExpectedLabel() throws Exception {
        assertThat(LocalPem.der(file(LocalTestPki.pem("PRIVATE KEY", DER)), "PRIVATE KEY")).isEqualTo(DER);
        // Surrounding whitespace and CRLF line ends are not content.
        var crlf = LocalTestPki.pem("PRIVATE KEY", DER).replace("\n", "\r\n");
        assertThat(LocalPem.der(file("\n  " + crlf + "\n\n"), "PRIVATE KEY")).isEqualTo(DER);
    }

    @Test
    void refusesEverythingElse() throws Exception {
        var valid = LocalTestPki.pem("PRIVATE KEY", DER);
        var cases = new java.util.LinkedHashMap<String, String>();
        cases.put("encrypted pkcs8", LocalTestPki.pem("ENCRYPTED PRIVATE KEY", DER));
        cases.put("legacy rsa", LocalTestPki.pem("RSA PRIVATE KEY", DER));
        cases.put("wrong label", LocalTestPki.pem("PUBLIC KEY", DER));
        cases.put("truncated", valid.substring(0, valid.length() - 10));
        cases.put("only the begin line", "-----BEGIN PRIVATE KEY-----");
        cases.put("two blocks", valid + valid);
        cases.put("two labels", valid + LocalTestPki.pem("CERTIFICATE", DER));
        cases.put("leading text", "TEST_ONLY_SECRET\n" + valid);
        cases.put("trailing text", valid + "TEST_ONLY_SECRET\n");
        cases.put("encapsulated header", valid.replaceFirst("\n", "\nProc-Type: 4,ENCRYPTED\n"));
        cases.put("not base64", "-----BEGIN PRIVATE KEY-----\n!TEST_ONLY_SECRET\n-----END PRIVATE KEY-----\n");
        cases.put("nested marker", "-----BEGIN PRIVATE KEY-----\n-----BEGIN PRIVATE KEY-----\n-----END PRIVATE KEY-----\n");
        cases.put("empty", "");
        for (Map.Entry<String, String> entry : cases.entrySet()) {
            var path = file(entry.getValue());
            assertRejected(() -> LocalPem.der(path, "PRIVATE KEY"), Role.CONTRACT, Category.INVALID);
        }
    }

    @Test
    void boundsTheReadAndNamesAMissingFile() throws Exception {
        // 16 KiB exactly is allowed; one byte more is too large, whatever the content.
        var padding = " ".repeat(LocalFiles.KEY_MAX - LocalTestPki.pem("PRIVATE KEY", DER).length());
        assertThat(LocalPem.der(file(LocalTestPki.pem("PRIVATE KEY", DER) + padding), "PRIVATE KEY"))
                .isEqualTo(DER);
        var oversized = file(LocalTestPki.pem("PRIVATE KEY", DER) + padding + " ");
        assertRejected(() -> LocalPem.der(oversized, "PRIVATE KEY"), Role.CONTRACT, Category.TOO_LARGE);
        var big = file("A".repeat(10 * LocalFiles.KEY_MAX));
        assertRejected(() -> LocalPem.der(big, "PRIVATE KEY"), Role.CONTRACT, Category.TOO_LARGE);
        var absent = dir.resolve("absent.pem");
        assertRejected(() -> LocalPem.der(absent, "PRIVATE KEY"), Role.PATH, Category.MISSING);
        assertRejected(() -> LocalPem.der(dir, "PRIVATE KEY"), Role.PATH, Category.MISSING);
    }
}
