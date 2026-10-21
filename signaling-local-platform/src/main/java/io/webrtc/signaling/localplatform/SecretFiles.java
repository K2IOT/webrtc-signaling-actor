package io.webrtc.signaling.localplatform;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;

/**
 * Bounded reads of key-material files. Nothing read here is ever logged, printed or put into an
 * exception; failures are bare {@link IOException}s without a path or value.
 *
 * <p>Status: LOCAL_TEST_ONLY, NOT_QUALIFIED.
 */
final class SecretFiles {

  static final int MAX_PASSWORD_BYTES = 1024;

  private SecretFiles() {}

  /**
   * Reads at most {@code maxBytes} bytes; a larger file is refused without reading it to the end.
   */
  static byte[] readBounded(Path file, int maxBytes) throws IOException {
    try (InputStream in = Files.newInputStream(file)) {
      byte[] bytes = in.readNBytes(maxBytes + 1);
      if (bytes.length > maxBytes) {
        Arrays.fill(bytes, (byte) 0);
        throw new IOException();
      }
      return bytes;
    }
  }

  /**
   * Reads a password file: strict UTF-8, one optional trailing line break removed, not empty, no
   * control characters. The caller clears the returned array.
   */
  static char[] readPassword(Path file) throws IOException {
    byte[] bytes = readBounded(file, MAX_PASSWORD_BYTES);
    CharBuffer chars = null;
    try {
      chars =
          StandardCharsets.UTF_8
              .newDecoder()
              .onMalformedInput(CodingErrorAction.REPORT)
              .onUnmappableCharacter(CodingErrorAction.REPORT)
              .decode(ByteBuffer.wrap(bytes));
      int end = chars.limit();
      if (end > 0 && chars.get(end - 1) == '\n') {
        end--;
        if (end > 0 && chars.get(end - 1) == '\r') {
          end--;
        }
      }
      if (end == 0) {
        throw new IOException();
      }
      char[] password = new char[end];
      for (int i = 0; i < end; i++) {
        char c = chars.get(i);
        if (Character.isISOControl(c)) {
          Arrays.fill(password, '\0');
          throw new IOException();
        }
        password[i] = c;
      }
      return password;
    } catch (CharacterCodingException malformed) {
      throw new IOException();
    } finally {
      Arrays.fill(bytes, (byte) 0);
      if (chars != null && chars.hasArray()) {
        Arrays.fill(chars.array(), '\0');
      }
    }
  }
}
