package io.webrtc.signaling.control;

import io.webrtc.signaling.protocol.Identity.UserId;
import java.nio.charset.StandardCharsets;
import java.security.*;

public final class BucketHasher {
  private BucketHasher() {}

  /** v1: first 8 SHA-256 octets as an unsigned big-endian integer modulo 16384. */
  public static int bucket(UserId user) {
    try {
      var d =
          MessageDigest.getInstance("SHA-256")
              .digest(user.value().getBytes(StandardCharsets.UTF_8));
      return ((d[6] & 0x3f) << 8) | (d[7] & 0xff);
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException(e);
    }
  }
}
