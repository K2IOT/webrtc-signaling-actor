package io.webrtc.signaling.protocol;

/** Narrow protocol contract, mapped from launcher configuration without a dependency cycle. */
public record ProtocolLimits(
    int frameBytes,
    int jwtBytes,
    int sdpBytes,
    int iceCandidateBytes,
    int iceBatchCount,
    int iceBatchBytes,
    int iceRoundCount,
    int iceRoundBytes,
    int envelopeBytes) {
  public ProtocolLimits {
    int[] actual = {
      frameBytes,
      jwtBytes,
      sdpBytes,
      iceCandidateBytes,
      iceBatchCount,
      iceBatchBytes,
      iceRoundCount,
      iceRoundBytes,
      envelopeBytes
    };
    int[] max = {81920, 8192, 65536, 2048, 20, 8192, 256, 262144, 98304};
    for (int i = 0; i < actual.length; i++)
      if (actual[i] <= 0 || actual[i] > max[i])
        throw new IllegalArgumentException("unsafe protocol limit");
    if (jwtBytes > frameBytes
        || sdpBytes >= frameBytes
        || iceBatchBytes >= frameBytes
        || frameBytes >= envelopeBytes
        || iceCandidateBytes > iceBatchBytes
        || iceBatchBytes > iceRoundBytes
        || iceBatchCount > iceRoundCount)
      throw new IllegalArgumentException("inconsistent protocol budgets");
  }

  public static ProtocolLimits v1() {
    return new ProtocolLimits(81920, 8192, 65536, 2048, 20, 8192, 256, 262144, 98304);
  }
}
