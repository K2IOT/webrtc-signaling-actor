package io.webrtc.signaling.gateway;

/**
 * Non-critical frame rate limits (spec 10, 49.7, 49.8). A socket owns a burst bucket for ordinary
 * frames and a total bucket that also covers critical frames; all sockets of one (issuer, user)
 * share one user bucket so five sessions cannot multiply the user's allowance. Critical control
 * (CANCEL, HANGUP, REJECT, refresh, receipts) bypasses the socket and user buckets and is bounded
 * only by the total bucket.
 */
public record GatewayRateLimits(
    double socketRate,
    double socketBurst,
    double socketTotalRate,
    double socketTotalBurst,
    double userRate,
    double userBurst,
    int trackedUsers) {
  public static final GatewayRateLimits V1 = new GatewayRateLimits(20, 40, 30, 60, 40, 80, 200_000);

  public GatewayRateLimits {
    positive(socketRate, 1_000, "socket rate");
    positive(socketBurst, 10_000, "socket burst");
    positive(socketTotalRate, 1_000, "socket total rate");
    positive(socketTotalBurst, 10_000, "socket total burst");
    positive(userRate, 5_000, "user rate");
    positive(userBurst, 50_000, "user burst");
    if (socketRate > socketTotalRate
        || socketBurst > socketTotalBurst
        || socketBurst < 1
        || socketTotalBurst < 1
        || userBurst < 1) throw new IllegalArgumentException("inconsistent rate limits");
    if (trackedUsers < 1 || trackedUsers > 1_000_000)
      throw new IllegalArgumentException("invalid tracked user bound");
  }

  private static void positive(double value, double max, String name) {
    if (!(value > 0) || value > max) throw new IllegalArgumentException("invalid " + name);
  }
}
