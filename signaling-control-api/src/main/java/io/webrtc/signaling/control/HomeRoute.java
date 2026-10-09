package io.webrtc.signaling.control;

import java.net.URI;

public record HomeRoute(int bucket, String cell, long epoch, String wssUrl) {
  public HomeRoute {
    if (bucket < 0
        || bucket >= 16384
        || cell == null
        || !cell.matches("[a-z][a-z0-9-]{0,23}")
        || epoch <= 0
        || wssUrl == null
        || !"wss".equals(URI.create(wssUrl).getScheme())
        || URI.create(wssUrl).getRawUserInfo() != null
        || URI.create(wssUrl).getHost() == null)
      throw new IllegalArgumentException("invalid home route");
  }
}
