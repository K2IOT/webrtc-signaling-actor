package io.webrtc.signaling.rpc;

import java.security.cert.X509Certificate;
import javax.net.ssl.SSLSession;

/** One exact workload URI per certificate; CA trust alone does not identify a cell. */
final class RpcTlsIdentity {
  private RpcTlsIdentity() {}

  static CellRpcServer.Peer extract(SSLSession session, String environment) {
    try {
      if (session == null || !"TLSv1.3".equals(session.getProtocol())) return null;
      return extract((X509Certificate) session.getPeerCertificates()[0], environment);
    } catch (Exception invalid) {
      return null;
    }
  }

  static CellRpcServer.Peer extract(X509Certificate certificate, String environment) {
    try {
      CellRpcServer.Peer found = null;
      var names = certificate.getSubjectAlternativeNames();
      if (names == null) return null;
      var pattern =
          java.util.regex.Pattern.compile(
              "spiffe://signaling/"
                  + java.util.regex.Pattern.quote(environment)
                  + "/cell/([a-z][a-z0-9-]{0,23})/(actor|gateway)(?:/([A-Za-z0-9_.-]{1,128}))?");
      for (var name : names) {
        if (((Integer) name.get(0)) != 6) continue;
        String uri = (String) name.get(1);
        if (!uri.startsWith("spiffe://signaling/")) continue;
        var match = pattern.matcher(uri);
        if (!match.matches() || found != null) return null;
        found =
            new CellRpcServer.Peer(
                match.group(1), match.group(2), match.group(3) == null ? "" : match.group(3));
      }
      return found;
    } catch (Exception invalid) {
      return null;
    }
  }
}
