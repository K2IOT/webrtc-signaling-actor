package io.webrtc.signaling.localplatform;

import java.security.cert.CertificateParsingException;
import java.security.cert.X509Certificate;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Caller identity taken from the verified client certificate, never from request headers.
 *
 * <p>A workload certificate carries exactly one URI SAN of the shape {@code
 * spiffe://signaling/local-test-only/cell/<cell>/(actor|gateway|control)[/<instance>]}. An admin
 * certificate carries exactly one URI SAN {@code spiffe://signaling/local-test-only/admin/<name>}.
 * Each parser accepts only its own shape. Rejections carry no message or cause, and {@link
 * #toString()} does not print the identity.
 *
 * <p>Status: LOCAL_TEST_ONLY, NOT_QUALIFIED. The trust domain is a placeholder, not a deployable
 * identity.
 */
public record PeerIdentity(String spiffeId, String cell, String plane, String instance) {

  private static final int URI_SAN_TYPE = 6;
  private static final int MAX_URI_LENGTH = 256;
  private static final Pattern WORKLOAD =
      Pattern.compile(
          "spiffe://signaling/local-test-only/cell/([a-z][a-z0-9-]{0,23})"
              + "/(actor|gateway|control)(?:/([a-z0-9-]{1,63}))?");
  private static final Pattern ADMIN =
      Pattern.compile("spiffe://signaling/local-test-only/admin/([a-z0-9-]{1,32})");

  /**
   * Parses a workload identity.
   *
   * @throws IllegalArgumentException if the certificate has zero or several URI SANs, a different
   *     trust domain or path shape, or a cell outside {@code cells}
   */
  public static PeerIdentity fromCertificate(X509Certificate certificate, Set<String> cells) {
    if (cells == null) {
      throw reject();
    }
    String uri = singleUriSan(certificate);
    Matcher matcher = WORKLOAD.matcher(uri);
    if (!matcher.matches() || !cells.contains(matcher.group(1))) {
      throw reject();
    }
    return new PeerIdentity(uri, matcher.group(1), matcher.group(2), matcher.group(3));
  }

  /**
   * Parses an admin identity and returns its name.
   *
   * @throws IllegalArgumentException if the certificate has zero or several URI SANs or a URI that
   *     is not an admin identity in this trust domain
   */
  public static String admin(X509Certificate certificate) {
    Matcher matcher = ADMIN.matcher(singleUriSan(certificate));
    if (!matcher.matches()) {
      throw reject();
    }
    return matcher.group(1);
  }

  private static String singleUriSan(X509Certificate certificate) {
    if (certificate == null) {
      throw reject();
    }
    Collection<List<?>> names;
    try {
      names = certificate.getSubjectAlternativeNames();
    } catch (CertificateParsingException | RuntimeException e) {
      throw reject();
    }
    if (names == null) {
      throw reject();
    }
    String found = null;
    for (List<?> name : names) {
      if (name == null || name.size() < 2 || !Integer.valueOf(URI_SAN_TYPE).equals(name.get(0))) {
        continue;
      }
      if (found != null || !(name.get(1) instanceof String uri)) {
        throw reject();
      }
      found = uri;
    }
    if (found == null || found.length() > MAX_URI_LENGTH) {
      throw reject();
    }
    return found;
  }

  private static IllegalArgumentException reject() {
    return new IllegalArgumentException();
  }

  @Override
  public String toString() {
    return "PeerIdentity";
  }
}
