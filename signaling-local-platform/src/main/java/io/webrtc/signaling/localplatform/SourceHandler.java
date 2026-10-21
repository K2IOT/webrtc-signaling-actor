package io.webrtc.signaling.localplatform;

import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpsExchange;
import io.webrtc.signaling.localplatform.Responses.Outcome;
import io.webrtc.signaling.localplatform.Responses.Refusal;
import java.net.URI;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Pattern;
import javax.net.ssl.SSLPeerUnverifiedException;
import javax.net.ssl.SSLSession;

/**
 * The workload source: {@code GET /v1/clock-bound} and {@code GET /v1/revocations}.
 *
 * <p>Order of checks: path ({@code 404}), method ({@code 405}), no query and no request body
 * ({@code 400}), caller identity from the TLS session certificate ({@code 403}; headers never
 * establish identity), then the headers: exactly one {@value #POD_HEADER} and one {@value
 * #BOOT_HEADER} in canonical UUID form, and for revocations exactly one {@value #OFFSET_HEADER}
 * matching {@code 0|[1-9][0-9]{0,18}} that fits a {@code long} ({@code 400}). The clock route
 * ignores the offset header. Only a well-formed request registers its boot ({@code 403} when the
 * registry refuses it).
 *
 * <p>Service outcomes: an unconfigured cell is {@code 403}, a cursor above the high-water mark
 * {@code 409}; the store being unavailable, a full pod table and any unexpected failure are an
 * empty {@code 503}. Framing and the body bound are {@link Responses}' job.
 *
 * <p>Status: LOCAL_TEST_ONLY, NOT_QUALIFIED.
 */
final class SourceHandler implements HttpHandler {

  static final String CLOCK_PATH = "/v1/clock-bound";
  static final String REVOCATIONS_PATH = "/v1/revocations";
  static final String POD_HEADER = "X-Signaling-Pod-Uid";
  static final String BOOT_HEADER = "X-Signaling-Process-Boot";
  static final String OFFSET_HEADER = "X-Signaling-Source-Offset";

  private static final Pattern UUID_TEXT =
      Pattern.compile(
          "[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}");
  private static final Pattern OFFSET_TEXT = Pattern.compile("0|[1-9][0-9]{0,18}");

  /** Produces the clock body for a registered caller. */
  @FunctionalInterface
  interface ClockEndpoint {
    String body(BootRegistry.Registration registration);
  }

  /** Produces the revocation page body for a cell after an offset. */
  @FunctionalInterface
  interface PageEndpoint {
    String body(String cell, long from);
  }

  private final Set<String> cells;
  private final BootRegistry registry;
  private final ClockEndpoint clock;
  private final PageEndpoint pages;

  SourceHandler(
      Set<String> cells,
      BootRegistry registry,
      ClockReportService clock,
      RevocationPageService pages) {
    this(cells, registry, clock::clockBody, pages::pageBody);
  }

  /** Seam for tests: any body producers behind the same request handling and framing. */
  SourceHandler(Set<String> cells, BootRegistry registry, ClockEndpoint clock, PageEndpoint pages) {
    this.cells = Set.copyOf(Objects.requireNonNull(cells, "cells"));
    this.registry = Objects.requireNonNull(registry, "registry");
    this.clock = Objects.requireNonNull(clock, "clock");
    this.pages = Objects.requireNonNull(pages, "pages");
  }

  @Override
  public void handle(HttpExchange exchange) {
    Outcome outcome;
    try {
      outcome = serve(exchange);
    } catch (Refusal refusal) {
      outcome = refusal.outcome();
    } catch (RuntimeException unexpected) {
      outcome = Outcome.empty(Responses.UNAVAILABLE);
    }
    Responses.send(exchange, outcome);
  }

  private Outcome serve(HttpExchange exchange) throws Refusal {
    URI uri = exchange.getRequestURI();
    String path = uri.getRawPath();
    boolean clockRoute = CLOCK_PATH.equals(path);
    if (!clockRoute && !REVOCATIONS_PATH.equals(path)) {
      throw new Refusal(Responses.NOT_FOUND);
    }
    if (!"GET".equals(exchange.getRequestMethod())) {
      throw new Refusal(Outcome.methodNotAllowed("GET"));
    }
    Headers headers = exchange.getRequestHeaders();
    if (uri.getRawQuery() != null || uri.getRawFragment() != null || hasBody(headers)) {
      throw new Refusal(Responses.BAD_REQUEST);
    }
    PeerIdentity peer = peer(exchange);
    UUID pod = uuid(headers, POD_HEADER);
    UUID boot = uuid(headers, BOOT_HEADER);
    long offset = clockRoute ? -1 : offset(headers);
    BootRegistry.Registration registration;
    try {
      registration = registry.register(peer, pod, boot);
    } catch (BootRegistry.BootRejected rejected) {
      throw new Refusal(Responses.FORBIDDEN);
    }
    return clockRoute ? clockReport(registration) : page(peer, offset);
  }

  private Outcome clockReport(BootRegistry.Registration registration) throws Refusal {
    try {
      return Outcome.json(Responses.OK, clock.body(registration));
    } catch (IllegalArgumentException unconfiguredCell) {
      throw new Refusal(Responses.FORBIDDEN);
    }
  }

  private Outcome page(PeerIdentity peer, long offset) throws Refusal {
    try {
      return Outcome.json(Responses.OK, pages.body(peer.cell(), offset));
    } catch (RevocationLog.CursorAhead ahead) {
      throw new Refusal(Responses.CONFLICT);
    } catch (IllegalArgumentException invalid) {
      throw new Refusal(Responses.BAD_REQUEST);
    }
  }

  private PeerIdentity peer(HttpExchange exchange) throws Refusal {
    try {
      if (!(exchange instanceof HttpsExchange secure)) {
        throw new Refusal(Responses.FORBIDDEN);
      }
      SSLSession session = secure.getSSLSession();
      Certificate[] chain = session == null ? null : session.getPeerCertificates();
      if (chain == null || chain.length == 0 || !(chain[0] instanceof X509Certificate leaf)) {
        throw new Refusal(Responses.FORBIDDEN);
      }
      return PeerIdentity.fromCertificate(leaf, cells);
    } catch (SSLPeerUnverifiedException | IllegalArgumentException rejected) {
      throw new Refusal(Responses.FORBIDDEN);
    }
  }

  /** A GET carries no body: any length other than {@code 0} or any transfer coding is refused. */
  static boolean hasBody(Headers headers) {
    List<String> length = headers.get("Content-Length");
    return headers.containsKey("Transfer-Encoding")
        || length != null && !(length.size() == 1 && "0".equals(length.get(0)));
  }

  private static String single(Headers headers, String name) throws Refusal {
    List<String> values = headers.get(name);
    if (values == null || values.size() != 1 || values.get(0) == null) {
      throw new Refusal(Responses.BAD_REQUEST);
    }
    return values.get(0);
  }

  private static UUID uuid(Headers headers, String name) throws Refusal {
    String value = single(headers, name);
    if (!UUID_TEXT.matcher(value).matches()) {
      throw new Refusal(Responses.BAD_REQUEST);
    }
    return UUID.fromString(value);
  }

  private static long offset(Headers headers) throws Refusal {
    String value = single(headers, OFFSET_HEADER);
    if (!OFFSET_TEXT.matcher(value).matches()) {
      throw new Refusal(Responses.BAD_REQUEST);
    }
    try {
      return Long.parseLong(value);
    } catch (NumberFormatException overflow) {
      throw new Refusal(Responses.BAD_REQUEST);
    }
  }
}
