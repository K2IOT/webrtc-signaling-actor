package io.webrtc.signaling.localplatform;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.StreamReadFeature;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpsExchange;
import io.webrtc.signaling.localplatform.Responses.Outcome;
import io.webrtc.signaling.localplatform.Responses.Refusal;
import io.webrtc.signaling.protocol.Identity.UserId;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.OptionalLong;
import java.util.Set;
import java.util.regex.Pattern;
import javax.net.ssl.SSLPeerUnverifiedException;
import javax.net.ssl.SSLSession;

/**
 * The admin role: {@code POST /v1/admin/revocations} appends one revoke event.
 *
 * <p>Order of checks: path ({@code 404}), method ({@code 405}), admin identity from the TLS session
 * certificate via {@link PeerIdentity#admin} ({@code 403}), no query ({@code 400}), exactly one
 * {@code Content-Type} of {@code application/json} with an optional {@code charset=utf-8} ({@code
 * 415}), a body of at most {@value #MAX_BODY_BYTES} bytes ({@code 413}, read bounded), then strict
 * JSON: strict UTF-8, one object, keys only {@code userId} (required string), {@code jti} (optional
 * string) and {@code epoch} (optional integer that fits a {@code long}), no duplicate keys, no
 * trailing tokens ({@code 400}). The user id is validated with the protocol {@link UserId}
 * constructor (NFC, no trimming).
 *
 * <p>Outcomes: {@code 201 {"offset": n, "epoch": e}}, {@code 409} for {@link
 * RevocationLog.EpochConflict}, {@code 400} for input the log refuses, empty {@code 503} when the
 * store is unavailable or anything unexpected happens.
 *
 * <p>Status: LOCAL_TEST_ONLY, NOT_QUALIFIED.
 */
final class AdminHandler implements HttpHandler {

  static final String PATH = "/v1/admin/revocations";
  static final int MAX_BODY_BYTES = 4096;

  private static final Pattern JSON_TYPE =
      Pattern.compile("application/json(?:\\s*;\\s*charset=utf-8)?");
  private static final Pattern DECIMAL = Pattern.compile("[0-9]{1,10}");
  private static final Set<String> KEYS = Set.of("userId", "jti", "epoch");
  private static final ObjectMapper JSON =
      JsonMapper.builder()
          .enable(StreamReadFeature.STRICT_DUPLICATE_DETECTION)
          .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS)
          .build();

  private record Revoke(String userId, String jti, OptionalLong epoch) {}

  private final RevocationLog log;

  AdminHandler(RevocationLog log) {
    this.log = Objects.requireNonNull(log, "log");
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
    if (!PATH.equals(uri.getRawPath())) {
      throw new Refusal(Responses.NOT_FOUND);
    }
    if (!"POST".equals(exchange.getRequestMethod())) {
      throw new Refusal(Outcome.methodNotAllowed("POST"));
    }
    requireAdmin(exchange);
    if (uri.getRawQuery() != null || uri.getRawFragment() != null) {
      throw new Refusal(Responses.BAD_REQUEST);
    }
    Headers headers = exchange.getRequestHeaders();
    requireJsonContentType(headers);
    Revoke revoke = parse(body(exchange, headers));
    try {
      RevocationLog.Appended appended = log.append(revoke.userId(), revoke.jti(), revoke.epoch());
      ObjectNode result = JSON.createObjectNode();
      result.put("offset", appended.offset());
      result.put("epoch", appended.epoch());
      return Outcome.json(Responses.CREATED, JSON.writeValueAsString(result));
    } catch (RevocationLog.EpochConflict conflict) {
      throw new Refusal(Responses.CONFLICT);
    } catch (IllegalArgumentException invalid) {
      // The log's own input validation (user id, jti, epoch bounds).
      throw new Refusal(Responses.BAD_REQUEST);
    } catch (JsonProcessingException unexpected) {
      throw new Refusal(Responses.UNAVAILABLE);
    }
  }

  private static void requireAdmin(HttpExchange exchange) throws Refusal {
    try {
      if (!(exchange instanceof HttpsExchange secure)) {
        throw new Refusal(Responses.FORBIDDEN);
      }
      SSLSession session = secure.getSSLSession();
      Certificate[] chain = session == null ? null : session.getPeerCertificates();
      if (chain == null || chain.length == 0 || !(chain[0] instanceof X509Certificate leaf)) {
        throw new Refusal(Responses.FORBIDDEN);
      }
      PeerIdentity.admin(leaf);
    } catch (SSLPeerUnverifiedException | IllegalArgumentException rejected) {
      throw new Refusal(Responses.FORBIDDEN);
    }
  }

  private static void requireJsonContentType(Headers headers) throws Refusal {
    List<String> types = headers.get("Content-Type");
    if (types != null && types.size() > 1) {
      throw new Refusal(Responses.BAD_REQUEST);
    }
    if (types == null
        || types.get(0) == null
        || !JSON_TYPE.matcher(types.get(0).strip().toLowerCase(Locale.ROOT)).matches()) {
      throw new Refusal(Responses.UNSUPPORTED_MEDIA_TYPE);
    }
  }

  /** Reads at most {@value #MAX_BODY_BYTES} bytes; a declared or actual larger body is refused. */
  private static byte[] body(HttpExchange exchange, Headers headers) throws Refusal {
    List<String> length = headers.get("Content-Length");
    if (length != null) {
      if (length.size() != 1
          || length.get(0) == null
          || !DECIMAL.matcher(length.get(0)).matches()) {
        throw new Refusal(Responses.BAD_REQUEST);
      }
      if (Long.parseLong(length.get(0)) > MAX_BODY_BYTES) {
        throw new Refusal(Responses.PAYLOAD_TOO_LARGE);
      }
    }
    try (InputStream in = exchange.getRequestBody()) {
      byte[] body = in.readNBytes(MAX_BODY_BYTES + 1);
      if (body.length > MAX_BODY_BYTES) {
        throw new Refusal(Responses.PAYLOAD_TOO_LARGE);
      }
      return body;
    } catch (IOException unreadable) {
      throw new Refusal(Responses.BAD_REQUEST);
    }
  }

  private static Revoke parse(byte[] body) throws Refusal {
    try {
      String text =
          StandardCharsets.UTF_8
              .newDecoder()
              .onMalformedInput(CodingErrorAction.REPORT)
              .onUnmappableCharacter(CodingErrorAction.REPORT)
              .decode(ByteBuffer.wrap(body))
              .toString();
      JsonNode root = JSON.readTree(text);
      if (root == null || !root.isObject()) {
        throw new Refusal(Responses.BAD_REQUEST);
      }
      for (Iterator<String> names = root.fieldNames(); names.hasNext(); ) {
        if (!KEYS.contains(names.next())) {
          throw new Refusal(Responses.BAD_REQUEST);
        }
      }
      JsonNode user = root.get("userId");
      JsonNode jti = root.get("jti");
      JsonNode epoch = root.get("epoch");
      if (user == null
          || !user.isTextual()
          || jti != null && !jti.isTextual()
          || epoch != null && !(epoch.isIntegralNumber() && epoch.canConvertToLong())) {
        throw new Refusal(Responses.BAD_REQUEST);
      }
      String userId = new UserId(user.textValue()).value();
      return new Revoke(
          userId,
          jti == null ? null : jti.textValue(),
          epoch == null ? OptionalLong.empty() : OptionalLong.of(epoch.longValue()));
    } catch (CharacterCodingException | JsonProcessingException | IllegalArgumentException e) {
      throw new Refusal(Responses.BAD_REQUEST);
    }
  }
}
