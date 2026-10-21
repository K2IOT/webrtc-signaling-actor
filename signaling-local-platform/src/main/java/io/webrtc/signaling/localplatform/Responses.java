package io.webrtc.signaling.localplatform;

import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;

/**
 * The only way any listener writes a response, so the framing rules hold everywhere.
 *
 * <p>Every response has an exact {@code Content-Length} (never chunked, never compressed, never a
 * redirect) and {@code Connection: close}. A body is {@code application/json}; error responses have
 * an empty body and carry no detail. A body above {@value #MAX_BODY_BYTES} bytes is never sent: the
 * response becomes an empty {@code 503} instead.
 *
 * <p>Status: LOCAL_TEST_ONLY, NOT_QUALIFIED.
 */
final class Responses {

  /** Largest body the consumer ({@code NativeSourceHttp}) accepts. */
  static final int MAX_BODY_BYTES = 32768;

  static final int OK = 200;
  static final int CREATED = 201;
  static final int BAD_REQUEST = 400;
  static final int FORBIDDEN = 403;
  static final int NOT_FOUND = 404;
  static final int METHOD_NOT_ALLOWED = 405;
  static final int CONFLICT = 409;
  static final int PAYLOAD_TOO_LARGE = 413;
  static final int UNSUPPORTED_MEDIA_TYPE = 415;
  static final int UNAVAILABLE = 503;

  private static final byte[] EMPTY = new byte[0];

  /** A status and a body; an empty body means "no body". */
  record Outcome(int status, byte[] body, String allow) {
    static Outcome empty(int status) {
      return new Outcome(status, EMPTY, null);
    }

    static Outcome json(int status, String body) {
      return new Outcome(status, body.getBytes(StandardCharsets.UTF_8), null);
    }

    static Outcome methodNotAllowed(String allow) {
      return new Outcome(METHOD_NOT_ALLOWED, EMPTY, allow);
    }
  }

  /** Refuses a request with an empty-bodied status; carries no message, cause or stack trace. */
  static final class Refusal extends Exception {
    private static final long serialVersionUID = 1L;

    private final transient Outcome outcome;

    Refusal(int status) {
      this(Outcome.empty(status));
    }

    Refusal(Outcome outcome) {
      super(null, null, false, false);
      this.outcome = outcome;
    }

    Outcome outcome() {
      return outcome;
    }
  }

  private Responses() {}

  /** Writes {@code outcome} and closes the exchange; an I/O failure only means the peer left. */
  static void send(HttpExchange exchange, Outcome outcome) {
    try {
      boolean fits = outcome.body().length <= MAX_BODY_BYTES;
      byte[] body = fits ? outcome.body() : EMPTY;
      int status = fits ? outcome.status() : UNAVAILABLE;
      Headers headers = exchange.getResponseHeaders();
      headers.set("Connection", "close");
      headers.set("Cache-Control", "no-store");
      if (body.length > 0) {
        headers.set("Content-Type", "application/json");
      }
      if (fits && outcome.allow() != null) {
        headers.set("Allow", outcome.allow());
      }
      // -1 makes the JDK server send "Content-Length: 0"; 0 would mean chunked.
      exchange.sendResponseHeaders(status, body.length == 0 ? -1 : body.length);
      if (body.length > 0) {
        try (OutputStream out = exchange.getResponseBody()) {
          out.write(body);
        }
      }
    } catch (IOException peerGone) {
      // Nothing to report: the connection is closed below either way.
    } finally {
      exchange.close();
    }
  }
}
