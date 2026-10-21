package io.webrtc.signaling.localplatform;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import io.webrtc.signaling.localplatform.Responses.Outcome;
import java.net.URI;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Objects;
import javax.sql.DataSource;

/**
 * Plain-HTTP readiness on the configured concrete health address: {@code GET /ready}.
 *
 * <p>{@code 200} only when the host clock observer reports {@code continuous} and the database
 * answers a query against the migrated schema ({@code SELECT 1 FROM source_state}, which proves
 * both that the database answers and that the source migration is applied). Anything else is an
 * empty {@code 503}. Other paths are {@code 404}, other methods {@code 405}. No detail is ever
 * returned.
 *
 * <p>Status: LOCAL_TEST_ONLY, NOT_QUALIFIED.
 */
final class ReadyHandler implements HttpHandler {

  static final String PATH = "/ready";

  private static final String PROBE = "SELECT 1 FROM source_state";
  private static final int PROBE_TIMEOUT_SECONDS = 2;

  private final DataSource dataSource;
  private final HostClockObserver observer;

  ReadyHandler(DataSource dataSource, HostClockObserver observer) {
    this.dataSource = Objects.requireNonNull(dataSource, "dataSource");
    this.observer = Objects.requireNonNull(observer, "observer");
  }

  @Override
  public void handle(HttpExchange exchange) {
    Outcome outcome;
    try {
      outcome = serve(exchange);
    } catch (RuntimeException unexpected) {
      outcome = Outcome.empty(Responses.UNAVAILABLE);
    }
    Responses.send(exchange, outcome);
  }

  private Outcome serve(HttpExchange exchange) {
    URI uri = exchange.getRequestURI();
    if (!PATH.equals(uri.getRawPath()) || uri.getRawQuery() != null) {
      return Outcome.empty(Responses.NOT_FOUND);
    }
    if (!"GET".equals(exchange.getRequestMethod())) {
      return Outcome.methodNotAllowed("GET");
    }
    boolean ready = observer.snapshot().continuous() && databaseAnswers();
    return Outcome.empty(ready ? Responses.OK : Responses.UNAVAILABLE);
  }

  private boolean databaseAnswers() {
    try (Connection connection = dataSource.getConnection();
        PreparedStatement probe = connection.prepareStatement(PROBE)) {
      probe.setQueryTimeout(PROBE_TIMEOUT_SECONDS);
      try (ResultSet row = probe.executeQuery()) {
        return row.next();
      }
    } catch (SQLException | RuntimeException unavailable) {
      return false;
    }
  }
}
