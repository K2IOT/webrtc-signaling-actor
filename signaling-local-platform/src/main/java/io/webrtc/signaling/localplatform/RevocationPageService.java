package io.webrtc.signaling.localplatform;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import io.webrtc.signaling.auth.RevocationState;
import io.webrtc.signaling.protocol.Identity.SessionKey;
import io.webrtc.signaling.storage.worker.RevocationReconciler;
import io.webrtc.signaling.storage.worker.RevocationSourceVerifier;
import java.time.Clock;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Base64;
import java.util.List;
import java.util.Objects;
import java.util.regex.Pattern;

/**
 * Builds the signed revocation page the actor-side {@code NativeRevocationSource} polls.
 *
 * <p>One page is one {@link RevocationLog#read} snapshot turned into a {@link
 * RevocationReconciler.Batch}: up to {@value #PAGE_SIZE} events after {@code from}; {@code
 * highWater} is the last returned offset, or the snapshot's high-water mark when the page is empty
 * (a caught-up reader asks from its own high water and gets {@code from == highWater}); {@code
 * currentSourceHighWater} is the snapshot's high-water mark, so a page is "caught up" exactly when
 * it reached it. {@code checkedAt} is the platform clock, truncated to microseconds, but never
 * earlier than the newest commit in the page (the {@code Batch} invariant). No key retirement is
 * modelled: {@code retiredKeys} is always empty.
 *
 * <p>The proof is {@code <keyId>.<base64url(Ed25519 signature)>} over {@link
 * RevocationSourceVerifier#signingBytes} for the requesting cell, the issuer and the unsigned
 * batch, so a page signed for one cell does not verify for another. The events carry the configured
 * issuer; the log itself is issuer-agnostic.
 *
 * <p>The JSON is the plain {@code Batch} record: seven fields, {@code Instant}s as ISO-8601 strings
 * with up to microsecond precision, user ids as plain strings, and an explicit {@code null} {@code
 * jti} for the user scope (the consumer rejects missing creator properties).
 *
 * <p>Status: LOCAL_TEST_ONLY, NOT_QUALIFIED.
 */
public final class RevocationPageService {

  static final int PAGE_SIZE = 16;

  private static final Pattern CELL = Pattern.compile("[a-z][a-z0-9-]{0,23}");

  private static final ObjectMapper JSON =
      new ObjectMapper()
          .findAndRegisterModules()
          .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

  private final SourceSigner signer;
  private final RevocationLog log;
  private final String issuer;
  private final Clock wallClock;

  public RevocationPageService(
      SourceSigner signer, RevocationLog log, String issuer, Clock wallClock) {
    this.signer = Objects.requireNonNull(signer, "signer");
    this.log = Objects.requireNonNull(log, "log");
    this.wallClock = Objects.requireNonNull(wallClock, "wallClock");
    // Same bounded-identifier rules the verifier and the events apply to the issuer.
    new SessionKey(Objects.requireNonNull(issuer, "issuer"), "revocation-source");
    this.issuer = issuer;
  }

  /**
   * Returns the signed page JSON for {@code cell} starting after offset {@code from}.
   *
   * @throws IllegalArgumentException if {@code from} is negative or {@code cell} is not a valid
   *     cell name
   * @throws RevocationLog.CursorAhead if {@code from} is above the log's high-water mark
   * @throws RevocationLog.Unavailable if the log cannot answer or is internally inconsistent
   */
  public String pageBody(String cell, long from) {
    Objects.requireNonNull(cell, "cell");
    if (from < 0 || !CELL.matcher(cell).matches()) {
      throw new IllegalArgumentException();
    }
    RevocationLog.Snapshot snapshot = log.read(from, PAGE_SIZE);
    List<RevocationState.Event> events = snapshot.events().stream().map(this::scoped).toList();
    long highWater;
    if (events.isEmpty()) {
      if (snapshot.currentHighWater() != from) {
        // Offsets are gapless: a non-caught-up reader always gets at least one event.
        throw new RevocationLog.Unavailable();
      }
      highWater = snapshot.currentHighWater();
    } else {
      highWater = events.get(events.size() - 1).sourceOffset();
    }
    Instant now = wallClock.instant();
    Instant checkedAt =
        (snapshot.newestCommittedAt().isAfter(now) ? snapshot.newestCommittedAt() : now)
            .truncatedTo(ChronoUnit.MICROS);
    RevocationReconciler.Batch batch;
    try {
      RevocationReconciler.Batch unsigned =
          new RevocationReconciler.Batch(
              from, highWater, events, checkedAt, "", List.of(), snapshot.currentHighWater());
      byte[] signature = signer.sign(RevocationSourceVerifier.signingBytes(cell, issuer, unsigned));
      batch =
          new RevocationReconciler.Batch(
              from,
              highWater,
              events,
              checkedAt,
              signer.keyId()
                  + "."
                  + Base64.getUrlEncoder().withoutPadding().encodeToString(signature),
              List.of(),
              snapshot.currentHighWater());
    } catch (IllegalArgumentException inconsistent) {
      // The log returned a page that violates the Batch invariants (ordering, bounds).
      throw new RevocationLog.Unavailable();
    }
    try {
      return JSON.writeValueAsString(batch);
    } catch (JsonProcessingException e) {
      throw new IllegalStateException("revocation page could not be serialised", null);
    }
  }

  private RevocationState.Event scoped(RevocationState.Event event) {
    return new RevocationState.Event(
        issuer,
        event.userId(),
        event.jti(),
        event.epoch(),
        event.sourceOffset(),
        event.committedAt().truncatedTo(ChronoUnit.MICROS));
  }
}
