package io.webrtc.signaling.localplatform;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.StreamReadConstraints;
import com.fasterxml.jackson.core.StreamReadFeature;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.webrtc.signaling.auth.AuthPrincipal;
import io.webrtc.signaling.auth.AuthorizationStatus;
import io.webrtc.signaling.auth.RevocationState;
import io.webrtc.signaling.protocol.Identity.SessionKey;
import io.webrtc.signaling.protocol.Identity.UserId;
import io.webrtc.signaling.storage.worker.RevocationReconciler;
import io.webrtc.signaling.storage.worker.RevocationSourceVerifier;
import java.security.KeyPair;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import org.junit.jupiter.api.Test;

class RevocationPageServiceTest {

  private static final String ISSUER = "https://issuer.example.test/local";
  private static final String KEY_ID = "local-source-1";

  /** 2026-01-01T00:00:10Z plus 1234 ns so truncation to microseconds is observable. */
  private static final Instant NOW = Instant.parse("2026-01-01T00:00:10Z").plusNanos(1234);

  /**
   * Same parsing configuration as the actor-side {@code NativeRevocationSource}: strict duplicates,
   * bounded strings, no scalar coercion, missing creator properties and trailing tokens rejected,
   * unknown properties rejected (Jackson default).
   */
  private static final ObjectMapper CONSUMER =
      new ObjectMapper(
              JsonFactory.builder()
                  .enable(StreamReadFeature.STRICT_DUPLICATE_DETECTION)
                  .streamReadConstraints(
                      StreamReadConstraints.builder()
                          .maxNestingDepth(8)
                          .maxStringLength(8192)
                          .maxNumberLength(20)
                          .build())
                  .build())
          .findAndRegisterModules()
          .disable(MapperFeature.ALLOW_COERCION_OF_SCALARS)
          .enable(
              DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES,
              DeserializationFeature.FAIL_ON_MISSING_CREATOR_PROPERTIES,
              DeserializationFeature.FAIL_ON_TRAILING_TOKENS);

  private final KeyPair pair = TestKeys.generate("Ed25519");
  private final SourceSigner signer = SourceSigner.of(pair, KEY_ID);
  private final FakeLog log = new FakeLog();
  private final Clock clock = Clock.fixed(NOW, ZoneOffset.UTC);
  private final RevocationPageService service =
      new RevocationPageService(signer, log, ISSUER, clock);

  private RevocationSourceVerifier verifier(String cell) {
    return new RevocationSourceVerifier(cell, ISSUER, Map.of(KEY_ID, pair.getPublic()));
  }

  private RevocationReconciler.Batch parse(String body) throws Exception {
    return CONSUMER.readValue(body, RevocationReconciler.Batch.class);
  }

  @Test
  void emptyCaughtUpPageHasEqualOffsets() throws Exception {
    RevocationReconciler.Batch fresh = parse(service.pageBody("c001", 0));
    log.append("alice", null, OptionalLong.empty());
    log.append("bob", null, OptionalLong.empty());
    RevocationReconciler.Batch caughtUp = parse(service.pageBody("c001", 2));

    assertThat(fresh.fromOffset()).isZero();
    assertThat(fresh.highWater()).isZero();
    assertThat(fresh.currentSourceHighWater()).isZero();
    assertThat(fresh.events()).isEmpty();
    assertThat(fresh.caughtUp()).isTrue();
    assertThat(caughtUp.fromOffset()).isEqualTo(2);
    assertThat(caughtUp.highWater()).isEqualTo(2);
    assertThat(caughtUp.currentSourceHighWater()).isEqualTo(2);
    assertThat(caughtUp.events()).isEmpty();
    assertThat(caughtUp.caughtUp()).isTrue();
    assertThat(caughtUp.retiredKeys()).isEmpty();
  }

  @Test
  void fullPageStopsAtSixteenAndIsNotCaughtUp() throws Exception {
    for (int i = 0; i < 20; i++) {
      log.append("user-" + i, null, OptionalLong.empty());
    }

    RevocationReconciler.Batch first = parse(service.pageBody("c001", 0));
    RevocationReconciler.Batch second = parse(service.pageBody("c001", first.highWater()));

    assertThat(first.events()).hasSize(16);
    assertThat(first.fromOffset()).isZero();
    assertThat(first.highWater()).isEqualTo(16);
    assertThat(first.currentSourceHighWater()).isEqualTo(20);
    assertThat(first.caughtUp()).isFalse();
    assertThat(second.events()).hasSize(4);
    assertThat(second.fromOffset()).isEqualTo(16);
    assertThat(second.highWater()).isEqualTo(20);
    assertThat(second.caughtUp()).isTrue();
  }

  @Test
  void checkedAtIsNowTruncatedToMicrosecondsUnlessAnEventIsNewer() throws Exception {
    log.append("alice", null, OptionalLong.empty());

    RevocationReconciler.Batch batch = parse(service.pageBody("c001", 0));
    assertThat(batch.checkedAt()).isEqualTo(NOW.truncatedTo(ChronoUnit.MICROS));
    assertThat(batch.checkedAt().getNano() % 1000).isZero();

    // A commit stamped after the platform clock (clock skew) must never exceed checkedAt, or the
    // Batch invariant would reject the page: checkedAt follows the newest commit instead.
    Instant future = NOW.plusSeconds(30).truncatedTo(ChronoUnit.MICROS);
    log.appendAt("bob", null, future);
    RevocationReconciler.Batch skewed = parse(service.pageBody("c001", 0));
    assertThat(skewed.checkedAt()).isEqualTo(future);
    assertThat(verifier("c001").test(skewed)).isTrue();
  }

  @Test
  void proofVerifiesWithRevocationSourceVerifier() throws Exception {
    log.append("alice", null, OptionalLong.empty());
    log.append("alice", "jti-1", OptionalLong.empty());

    String body = service.pageBody("c001", 0);
    RevocationReconciler.Batch batch = parse(body);

    assertThat(verifier("c001").test(batch)).isTrue();
    assertThat(batch.sourceProof()).startsWith(KEY_ID + ".");
    assertThat(batch.sourceProof()).hasSize(KEY_ID.length() + 1 + 86);
    assertThat(batch.events()).allSatisfy(e -> assertThat(e.issuer()).isEqualTo(ISSUER));

    // Any change to the signed content breaks the proof.
    RevocationReconciler.Batch otherWaterMark =
        new RevocationReconciler.Batch(
            batch.fromOffset(),
            batch.highWater(),
            batch.events(),
            batch.checkedAt(),
            batch.sourceProof(),
            List.of(),
            batch.currentSourceHighWater() + 1);
    RevocationReconciler.Batch otherTime =
        new RevocationReconciler.Batch(
            batch.fromOffset(),
            batch.highWater(),
            batch.events(),
            batch.checkedAt().plusNanos(1000),
            batch.sourceProof(),
            List.of(),
            batch.currentSourceHighWater());
    List<RevocationState.Event> tampered = new ArrayList<>(batch.events());
    RevocationState.Event first = tampered.get(0);
    tampered.set(
        0,
        new RevocationState.Event(
            first.issuer(),
            first.userId(),
            first.jti(),
            first.epoch() + 1,
            first.sourceOffset(),
            first.committedAt()));
    RevocationReconciler.Batch otherEvents =
        new RevocationReconciler.Batch(
            batch.fromOffset(),
            batch.highWater(),
            tampered,
            batch.checkedAt(),
            batch.sourceProof(),
            List.of(),
            batch.currentSourceHighWater());
    assertThat(verifier("c001").test(otherWaterMark)).isFalse();
    assertThat(verifier("c001").test(otherTime)).isFalse();
    assertThat(verifier("c001").test(otherEvents)).isFalse();
  }

  @Test
  void proofRejectedForAnotherCell() throws Exception {
    log.append("alice", null, OptionalLong.empty());

    RevocationReconciler.Batch forC001 = parse(service.pageBody("c001", 0));

    assertThat(verifier("c001").test(forC001)).isTrue();
    assertThat(verifier("c002").test(forC001)).isFalse();
  }

  @Test
  void proofRejectedUnderAnotherKey() throws Exception {
    RevocationReconciler.Batch batch = parse(service.pageBody("c001", 0));
    KeyPair stranger = TestKeys.generate("Ed25519");

    RevocationSourceVerifier foreign =
        new RevocationSourceVerifier("c001", ISSUER, Map.of(KEY_ID, stranger.getPublic()));

    assertThat(foreign.test(batch)).isFalse();
  }

  @Test
  void pageJsonRoundTripsThroughTheSameParserTheActorUses() throws Exception {
    log.append("Aliceé", null, OptionalLong.empty());
    log.append("Aliceé", "jti-1", OptionalLong.empty());
    log.append("bob", null, OptionalLong.empty());

    String body = service.pageBody("c001", 0);
    RevocationReconciler.Batch batch = parse(body);

    // Exactly the seven Batch fields; caughtUp() must not leak as an extra property.
    JsonNode tree = CONSUMER.readTree(body);
    assertThat(fieldNames(tree))
        .containsExactlyInAnyOrder(
            "fromOffset",
            "highWater",
            "events",
            "checkedAt",
            "sourceProof",
            "retiredKeys",
            "currentSourceHighWater");
    assertThat(tree.get("retiredKeys").isArray()).isTrue();
    assertThat(tree.get("retiredKeys")).isEmpty();
    // Counters are JSON numbers, user ids are plain strings, a user-scope jti is an explicit null.
    JsonNode firstEvent = tree.get("events").get(0);
    assertThat(fieldNames(firstEvent))
        .containsExactlyInAnyOrder(
            "issuer", "userId", "jti", "epoch", "sourceOffset", "committedAt");
    assertThat(firstEvent.get("userId").isTextual()).isTrue();
    assertThat(firstEvent.get("userId").asText()).isEqualTo("Aliceé");
    assertThat(firstEvent.get("jti").isNull()).isTrue();
    assertThat(firstEvent.get("epoch").isIntegralNumber()).isTrue();
    assertThat(firstEvent.get("sourceOffset").isIntegralNumber()).isTrue();
    assertThat(tree.get("fromOffset").isIntegralNumber()).isTrue();
    assertThat(tree.get("checkedAt").isTextual()).isTrue();

    assertThat(batch.events()).hasSize(3);
    RevocationState.Event e0 = batch.events().get(0);
    RevocationState.Event e1 = batch.events().get(1);
    assertThat(e0.userId()).isEqualTo(new UserId("Aliceé"));
    assertThat(e0.issuer()).isEqualTo(ISSUER);
    assertThat(e0.jti()).isNull();
    assertThat(e0.epoch()).isZero();
    assertThat(e0.sourceOffset()).isEqualTo(1);
    assertThat(e1.jti()).isEqualTo("jti-1");
    assertThat(e1.sourceOffset()).isEqualTo(2);
    // Instants survive at full microsecond precision, which is what is signed.
    assertThat(e0.committedAt()).isEqualTo(log.events().get(0).committedAt());
    assertThat(batch.checkedAt()).isEqualTo(NOW.truncatedTo(ChronoUnit.MICROS));
    assertThat(verifier("c001").test(batch)).isTrue();
  }

  @Test
  void revokedUserMakesEpochZeroTokenRevoked() throws Exception {
    RevocationState state = new RevocationState(new MapStore(), Duration.ofSeconds(5));
    log.append("alice", null, OptionalLong.empty());
    log.append("carol", "jti-carol", OptionalLong.empty());

    RevocationReconciler.Batch batch = parse(service.pageBody("c001", 0));
    apply(state, batch);
    Instant now = batch.checkedAt().plusSeconds(1);

    // User-scope revoke of alice: every one of her epoch-0 tokens is revoked.
    assertThat(state.checkRevocation(principal("alice", "any-jti", 0), now))
        .isEqualTo(AuthorizationStatus.REVOKED);
    // Session-scope revoke of carol/jti-carol: only that session.
    assertThat(state.checkRevocation(principal("carol", "jti-carol", 0), now))
        .isEqualTo(AuthorizationStatus.REVOKED);
    assertThat(state.checkRevocation(principal("carol", "other-jti", 0), now))
        .isEqualTo(AuthorizationStatus.ALLOWED);
    // A token minted under a later security epoch is not revoked by the epoch-0 event.
    assertThat(state.checkRevocation(principal("alice", "any-jti", 1), now))
        .isEqualTo(AuthorizationStatus.ALLOWED);
  }

  @Test
  void unrevokedUserKeepsEpochZeroTokenAllowed() throws Exception {
    RevocationState state = new RevocationState(new MapStore(), Duration.ofSeconds(5));
    log.append("alice", null, OptionalLong.empty());

    RevocationReconciler.Batch batch = parse(service.pageBody("c001", 0));
    apply(state, batch);
    Instant now = batch.checkedAt().plusSeconds(1);

    assertThat(state.checkRevocation(principal("bob", "jti-bob", 0), now))
        .isEqualTo(AuthorizationStatus.ALLOWED);

    // And with no events at all the epoch-0 token is allowed once the reader is caught up.
    FakeLog empty = new FakeLog();
    RevocationState fresh = new RevocationState(new MapStore(), Duration.ofSeconds(5));
    RevocationReconciler.Batch none =
        parse(new RevocationPageService(signer, empty, ISSUER, clock).pageBody("c001", 0));
    apply(fresh, none);
    assertThat(
            fresh.checkRevocation(principal("alice", "jti-1", 0), none.checkedAt().plusSeconds(1)))
        .isEqualTo(AuthorizationStatus.ALLOWED);
  }

  @Test
  void cursorAheadPropagatesFromTheLog() {
    assertThatThrownBy(() -> service.pageBody("c001", 5))
        .isInstanceOf(RevocationLog.CursorAhead.class);
  }

  @Test
  void invalidArgumentsAreRejected() {
    assertThatThrownBy(() -> service.pageBody("c001", -1))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> service.pageBody("C001", 0))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> service.pageBody("", 0)).isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> service.pageBody(null, 0)).isInstanceOf(NullPointerException.class);
    assertThatThrownBy(() -> new RevocationPageService(signer, log, " ", clock))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void aLogWithAGapIsReportedUnavailableNotSigned() {
    FakeLog gappy =
        new FakeLog() {
          @Override
          public Snapshot read(long from, int limit) {
            return new Snapshot(List.of(), 7, Instant.EPOCH);
          }
        };

    assertThatThrownBy(
            () -> new RevocationPageService(signer, gappy, ISSUER, clock).pageBody("c001", 3))
        .isInstanceOf(RevocationLog.Unavailable.class);
  }

  private static List<String> fieldNames(JsonNode node) {
    List<String> names = new ArrayList<>();
    node.fieldNames().forEachRemaining(names::add);
    return names;
  }

  private static void apply(RevocationState state, RevocationReconciler.Batch batch) {
    for (RevocationState.Event event : batch.events()) {
      state.apply(event);
    }
    state.markReconciled(batch.highWater(), batch.checkedAt());
  }

  private static AuthPrincipal principal(String user, String jti, long securityEpoch) {
    return new AuthPrincipal(
        new UserId(user),
        new SessionKey(ISSUER, jti),
        NOW.plusSeconds(3600),
        NOW.minusSeconds(60),
        "key-1",
        securityEpoch);
  }

  /** Store with the semantics of {@code TestSecurityStore} in signaling-auth. */
  private static final class MapStore implements RevocationState.Store {
    private record Key(String issuer, UserId user, String jti) {}

    private final Map<Key, Long> epochs = new HashMap<>();
    private RevocationState.Progress progress;

    @Override
    public synchronized boolean apply(RevocationState.Event e) {
      Key key = new Key(e.issuer(), e.userId(), e.jti());
      if (epochs.getOrDefault(key, -1L) >= e.epoch()) {
        return false;
      }
      epochs.put(key, e.epoch());
      return true;
    }

    @Override
    public synchronized long epoch(String issuer, UserId user, String jti) {
      return epochs.getOrDefault(new Key(issuer, user, jti), -1L);
    }

    @Override
    public synchronized RevocationState.Progress progress() {
      return progress;
    }

    @Override
    public synchronized void reconcile(long offset, Instant checkedAt) {
      if (progress == null || offset >= progress.offset()) {
        progress = new RevocationState.Progress(offset, checkedAt);
      }
    }
  }

  /** In-memory {@link RevocationLog}: same offset, epoch and snapshot rules as the SQL one. */
  private static class FakeLog implements RevocationLog {
    private final List<RevocationState.Event> events = new ArrayList<>();

    synchronized List<RevocationState.Event> events() {
      return List.copyOf(events);
    }

    @Override
    public synchronized Appended append(String userId, String jtiOrNull, OptionalLong epoch) {
      return appendAt(
          userId,
          jtiOrNull,
          NOW.minusSeconds(2)
              .truncatedTo(ChronoUnit.MICROS)
              .plusNanos(123_000L * (events.size() + 1)),
          epoch);
    }

    synchronized Appended appendAt(String userId, String jtiOrNull, Instant at) {
      return appendAt(userId, jtiOrNull, at, OptionalLong.empty());
    }

    private Appended appendAt(String userId, String jtiOrNull, Instant at, OptionalLong epoch) {
      UserId user = new UserId(userId);
      long latest = -1;
      for (RevocationState.Event e : events) {
        if (e.userId().equals(user) && java.util.Objects.equals(e.jti(), jtiOrNull)) {
          latest = Math.max(latest, e.epoch());
        }
      }
      long chosen = epoch.orElse(latest + 1);
      if (chosen <= latest) {
        throw new EpochConflict();
      }
      long offset = events.size() + 1L;
      events.add(new RevocationState.Event(UNSCOPED_ISSUER, user, jtiOrNull, chosen, offset, at));
      return new Appended(offset, chosen);
    }

    @Override
    public synchronized Snapshot read(long from, int limit) {
      long high = events.size();
      if (from > high) {
        throw new CursorAhead();
      }
      List<RevocationState.Event> page =
          events.stream().filter(e -> e.sourceOffset() > from).limit(limit).toList();
      Instant newest =
          page.stream()
              .map(RevocationState.Event::committedAt)
              .max(Instant::compareTo)
              .orElse(Instant.EPOCH);
      return new Snapshot(page, high, newest);
    }
  }
}
