package io.webrtc.signaling.localplatform;

import io.webrtc.signaling.protocol.Identity.UserId;
import io.webrtc.signaling.storage.SessionRegistryService;
import java.util.Map;
import java.util.Objects;

/**
 * Which user buckets each cell is home for. A revocation page for a cell carries only the events
 * of its home users ("complete cell-filtered coverage", config/native-sources.md): a cell fences
 * its whole feed on an event in a bucket it does not own.
 *
 * <p>Status: LOCAL_TEST_ONLY, NOT_QUALIFIED.
 */
public final class HomeBuckets {

  /** Inclusive bucket range, within 0..16383. */
  public record Range(int first, int last) {
    public Range {
      if (first < 0 || last > 16383 || first > last) {
        throw new IllegalArgumentException("invalid home bucket range");
      }
    }

    boolean contains(int bucket) {
      return bucket >= first && bucket <= last;
    }
  }

  private static final HomeBuckets EVERYWHERE = new HomeBuckets(null);

  private final Map<String, Range> ranges;

  private HomeBuckets(Map<String, Range> ranges) {
    this.ranges = ranges;
  }

  /** One cell owns every bucket: its pages carry every event. */
  public static HomeBuckets everywhere() {
    return EVERYWHERE;
  }

  /**
   * One range per cell; together they must cover 0..16383 exactly. A bucket outside every range
   * would have its revocations filtered from every page while the cursors advance past them, so a
   * revoked token would keep working (fail open). Gaps and overlaps are refused.
   */
  public static HomeBuckets of(Map<String, Range> ranges) {
    var copy = Map.copyOf(Objects.requireNonNull(ranges, "ranges"));
    var sorted =
        copy.values().stream().sorted(java.util.Comparator.comparingInt(Range::first)).toList();
    int next = 0;
    for (Range range : sorted) {
      if (range.first() != next) {
        throw new IllegalArgumentException("home bucket ranges must tile 0..16383");
      }
      next = range.last() + 1;
    }
    if (next != 16384) {
      throw new IllegalArgumentException("home bucket ranges must tile 0..16383");
    }
    return new HomeBuckets(copy);
  }

  /** Whether {@code cell} is the home of {@code user}. */
  public boolean home(String cell, UserId user) {
    if (ranges == null) {
      return true;
    }
    Range range = ranges.get(cell);
    return range != null && range.contains(SessionRegistryService.bucket(user));
  }

  @Override
  public String toString() {
    return "HomeBuckets[LOCAL_TEST_ONLY]";
  }
}
