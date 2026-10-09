package io.webrtc.signaling.loadgen;

import static org.assertj.core.api.Assertions.*;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.*;
import org.junit.jupiter.api.Test;

class SkewTargetSelectorTest {
  static int bucket(String user) throws Exception {
    var d = MessageDigest.getInstance("SHA-256").digest(user.getBytes(StandardCharsets.UTF_8));
    return ((d[6] & 63) << 8) | (d[7] & 255);
  }

  static String[] directory(int cells) {
    var values = new String[16384];
    for (int i = 0; i < values.length; i++) values[i] = "c%03d".formatted(i % cells + 1);
    return values;
  }

  @Test
  void actualTargetPopulationHasFiveTimesBaselineDestinationAndBucketArrivalShare()
      throws Exception {
    var directory = directory(50);
    var selector = new SkewTargetSelector(20261005, 200000, "TEST_ONLY_user", directory, 5, 5);
    long cell = 0, bucket = 0;
    int samples = 1000000;
    for (int ordinal = 0; ordinal < samples; ordinal++) {
      long target = selector.target(ordinal);
      assertThat(target).isBetween(100000L, 199999L);
      int nativeBucket = bucket("TEST_ONLY_user" + target);
      if (directory[nativeBucket].equals(selector.destinationCell())) cell++;
      if (nativeBucket == selector.hotBucket()) bucket++;
    }
    assertThat((double) cell / samples)
        .isCloseTo(5.0 * selector.destinationUsers() / selector.targetUsers(), within(.003));
    assertThat((double) bucket / samples)
        .isCloseTo(5.0 * selector.bucketUsers() / selector.targetUsers(), within(.00008));
    assertThat(directory[selector.hotBucket()]).isNotEqualTo(selector.destinationCell());
  }

  @Test
  void originalGlobalOrdinalProducesSameTargetAcrossWorkerPartitions() throws Exception {
    var a = new SkewTargetSelector(20261005, 20000, "TEST_ONLY_user", directory(50), 5, 5);
    var b = new SkewTargetSelector(20261005, 20000, "TEST_ONLY_user", directory(50), 5, 5);
    for (int ordinal = 0; ordinal < 10000; ordinal++)
      assertThat(a.target(ordinal)).isEqualTo(b.target(ordinal));
    assertThat(a.destinationCell()).isEqualTo(b.destinationCell());
    assertThat(a.hotBucket()).isEqualTo(b.hotBucket());
  }

  @Test
  void impossibleDestinationShareRejectsInsteadOfSilentlyReducingFiveTimesDemand() {
    assertThatThrownBy(() -> new SkewTargetSelector(1, 20000, "TEST_ONLY_user", directory(1), 5, 5))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("SKEW_PROFILE_IMPOSSIBLE");
  }

  @Test
  void bucketOnlySkewWorksInOneNativeCellWithoutInventingHotDestination() throws Exception {
    var selector = new SkewTargetSelector(20261005, 200000, "TEST_ONLY_user", directory(1), 1, 5);
    assertThat(selector.destinationCell()).isNull();
    assertThat(selector.destinationUsers()).isZero();
    long hits = 0;
    for (int ordinal = 0; ordinal < 1000000; ordinal++)
      if (bucket("TEST_ONLY_user" + selector.target(ordinal)) == selector.hotBucket()) hits++;
    assertThat(hits / 1000000.0)
        .isCloseTo(5.0 * selector.bucketUsers() / selector.targetUsers(), within(.00008));
  }

  @Test
  void destinationOnlySkewDoesNotInventAnAdditionalHotBucket() {
    var selector = new SkewTargetSelector(20261005, 20000, "TEST_ONLY_user", directory(50), 5, 1);
    assertThat(selector.hotBucket()).isEqualTo(-1);
    assertThat(selector.bucketUsers()).isZero();
  }
}
