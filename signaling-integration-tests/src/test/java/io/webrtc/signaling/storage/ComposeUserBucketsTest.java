package io.webrtc.signaling.storage;

import static org.assertj.core.api.Assertions.assertThat;

import io.webrtc.signaling.protocol.Identity.UserId;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * Pins the compose smoke users to their cells (SP3 Task 6, spec section 7 step 2: the two users
 * must live in different cells). {@code topology.env} carries the account user ids; cell {@code
 * c001} owns the buckets below {@code SPLIT}, {@code c002} the rest. The fixed buckets below also
 * pin {@link SessionRegistryService#bucket(UserId)}, so a change of the hash fails here instead of
 * silently moving the users. LOCAL_TEST_ONLY / NOT_QUALIFIED; no Docker.
 */
class ComposeUserBucketsTest {
  private static final Map<String, String> TOPOLOGY = readTopology();

  @Test
  void bucketFunctionIsPinned() {
    assertThat(SessionRegistryService.bucket(new UserId("6acdfd9e63f4055d3f6672d4")))
        .isEqualTo(3502);
    assertThat(SessionRegistryService.bucket(new UserId("6acdfda663f4055d3f6672d7")))
        .isEqualTo(10942);
    assertThat(SessionRegistryService.bucket(new UserId("TEST_ONLY_FIXED_A"))).isEqualTo(5071);
    assertThat(SessionRegistryService.bucket(new UserId("user-pin-b"))).isEqualTo(1096);
  }

  @Test
  void accountIdsAreSetAndWellFormed() {
    var admin = TOPOLOGY.get("ACCOUNT_ADMIN_USER_ID");
    var mod = TOPOLOGY.get("ACCOUNT_MOD_USER_ID");
    assertThat(admin).isNotNull().matches("[0-9a-f]{24}");
    assertThat(mod).isNotNull().matches("[0-9a-f]{24}");
    assertThat(admin).isNotEqualTo(mod);
    assertThat(new UserId(admin).value()).isEqualTo(admin);
    assertThat(new UserId(mod).value()).isEqualTo(mod);
  }

  @Test
  void adminAndModLiveInDifferentCells() {
    var split = Integer.parseInt(TOPOLOGY.get("SPLIT"));
    var admin = SessionRegistryService.bucket(new UserId(TOPOLOGY.get("ACCOUNT_ADMIN_USER_ID")));
    var mod = SessionRegistryService.bucket(new UserId(TOPOLOGY.get("ACCOUNT_MOD_USER_ID")));
    assertThat(admin < split)
        .as("admin bucket %d, mod bucket %d, split %d", admin, mod, split)
        .isNotEqualTo(mod < split);
  }

  @Test
  void adminIsInC001AndModInC002() {
    var split = Integer.parseInt(TOPOLOGY.get("SPLIT"));
    assertThat(cellOf(TOPOLOGY.get("ACCOUNT_ADMIN_USER_ID"), split)).isEqualTo("c001");
    assertThat(cellOf(TOPOLOGY.get("ACCOUNT_MOD_USER_ID"), split)).isEqualTo("c002");
  }

  private static String cellOf(String userId, int split) {
    return SessionRegistryService.bucket(new UserId(userId)) < split ? "c001" : "c002";
  }

  private static Map<String, String> readTopology() {
    try {
      var values = new LinkedHashMap<String, String>();
      var file = repoRoot().resolve("deploy/compose/topology.env");
      for (var line : Files.readAllLines(file, StandardCharsets.UTF_8)) {
        if (line.isBlank() || line.startsWith("#") || !line.contains("=")) continue;
        var value = line.substring(line.indexOf('=') + 1);
        if (value.length() >= 2
            && (value.startsWith("'") && value.endsWith("'")
                || value.startsWith("\"") && value.endsWith("\""))) {
          value = value.substring(1, value.length() - 1);
        }
        values.put(line.substring(0, line.indexOf('=')), value);
      }
      return values;
    } catch (IOException e) {
      throw new IllegalStateException("topology.env unreadable", e);
    }
  }

  private static Path repoRoot() {
    var dir = Path.of(System.getProperty("user.dir")).toAbsolutePath();
    for (var at = dir; at != null; at = at.getParent())
      if (Files.isRegularFile(at.resolve("deploy/compose/topology.env"))) return at;
    throw new IllegalStateException("repository root not found above " + dir);
  }
}
