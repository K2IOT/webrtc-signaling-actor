package io.webrtc.signaling.localplatform;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class LocalPlatformConfigTest {

  private static final String VALID =
      """
      {
        "cells": {"cell-a": 1, "cell-b": 7},
        "keyId": "TEST_ONLY-key-1",
        "signingKey": "/run/test-only/platform/signing.key",
        "source": {
          "bind": "192.0.2.10", "port": 9443,
          "keystore": "/run/test-only/platform/source.p12",
          "keystorePasswordFile": "/run/test-only/platform/source.pass",
          "clientTrust": "/run/test-only/platform/source-trust.p12"
        },
        "admin": {
          "bind": "192.0.2.11", "port": 9444,
          "keystore": "/run/test-only/platform/admin.p12",
          "keystorePasswordFile": "/run/test-only/platform/admin.pass",
          "clientTrust": "/run/test-only/platform/admin-trust.p12"
        },
        "health": {"bind": "192.0.2.12", "port": 9445},
        "database": {
          "jdbcUrl": "jdbc:postgresql://192.0.2.20:5432/test_only",
          "user": "test_only_user",
          "passwordFile": "/run/test-only/platform/db.pass"
        }
      }
      """;

  @TempDir Path dir;

  private Map<String, String> env(String json) throws IOException {
    Path file = dir.resolve("platform.json");
    Files.writeString(file, json, StandardCharsets.UTF_8);
    Map<String, String> env = new HashMap<>();
    env.put("SIGNALING_PLATFORM_CONFIG", file.toString());
    env.put("SIGNALING_LOCAL_ACKNOWLEDGEMENT", "LOCAL_TEST_ONLY");
    env.put("SIGNALING_IDENTITY_ISSUER", "https://issuer.test-only.invalid");
    return env;
  }

  private void assertRejected(Map<String, String> env, LocalPlatformFailure.Category category) {
    assertThatThrownBy(() -> LocalPlatformConfig.load(env))
        .isInstanceOfSatisfying(
            LocalPlatformFailure.class,
            failure -> {
              assertThat(failure.category()).isEqualTo(category);
              assertThat(failure.message()).isEqualTo(category.name());
              assertThat(failure.getMessage()).isEqualTo(category.name());
            });
  }

  private void assertConfigRejected(String json) throws IOException {
    assertRejected(env(json), LocalPlatformFailure.Category.CONFIG);
  }

  @Test
  void missingAcknowledgementIsRejected() throws IOException {
    Map<String, String> env = env(VALID);
    env.remove("SIGNALING_LOCAL_ACKNOWLEDGEMENT");
    assertRejected(env, LocalPlatformFailure.Category.ACKNOWLEDGEMENT);
  }

  @Test
  void wrongAcknowledgementValueIsRejected() throws IOException {
    for (String wrong : new String[] {"", "local_test_only", "LOCAL_TEST_ONLY ", "true"}) {
      Map<String, String> env = env(VALID);
      env.put("SIGNALING_LOCAL_ACKNOWLEDGEMENT", wrong);
      assertRejected(env, LocalPlatformFailure.Category.ACKNOWLEDGEMENT);
    }
  }

  @Test
  void acknowledgementIsCheckedBeforeAnythingElse() {
    assertRejected(Map.of(), LocalPlatformFailure.Category.ACKNOWLEDGEMENT);
  }

  @Test
  void blankIssuerIsRejected() throws IOException {
    for (String blank : new String[] {"", "   ", "${SIGNALING_IDENTITY_ISSUER}"}) {
      Map<String, String> env = env(VALID);
      env.put("SIGNALING_IDENTITY_ISSUER", blank);
      assertRejected(env, LocalPlatformFailure.Category.IDENTITY);
    }
    Map<String, String> env = env(VALID);
    env.remove("SIGNALING_IDENTITY_ISSUER");
    assertRejected(env, LocalPlatformFailure.Category.IDENTITY);
  }

  @Test
  void missingOrRelativeConfigLocationIsRejected() throws IOException {
    Map<String, String> missing = env(VALID);
    missing.remove("SIGNALING_PLATFORM_CONFIG");
    assertRejected(missing, LocalPlatformFailure.Category.CONFIG);
    Map<String, String> relative = env(VALID);
    relative.put("SIGNALING_PLATFORM_CONFIG", "platform.json");
    assertRejected(relative, LocalPlatformFailure.Category.CONFIG);
    Map<String, String> absent = env(VALID);
    absent.put("SIGNALING_PLATFORM_CONFIG", dir.resolve("absent.json").toString());
    assertRejected(absent, LocalPlatformFailure.Category.CONFIG);
  }

  @Test
  void relativePathIsRejected() throws IOException {
    assertConfigRejected(
        VALID.replace("/run/test-only/platform/signing.key", "platform/signing.key"));
    assertConfigRejected(VALID.replace("/run/test-only/platform/source.p12", "source.p12"));
    assertConfigRejected(VALID.replace("/run/test-only/platform/db.pass", "./db.pass"));
  }

  @Test
  void wildcardBindIsRejected() throws IOException {
    assertConfigRejected(VALID.replace("192.0.2.10", "0.0.0.0"));
    assertConfigRejected(VALID.replace("192.0.2.11", "::"));
    assertConfigRejected(VALID.replace("192.0.2.12", "0:0:0:0:0:0:0:0"));
  }

  @Test
  void hostnameBindIsRejected() throws IOException {
    assertConfigRejected(VALID.replace("192.0.2.10", "localhost"));
  }

  @Test
  void hexOnlyHostnamesAreRejectedWithoutNameResolution() throws IOException {
    for (String hostname : new String[] {"face", "cafe", "dead", "beef", "abc", "ab:cd", "1:2:3"}) {
      assertConfigRejected(VALID.replace("192.0.2.10", hostname));
    }
  }

  @Test
  void legacyAndMalformedIpv4FormsAreRejected() throws IOException {
    for (String legacy :
        new String[] {
          "127.1",
          "1.2.3",
          "0x7f.0.0.1",
          "2130706433",
          "01.2.3.4",
          "192.0.2.256",
          "1.2.3.4.5",
          "1.2.3.-4",
          "1..3.4",
          "1.2.3.4.",
          ".1.2.3.4"
        }) {
      assertConfigRejected(VALID.replace("192.0.2.10", legacy));
    }
  }

  @Test
  void strictIpv4AndIpv6LiteralsAreAccepted() throws IOException {
    LocalPlatformConfig v4 =
        LocalPlatformConfig.load(env(VALID.replace("192.0.2.10", "255.0.0.1")));
    assertThat(v4.source().bind()).isEqualTo(InetAddress.getByName("255.0.0.1"));
    LocalPlatformConfig v6 =
        LocalPlatformConfig.load(env(VALID.replace("192.0.2.10", "2001:db8::1")));
    assertThat(v6.source().bind()).isEqualTo(InetAddress.getByName("2001:db8::1"));
    LocalPlatformConfig loopback =
        LocalPlatformConfig.load(env(VALID.replace("192.0.2.10", "::1")));
    assertThat(loopback.source().bind().isLoopbackAddress()).isTrue();
  }

  @Test
  void unknownKeyIsRejected() throws IOException {
    assertConfigRejected(VALID.replace("\"keyId\"", "\"extra\": 1, \"keyId\""));
    assertConfigRejected(VALID.replace("\"port\": 9445", "\"port\": 9445, \"extra\": true"));
    assertConfigRejected(
        VALID.replace("\"user\": \"test_only_user\"", "\"user\": \"test_only_user\", \"x\": 1"));
  }

  @Test
  void missingKeyIsRejected() throws IOException {
    assertConfigRejected(VALID.replace("\"keyId\": \"TEST_ONLY-key-1\",", ""));
    assertConfigRejected(VALID.replace(", \"port\": 9445", ""));
  }

  @Test
  void duplicateKeyIsRejected() throws IOException {
    assertConfigRejected(VALID.replace("\"keyId\"", "\"keyId\": \"dup\", \"keyId\""));
    assertConfigRejected(VALID.replace("\"cell-a\": 1,", "\"cell-a\": 1, \"cell-a\": 2,"));
  }

  @Test
  void trailingContentIsRejected() throws IOException {
    assertConfigRejected(VALID + "{}");
    assertConfigRejected(VALID.trim() + " garbage");
    assertConfigRejected("");
    assertConfigRejected("[]");
  }

  @Test
  void storageEpochZeroIsRejected() throws IOException {
    assertConfigRejected(VALID.replace("\"cell-a\": 1", "\"cell-a\": 0"));
    assertConfigRejected(VALID.replace("\"cell-a\": 1", "\"cell-a\": -3"));
  }

  @Test
  void nonIntegralEpochIsRejected() throws IOException {
    assertConfigRejected(VALID.replace("\"cell-a\": 1", "\"cell-a\": 1.5"));
    assertConfigRejected(VALID.replace("\"cell-a\": 1", "\"cell-a\": \"1\""));
  }

  @Test
  void invalidCellsAreRejected() throws IOException {
    assertConfigRejected(VALID.replace("\"cell-a\": 1, \"cell-b\": 7", ""));
    assertConfigRejected(VALID.replace("\"cell-a\"", "\"Cell-A\""));
    assertConfigRejected(VALID.replace("\"cell-a\"", "\"1cell\""));
    assertConfigRejected(VALID.replace("\"cell-a\"", "\"" + "a".repeat(25) + "\""));
    StringBuilder nine = new StringBuilder();
    for (int i = 0; i < 9; i++) {
      nine.append(i == 0 ? "" : ",").append("\"c").append(i).append("\": 1");
    }
    assertConfigRejected(VALID.replace("\"cell-a\": 1, \"cell-b\": 7", nine.toString()));
  }

  @Test
  void eightCellsAreAccepted() throws IOException {
    StringBuilder eight = new StringBuilder();
    for (int i = 0; i < 8; i++) {
      eight.append(i == 0 ? "" : ",").append("\"c").append(i).append("\": 1");
    }
    LocalPlatformConfig config =
        LocalPlatformConfig.load(
            env(VALID.replace("\"cell-a\": 1, \"cell-b\": 7", eight.toString())));
    assertThat(config.cells()).hasSize(8);
  }

  @Test
  void invalidPortsAreRejectedAndPortZeroIsRejectedOnLoad() throws IOException {
    assertConfigRejected(VALID.replace("9443", "0"));
    assertConfigRejected(VALID.replace("9444", "65536"));
    assertConfigRejected(VALID.replace("9445", "-1"));
    assertConfigRejected(VALID.replace("9445", "\"9445\""));
  }

  @Test
  void blankKeyIdAndDatabaseValuesAreRejected() throws IOException {
    assertConfigRejected(VALID.replace("TEST_ONLY-key-1", " "));
    assertConfigRejected(VALID.replace("test_only_user", ""));
    assertConfigRejected(VALID.replace("jdbc:postgresql://192.0.2.20:5432/test_only", "mysql://x"));
  }

  @Test
  void keyIdMustMatchTheSignersPattern() throws IOException {
    for (String bad :
        new String[] {"a.b", "a:b", "a b", "ké", "a/b", "x".repeat(65), "a\\u0001b"}) {
      assertConfigRejected(VALID.replace("TEST_ONLY-key-1", bad));
    }
    String longest = "k".repeat(64);
    assertThat(LocalPlatformConfig.load(env(VALID.replace("TEST_ONLY-key-1", longest))).keyId())
        .isEqualTo(longest);
    assertThat(LocalPlatformConfig.load(env(VALID.replace("TEST_ONLY-key-1", "A_z-9"))).keyId())
        .isEqualTo("A_z-9");
  }

  @Test
  void issuerRejectedByTheSessionKeyRulesIsAnIdentityFailure() throws IOException {
    for (String bad :
        new String[] {
          " https://issuer.test-only.invalid",
          "https://issuer.test-only.invalid ",
          "https://issuer\u0007.test-only.invalid",
          "https://" + "i".repeat(520)
        }) {
      Map<String, String> env = env(VALID);
      env.put("SIGNALING_IDENTITY_ISSUER", bad);
      assertRejected(env, LocalPlatformFailure.Category.IDENTITY);
    }
    Map<String, String> longestAllowed = env(VALID);
    longestAllowed.put("SIGNALING_IDENTITY_ISSUER", "i".repeat(512));
    assertThat(LocalPlatformConfig.load(longestAllowed).issuer()).hasSize(512);
  }

  @Test
  void jdbcUrlAllowsOnlyTheListedQueryParameters() throws IOException {
    String base = "jdbc:postgresql://192.0.2.20:5432/test_only";
    for (String good :
        new String[] {
          "?sslmode=verify-full&sslrootcert=/run/test-only/ca.crt",
          "?sslmode=disable",
          "?sslcert=/c.crt&sslkey=/c.key&connectTimeout=5&socketTimeout=10&ApplicationName=x"
        }) {
      assertThat(
              LocalPlatformConfig.load(env(VALID.replace(base, base + good))).database().jdbcUrl())
          .isEqualTo(base + good);
    }
    for (String bad :
        new String[] {
          "?password=secret",
          "?PassWord=secret",
          "?sslmode=disable&password=x",
          "?socketFactory=org.example.Evil&socketFactoryArg=x",
          "?sslfactory=org.example.Evil",
          "?authenticationPluginClassName=org.example.Evil",
          "?sslpasswordcallback=org.example.Evil",
          "?loggerLevel=TRACE",
          "?user=other",
          "?sslmode=disable&sslmode=require",
          "?sslmode",
          "?sslmode=",
          "?&sslmode=disable",
          "?"
        }) {
      assertConfigRejected(VALID.replace(base, base + bad));
    }
  }

  @Test
  void validConfigLoads() throws IOException {
    LocalPlatformConfig config = LocalPlatformConfig.load(env(VALID));

    assertThat(config.issuer()).isEqualTo("https://issuer.test-only.invalid");
    assertThat(config.cells()).containsOnly(Map.entry("cell-a", 1L), Map.entry("cell-b", 7L));
    assertThat(config.keyId()).isEqualTo("TEST_ONLY-key-1");
    assertThat(config.signingKeyFile()).isEqualTo(Path.of("/run/test-only/platform/signing.key"));

    LocalPlatformConfig.Listener source = config.source();
    assertThat(source.bind()).isEqualTo(InetAddress.getByName("192.0.2.10"));
    assertThat(source.port()).isEqualTo(9443);
    assertThat(source.keystore()).isEqualTo(Path.of("/run/test-only/platform/source.p12"));
    assertThat(source.keystorePasswordFile())
        .isEqualTo(Path.of("/run/test-only/platform/source.pass"));
    assertThat(source.clientTrust()).isEqualTo(Path.of("/run/test-only/platform/source-trust.p12"));

    LocalPlatformConfig.Listener admin = config.admin();
    assertThat(admin.bind()).isEqualTo(InetAddress.getByName("192.0.2.11"));
    assertThat(admin.port()).isEqualTo(9444);
    assertThat(admin.clientTrust()).isEqualTo(Path.of("/run/test-only/platform/admin-trust.p12"));

    assertThat(config.health().bind()).isEqualTo(InetAddress.getByName("192.0.2.12"));
    assertThat(config.health().port()).isEqualTo(9445);

    assertThat(config.database().jdbcUrl())
        .isEqualTo("jdbc:postgresql://192.0.2.20:5432/test_only");
    assertThat(config.database().user()).isEqualTo("test_only_user");
    assertThat(config.database().passwordFile())
        .isEqualTo(Path.of("/run/test-only/platform/db.pass"));

    assertThat(config.clockValidity()).isEqualTo(Duration.ofSeconds(2));
  }

  @Test
  void cellsMapIsUnmodifiable() throws IOException {
    LocalPlatformConfig config = LocalPlatformConfig.load(env(VALID));
    assertThatThrownBy(() -> config.cells().put("cell-c", 1L))
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  void toStringRevealsNoPathOrSecretText() throws IOException {
    LocalPlatformConfig config = LocalPlatformConfig.load(env(VALID));
    for (String text :
        new String[] {
          config.toString(),
          config.source().toString(),
          config.admin().toString(),
          config.health().toString(),
          config.database().toString()
        }) {
      assertThat(text)
          .doesNotContain("/run/")
          .doesNotContain("pass")
          .doesNotContain("signing")
          .doesNotContain("keystore")
          .doesNotContain("test_only")
          .doesNotContain("192.0.2")
          .doesNotContain("issuer");
    }
  }

  @Test
  void withPortsPermitsZeroAndLeavesTheOriginalUntouched() throws IOException {
    LocalPlatformConfig config = LocalPlatformConfig.load(env(VALID));

    LocalPlatformConfig copy = config.withPorts(0, 0, 0);

    assertThat(copy.source().port()).isZero();
    assertThat(copy.admin().port()).isZero();
    assertThat(copy.health().port()).isZero();
    assertThat(copy.source().bind()).isEqualTo(config.source().bind());
    assertThat(copy.source().keystore()).isEqualTo(config.source().keystore());
    assertThat(copy.issuer()).isEqualTo(config.issuer());
    assertThat(copy.cells()).isEqualTo(config.cells());
    assertThat(copy.database().jdbcUrl()).isEqualTo(config.database().jdbcUrl());
    assertThat(config.source().port()).isEqualTo(9443);
  }

  @Test
  void applicationPrintsOnlyTheCategoryAndExitsNonZero() throws IOException {
    Map<String, String> env = env(VALID);
    env.remove("SIGNALING_LOCAL_ACKNOWLEDGEMENT");
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();

    int code = LocalPlatformApplication.run(env, new PrintStream(bytes, true));

    assertThat(code).isEqualTo(1);
    assertThat(bytes.toString(StandardCharsets.UTF_8))
        .isEqualTo("LOCAL_PLATFORM_REJECTED ACKNOWLEDGEMENT" + System.lineSeparator());
  }

  @Test
  void applicationHidesUnexpectedFailureDetail() throws IOException {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();

    int code =
        LocalPlatformApplication.run(
            env(VALID),
            new PrintStream(bytes, true),
            config -> {
              throw new IllegalStateException("/run/test-only/platform/db.pass secret detail");
            },
            running -> {
              throw new AssertionError("must not wait for a platform that never started");
            });

    assertThat(code).isEqualTo(1);
    assertThat(bytes.toString(StandardCharsets.UTF_8))
        .isEqualTo("LOCAL_PLATFORM_REJECTED CONFIG" + System.lineSeparator());
  }

  @Test
  void applicationReportsTheStartFailureCategory() throws IOException {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();

    // The configured signing key does not exist, so the real start() refuses with KEY.
    int code = LocalPlatformApplication.run(env(VALID), new PrintStream(bytes, true));

    assertThat(code).isEqualTo(1);
    assertThat(bytes.toString(StandardCharsets.UTF_8))
        .isEqualTo("LOCAL_PLATFORM_REJECTED KEY" + System.lineSeparator());
  }
}
