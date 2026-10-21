package io.webrtc.signaling.localplatform;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class LocalPlatformApplicationTest {

  private static final String VALID =
      """
      {
        "cells": {"cell-a": 1},
        "keyId": "local-source",
        "signingKey": "/run/test-only/platform/signing.key",
        "source": {
          "bind": "192.0.2.10", "port": 9443,
          "keystore": "/run/test-only/platform/source.p12",
          "keystorePasswordFile": "/run/test-only/platform/source.pass",
          "clientTrust": "/run/test-only/platform/workload-ca.pem"
        },
        "admin": {
          "bind": "192.0.2.11", "port": 9444,
          "keystore": "/run/test-only/platform/admin.p12",
          "keystorePasswordFile": "/run/test-only/platform/admin.pass",
          "clientTrust": "/run/test-only/platform/admin-ca.pem"
        },
        "health": {"bind": "192.0.2.12", "port": 8559},
        "database": {
          "jdbcUrl": "jdbc:postgresql://192.0.2.20:5432/test_only",
          "user": "test_only_user",
          "passwordFile": "/run/test-only/platform/db.pass"
        }
      }
      """;

  @TempDir Path dir;

  @Test
  void awaitShutdownBlocksUntilTheHookRunsAndThenClosesThePlatform() throws Exception {
    AtomicInteger closes = new AtomicInteger();
    AtomicReference<Thread> hook = new AtomicReference<>();
    CountDownLatch returned = new CountDownLatch(1);
    Thread main =
        new Thread(
            () -> {
              try {
                LocalPlatformApplication.awaitShutdown(closes::incrementAndGet, hook::set);
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
              }
              returned.countDown();
            });

    main.start();

    assertThat(returned.await(300, TimeUnit.MILLISECONDS)).isFalse();
    assertThat(main.isAlive()).isTrue();
    assertThat(closes).hasValue(0);
    assertThat(hook.get()).isNotNull();

    hook.get().start();

    assertThat(returned.await(5, TimeUnit.SECONDS)).isTrue();
    assertThat(closes).hasValue(1);
  }

  @Test
  void runHandsTheStartedPlatformToTheAwaiter() throws Exception {
    Path config = Files.writeString(dir.resolve("platform.json"), VALID);
    AutoCloseable platform = () -> {};
    AtomicReference<AutoCloseable> awaited = new AtomicReference<>();
    ByteArrayOutputStream err = new ByteArrayOutputStream();

    int code =
        LocalPlatformApplication.run(
            Map.of(
                "SIGNALING_PLATFORM_CONFIG", config.toString(),
                "SIGNALING_LOCAL_ACKNOWLEDGEMENT", "LOCAL_TEST_ONLY",
                "SIGNALING_IDENTITY_ISSUER", "https://issuer.test-only.invalid"),
            new PrintStream(err, true, StandardCharsets.UTF_8),
            loaded -> platform,
            awaited::set);

    assertThat(code).isZero();
    assertThat(awaited.get()).isSameAs(platform);
    assertThat(err.size()).isZero();
  }

  @Test
  void runDoesNotStartOrAwaitWithoutAcknowledgement() {
    AtomicReference<AutoCloseable> awaited = new AtomicReference<>();
    ByteArrayOutputStream err = new ByteArrayOutputStream();

    int code =
        LocalPlatformApplication.run(
            Map.of(),
            new PrintStream(err, true, StandardCharsets.UTF_8),
            config -> {
              throw new AssertionError("must not start");
            },
            awaited::set);

    assertThat(code).isEqualTo(1);
    assertThat(awaited.get()).isNull();
    assertThat(err.toString(StandardCharsets.UTF_8))
        .isEqualTo("LOCAL_PLATFORM_REJECTED ACKNOWLEDGEMENT" + System.lineSeparator());
  }

  @Test
  void interruptedWaitClosesThePlatform() throws Exception {
    AtomicInteger closes = new AtomicInteger();
    CountDownLatch registered = new CountDownLatch(1);
    AtomicReference<Throwable> outcome = new AtomicReference<>();
    Thread main =
        new Thread(
            () -> {
              try {
                LocalPlatformApplication.awaitShutdown(
                    closes::incrementAndGet, thread -> registered.countDown());
              } catch (Throwable t) {
                outcome.set(t);
              }
            });
    main.start();
    assertThat(registered.await(5, TimeUnit.SECONDS)).isTrue();

    main.interrupt();
    main.join(5_000);

    assertThat(main.isAlive()).isFalse();
    assertThat(outcome.get()).isInstanceOf(InterruptedException.class);
    assertThat(closes).hasValue(1);
  }
}
