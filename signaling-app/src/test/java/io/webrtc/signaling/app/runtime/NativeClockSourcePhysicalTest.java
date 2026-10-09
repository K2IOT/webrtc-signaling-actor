package io.webrtc.signaling.app.runtime;

import static org.assertj.core.api.Assertions.*;

import io.webrtc.signaling.auth.ClockSafetyMonitor;
import io.webrtc.signaling.rpc.RpcOperation;
import java.io.*;
import java.net.*;
import java.security.*;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import javax.net.ssl.*;
import org.junit.jupiter.api.Test;

/** Injects the OS/TLS close-error signal; an uncertain close is not a physical cleanup receipt. */
class NativeClockSourcePhysicalTest {
  @Test
  void socketFactoryThrowCannotProveThatNoPhysicalEffectsStarted() throws Exception {
    var keys = KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
    var pod = UUID.randomUUID();
    var boot = UUID.randomUUID();
    var monitor =
        new ClockSafetyMonitor(
            "c001",
            1,
            pod,
            boot,
            Map.of("TEST_ONLY", keys.getPublic()),
            Clock.systemUTC(),
            System::nanoTime);
    try (var source =
        new NativeClockSource(
            URI.create("https://localhost/v1/clock-bound"),
            new CloseErrorContext(true),
            monitor,
            pod,
            boot)) {
      var operation = source.job(Duration.ofSeconds(1)).run().apply(Duration.ofSeconds(1));
      assertThat(operation.logical().toCompletableFuture()).isCompletedExceptionally();
      assertThat(operation.physicalCompletion().toCompletableFuture()).isNotDone();
      assertThat(source.drain().toCompletableFuture()).isNotDone();
    }
  }

  @Test
  void socketCloseErrorRetainsTheAdmittedSourceUntilCleanupCanBeProven() throws Exception {
    var keys = KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
    var pod = UUID.randomUUID();
    var boot = UUID.randomUUID();
    var monitor =
        new ClockSafetyMonitor(
            "c001",
            1,
            pod,
            boot,
            Map.of("TEST_ONLY", keys.getPublic()),
            Clock.systemUTC(),
            System::nanoTime);
    try (var source =
        new NativeClockSource(
            URI.create("https://localhost/v1/clock-bound"),
            new CloseErrorContext(),
            monitor,
            pod,
            boot)) {
      var operation = source.job(Duration.ofSeconds(1)).run().apply(Duration.ofSeconds(1));
      assertThat(operation.logical().toCompletableFuture()).isCompletedExceptionally();
      assertThat(monitor.valid()).isFalse();
      assertThat(operation.physicalCompletion().toCompletableFuture()).isNotDone();
      assertThat(source.drain().toCompletableFuture()).isNotDone();
    }
  }

  @Test
  void reentrantUnknownPollKeepsTheWholeSourceDrainPending() throws Exception {
    var result = reentrantPoll();
    assertThat(result.drain().toCompletableFuture()).isNotDone();
  }

  @Test
  void jobReceiptStillBelongsToItsOriginalPollAfterReentrantAdmission() throws Exception {
    var result = reentrantPoll();
    assertThat(result.original().physicalCompletion().toCompletableFuture()).isDone();
  }

  private record ReentrantResult(RpcOperation<?> original, CompletionStage<Void> drain) {}

  private static ReentrantResult reentrantPoll() throws Exception {
    var keys = KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
    var pod = UUID.randomUUID();
    var boot = UUID.randomUUID();
    var monitor =
        new ClockSafetyMonitor(
            "c001",
            1,
            pod,
            boot,
            Map.of("TEST_ONLY", keys.getPublic()),
            Clock.systemUTC(),
            System::nanoTime);
    var entered = new CountDownLatch(1);
    var release = new CountDownLatch(1);
    var source =
        new NativeClockSource(
            URI.create("https://localhost/v1/clock-bound"),
            new CloseErrorContext(entered, release),
            monitor,
            pod,
            boot);
    var original = new CompletableFuture<RpcOperation<?>>();
    Thread.ofVirtual()
        .start(
            () -> {
              try {
                original.complete(
                    source.job(Duration.ofSeconds(1)).run().apply(Duration.ofSeconds(1)));
              } catch (Throwable e) {
                original.completeExceptionally(e);
              }
            });
    try {
      assertThat(entered.await(2, TimeUnit.SECONDS)).isTrue();
      var drain = new AtomicReference<CompletionStage<Void>>();
      source
          .settleAdmitted()
          .whenComplete(
              (v, e) -> {
                assertThat(source.poll(Duration.ofSeconds(1))).isFalse();
                drain.set(source.drain());
              });
      release.countDown();
      var operation = original.get(2, TimeUnit.SECONDS);
      assertThat(drain.get()).isNotNull();
      return new ReentrantResult(operation, drain.get());
    } finally {
      release.countDown();
      source.close();
    }
  }

  private static final class CloseErrorContext extends SSLContext {
    CloseErrorContext() {
      this(false);
    }

    CloseErrorContext(CountDownLatch entered, CountDownLatch release) {
      super(
          new CloseErrorSpi(false, entered, release),
          new Provider("TEST_ONLY_REENTRANT", "1", "Failure injection") {},
          "TLSv1.3");
    }

    CloseErrorContext(boolean factoryThrow) {
      super(
          new CloseErrorSpi(factoryThrow),
          new Provider("TEST_ONLY_CLOSE_ERROR", "1", "Failure injection") {},
          "TLSv1.3");
    }
  }

  private static final class CloseErrorSpi extends SSLContextSpi {
    private final boolean factoryThrow;
    private final CountDownLatch entered, release;
    private final AtomicInteger sockets = new AtomicInteger();

    CloseErrorSpi(boolean factoryThrow) {
      this(factoryThrow, null, null);
    }

    CloseErrorSpi(boolean factoryThrow, CountDownLatch entered, CountDownLatch release) {
      this.factoryThrow = factoryThrow;
      this.entered = entered;
      this.release = release;
    }

    protected void engineInit(KeyManager[] k, TrustManager[] t, SecureRandom r) {}

    protected SSLSocketFactory engineGetSocketFactory() {
      return new SSLSocketFactory() {
        public String[] getDefaultCipherSuites() {
          return new String[0];
        }

        public String[] getSupportedCipherSuites() {
          return new String[0];
        }

        public java.net.Socket createSocket() throws IOException {
          if (factoryThrow) throw new IOException("TEST_ONLY factory effects unknown");
          return entered != null && sockets.getAndIncrement() == 0
              ? new CloseErrorSocket(entered, release)
              : new CloseErrorSocket();
        }

        public java.net.Socket createSocket(java.net.Socket s, String h, int p, boolean a) {
          throw new UnsupportedOperationException();
        }

        public java.net.Socket createSocket(String h, int p) {
          throw new UnsupportedOperationException();
        }

        public java.net.Socket createSocket(String h, int p, InetAddress l, int lp) {
          throw new UnsupportedOperationException();
        }

        public java.net.Socket createSocket(InetAddress h, int p) {
          throw new UnsupportedOperationException();
        }

        public java.net.Socket createSocket(InetAddress h, int p, InetAddress l, int lp) {
          throw new UnsupportedOperationException();
        }
      };
    }

    protected SSLServerSocketFactory engineGetServerSocketFactory() {
      throw new UnsupportedOperationException();
    }

    protected SSLEngine engineCreateSSLEngine() {
      throw new UnsupportedOperationException();
    }

    protected SSLEngine engineCreateSSLEngine(String h, int p) {
      throw new UnsupportedOperationException();
    }

    protected SSLSessionContext engineGetServerSessionContext() {
      throw new UnsupportedOperationException();
    }

    protected SSLSessionContext engineGetClientSessionContext() {
      throw new UnsupportedOperationException();
    }
  }

  private static final class CloseErrorSocket extends SSLSocket {
    private final CountDownLatch entered, release;

    CloseErrorSocket() {
      this(null, null);
    }

    CloseErrorSocket(CountDownLatch entered, CountDownLatch release) {
      this.entered = entered;
      this.release = release;
    }

    public void connect(SocketAddress address, int timeout) throws IOException {
      if (entered != null) {
        entered.countDown();
        try {
          if (!release.await(2, TimeUnit.SECONDS)) throw new IOException("TEST_ONLY fixture wait");
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new IOException(e);
        }
      }
      throw new IOException("TEST_ONLY unknown connect");
    }

    public synchronized void close() throws IOException {
      if (entered == null) throw new IOException("TEST_ONLY unknown physical close");
    }

    public String[] getSupportedCipherSuites() {
      return new String[0];
    }

    public String[] getEnabledCipherSuites() {
      return new String[0];
    }

    public void setEnabledCipherSuites(String[] value) {}

    public String[] getSupportedProtocols() {
      return new String[] {"TLSv1.3"};
    }

    public String[] getEnabledProtocols() {
      return getSupportedProtocols();
    }

    public void setEnabledProtocols(String[] value) {}

    public SSLSession getSession() {
      throw new UnsupportedOperationException();
    }

    public void addHandshakeCompletedListener(HandshakeCompletedListener l) {}

    public void removeHandshakeCompletedListener(HandshakeCompletedListener l) {}

    public void startHandshake() {
      throw new UnsupportedOperationException();
    }

    public void setUseClientMode(boolean value) {}

    public boolean getUseClientMode() {
      return true;
    }

    public void setNeedClientAuth(boolean value) {}

    public boolean getNeedClientAuth() {
      return false;
    }

    public void setWantClientAuth(boolean value) {}

    public boolean getWantClientAuth() {
      return false;
    }

    public void setEnableSessionCreation(boolean value) {}

    public boolean getEnableSessionCreation() {
      return true;
    }
  }
}
