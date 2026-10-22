package io.webrtc.signaling.localplatform;

import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import com.sun.net.httpserver.HttpsServer;
import java.io.IOException;
import java.io.PrintStream;
import java.net.InetSocketAddress;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Deque;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import javax.net.ssl.SSLContext;
import javax.sql.DataSource;
import org.postgresql.ds.PGSimpleDataSource;

/**
 * Entry point of the standalone LOCAL_TEST_ONLY platform process (plain Java, no Spring).
 *
 * <p>{@link #start} loads the signing key ({@code KEY}), both server TLS contexts ({@code TLS}),
 * opens the database with the password read from its file and applies the source migration ({@code
 * DATABASE}), starts the host clock observer, and only then opens three listeners: the workload
 * source (mTLS), the admin role (mTLS, its own CA) and plain-HTTP readiness. A listener that cannot
 * bind is {@code CONFIG}. Every failure is a bare category; nothing opened so far is left running.
 *
 * <p>Status: LOCAL_TEST_ONLY, NOT_QUALIFIED.
 */
public final class LocalPlatformApplication {

  /** Production sampling interval of the host clock observer. */
  static final Duration OBSERVER_INTERVAL = Duration.ofMillis(100);

  private static final int SOURCE_THREADS = 4;
  private static final int ADMIN_THREADS = 2;
  private static final int HEALTH_THREADS = 1;
  private static final int BACKLOG = 64;
  private static final int DB_CONNECT_TIMEOUT_SECONDS = 5;
  private static final int DB_SOCKET_TIMEOUT_SECONDS = 10;
  private static final long EXECUTOR_STOP_SECONDS = 2;

  private LocalPlatformApplication() {}

  /** Starts a platform from a loaded configuration (seam for {@link #run}). */
  @FunctionalInterface
  interface Starter {
    AutoCloseable start(LocalPlatformConfig config);
  }

  /** Keeps a started platform alive until the process is asked to stop (seam for {@link #run}). */
  @FunctionalInterface
  interface Awaiter {
    void await(AutoCloseable running) throws InterruptedException;
  }

  /** A started platform. {@link #close()} stops it; it is idempotent. */
  public static final class Running implements AutoCloseable {

    private final HttpServer source;
    private final HttpServer admin;
    private final HttpServer health;
    private final Deque<AutoCloseable> resources;
    private final AtomicBoolean closed = new AtomicBoolean();

    private Running(
        HttpServer source, HttpServer admin, HttpServer health, Deque<AutoCloseable> resources) {
      this.source = source;
      this.admin = admin;
      this.health = health;
      this.resources = resources;
    }

    /** Actual source port (resolved when the configuration asked for an ephemeral one). */
    public int sourcePort() {
      return source.getAddress().getPort();
    }

    public int adminPort() {
      return admin.getAddress().getPort();
    }

    public int healthPort() {
      return health.getAddress().getPort();
    }

    /**
     * Stops the listeners (delay 0) and their executors, then the clock observer. The data source
     * holds no pooled connection, so nothing remains open. Calling it again does nothing.
     */
    @Override
    public void close() {
      if (closed.compareAndSet(false, true)) {
        closeAll(resources);
      }
    }

    @Override
    public String toString() {
      return "Running[LOCAL_TEST_ONLY]";
    }
  }

  public static void main(String[] args) {
    int code = run(System.getenv(), System.err);
    if (code != 0) {
      System.exit(code);
    }
  }

  /**
   * Runs the process: starts the platform and blocks until a shutdown hook (SIGTERM, SIGINT) stops
   * it. Returns the exit code; on failure prints only {@code LOCAL_PLATFORM_REJECTED <category>}.
   */
  static int run(Map<String, String> env, PrintStream err) {
    return run(
        env,
        err,
        LocalPlatformApplication::start,
        running -> awaitShutdown(running, Runtime.getRuntime()::addShutdownHook));
  }

  /** {@link #run(Map, PrintStream)} with the start and the wait injected (tests). */
  static int run(Map<String, String> env, PrintStream err, Starter starter, Awaiter awaiter) {
    AutoCloseable running;
    try {
      running = starter.start(LocalPlatformConfig.load(env));
    } catch (LocalPlatformFailure failure) {
      return reject(err, failure.category());
    } catch (Throwable unexpected) {
      return reject(err, LocalPlatformFailure.Category.CONFIG);
    }
    try {
      awaiter.await(running);
      return 0;
    } catch (InterruptedException interrupted) {
      Thread.currentThread().interrupt();
      return 0;
    } catch (IllegalStateException shuttingDown) {
      // Runtime.addShutdownHook refuses once the JVM is already shutting down (a signal during
      // startup): the platform is stopped here and that is a normal stop, not a failure.
      closeQuietly(running);
      return 0;
    } catch (RuntimeException unexpected) {
      closeQuietly(running);
      return reject(err, LocalPlatformFailure.Category.CONFIG);
    }
  }

  /**
   * Registers a hook that closes {@code running} and blocks the calling thread until that hook has
   * run. If the calling thread is interrupted first, {@code running} is closed here and the
   * interruption is rethrown.
   */
  static void awaitShutdown(AutoCloseable running, Consumer<Thread> registerHook)
      throws InterruptedException {
    CountDownLatch stopped = new CountDownLatch(1);
    registerHook.accept(
        new Thread(
            () -> {
              try {
                closeQuietly(running);
              } finally {
                stopped.countDown();
              }
            },
            "local-platform-shutdown"));
    try {
      stopped.await();
    } catch (InterruptedException interrupted) {
      closeQuietly(running);
      throw interrupted;
    }
  }

  public static Running start(LocalPlatformConfig config) {
    return start(config, System::nanoTime, Instant::now);
  }

  /**
   * Starts the platform with injected clocks. The monotonic and wall suppliers feed the host clock
   * observer, and the wall supplier is also the clock of the reports and pages, so a test can step
   * the wall clock of a running platform. Production uses {@link #start(LocalPlatformConfig)}.
   *
   * <p>TEST seam, not a production hook: it exists so that an integration test in another module
   * can step the clocks of an in-process platform. Nothing in the process or its configuration
   * reaches it; the real entry point always passes {@code System::nanoTime} and {@code
   * Instant::now}.
   */
  public static Running start(
      LocalPlatformConfig config, LongSupplier mono, Supplier<Instant> wall) {
    Objects.requireNonNull(config, "config");
    Objects.requireNonNull(mono, "mono");
    Objects.requireNonNull(wall, "wall");
    SourceSigner signer = SourceSigner.load(config.signingKeyFile(), config.keyId());
    SSLContext sourceTls = TlsContexts.server(config.source());
    SSLContext adminTls = TlsContexts.server(config.admin());
    DataSource dataSource = dataSource(config.database());
    try {
      JdbcRevocationLog.migrate(dataSource);
    } catch (RuntimeException unavailable) {
      throw new LocalPlatformFailure(LocalPlatformFailure.Category.DATABASE);
    }

    Deque<AutoCloseable> opened = new ArrayDeque<>();
    try {
      Clock clock = new SupplierClock(wall);
      HostClockObserver observer = new HostClockObserver(mono, wall, OBSERVER_INTERVAL);
      opened.push(observer);
      observer.start();
      RevocationLog log = new JdbcRevocationLog(dataSource);
      SourceHandler sourceHandler =
          new SourceHandler(
              config.cells().keySet(),
              new BootRegistry(),
              new ClockReportService(signer, observer, config.cells(), clock),
              new RevocationPageService(signer, log, config.issuer(), clock));
      HttpServer source =
          listen(
              https(config.source(), sourceTls), sourceHandler, SOURCE_THREADS, "source", opened);
      HttpServer admin =
          listen(
              https(config.admin(), adminTls),
              new AdminHandler(log),
              ADMIN_THREADS,
              "admin",
              opened);
      HttpServer health =
          listen(
              HttpServer.create(
                  new InetSocketAddress(config.health().bind(), config.health().port()), BACKLOG),
              new ReadyHandler(dataSource, observer),
              HEALTH_THREADS,
              "health",
              opened);
      return new Running(source, admin, health, opened);
    } catch (IOException | RuntimeException failure) {
      closeAllQuietly(opened);
      throw new LocalPlatformFailure(LocalPlatformFailure.Category.CONFIG);
    }
  }

  private static HttpsServer https(LocalPlatformConfig.Listener listener, SSLContext tls)
      throws IOException {
    HttpsServer server =
        HttpsServer.create(new InetSocketAddress(listener.bind(), listener.port()), BACKLOG);
    server.setHttpsConfigurator(TlsContexts.configurator(tls));
    return server;
  }

  /**
   * Binds {@code handler} at the root (it routes exact paths itself), gives the server a bounded
   * fixed pool and starts it. The server and its pool are pushed so that closing pops the server
   * first, then the pool.
   */
  private static HttpServer listen(
      HttpServer server,
      HttpHandler handler,
      int threads,
      String name,
      Deque<AutoCloseable> opened) {
    ExecutorService executor = Executors.newFixedThreadPool(threads, threads(name));
    opened.push(() -> stopExecutor(executor));
    opened.push(() -> server.stop(0));
    server.createContext("/", handler);
    server.setExecutor(executor);
    server.start();
    return server;
  }

  private static java.util.concurrent.ThreadFactory threads(String name) {
    AtomicInteger count = new AtomicInteger();
    return task -> {
      Thread thread = new Thread(task, "local-platform-" + name + "-" + count.incrementAndGet());
      thread.setDaemon(true);
      return thread;
    };
  }

  private static void stopExecutor(ExecutorService executor) throws InterruptedException {
    executor.shutdownNow();
    if (!executor.awaitTermination(EXECUTOR_STOP_SECONDS, TimeUnit.SECONDS)) {
      throw new IllegalStateException("listener threads did not stop");
    }
  }

  /**
   * A non-pooling data source; the password is read from its file and only handed to the driver.
   */
  private static DataSource dataSource(LocalPlatformConfig.Database database) {
    char[] password = null;
    try {
      password = SecretFiles.readPassword(database.passwordFile());
      PGSimpleDataSource dataSource = new PGSimpleDataSource();
      dataSource.setUrl(database.jdbcUrl());
      dataSource.setUser(database.user());
      dataSource.setPassword(new String(password));
      dataSource.setConnectTimeout(DB_CONNECT_TIMEOUT_SECONDS);
      dataSource.setLoginTimeout(DB_CONNECT_TIMEOUT_SECONDS);
      dataSource.setSocketTimeout(DB_SOCKET_TIMEOUT_SECONDS);
      dataSource.setApplicationName("signaling-local-platform");
      return dataSource;
    } catch (IOException | RuntimeException invalid) {
      throw new LocalPlatformFailure(LocalPlatformFailure.Category.DATABASE);
    } finally {
      if (password != null) {
        Arrays.fill(password, '\0');
      }
    }
  }

  /** Closes in stack order; every resource is attempted even if an earlier one fails. */
  private static void closeAll(Deque<AutoCloseable> resources) {
    boolean clean = true;
    for (AutoCloseable resource = resources.poll(); resource != null; resource = resources.poll()) {
      clean &= closeQuietly(resource);
    }
    if (!clean) {
      throw new IllegalStateException("local platform did not stop cleanly");
    }
  }

  /** {@link #closeAll} for a failure path: a close problem must not replace the original cause. */
  static void closeAllQuietly(Deque<AutoCloseable> resources) {
    try {
      closeAll(resources);
    } catch (RuntimeException ignored) {
      // Every resource was attempted; the failure being reported is the startup one.
    }
  }

  private static boolean closeQuietly(AutoCloseable resource) {
    try {
      resource.close();
      return true;
    } catch (InterruptedException interrupted) {
      Thread.currentThread().interrupt();
      return false;
    } catch (Exception failure) {
      return false;
    }
  }

  private static int reject(PrintStream err, LocalPlatformFailure.Category category) {
    err.println("LOCAL_PLATFORM_REJECTED " + category.name());
    return 1;
  }

  /** A UTC {@link Clock} reading the injected wall supplier. */
  private static final class SupplierClock extends Clock {
    private final Supplier<Instant> wall;

    private SupplierClock(Supplier<Instant> wall) {
      this.wall = wall;
    }

    @Override
    public ZoneId getZone() {
      return ZoneOffset.UTC;
    }

    @Override
    public Clock withZone(ZoneId zone) {
      if (ZoneOffset.UTC.equals(zone)) {
        return this;
      }
      throw new UnsupportedOperationException();
    }

    @Override
    public Instant instant() {
      return wall.get();
    }
  }
}
