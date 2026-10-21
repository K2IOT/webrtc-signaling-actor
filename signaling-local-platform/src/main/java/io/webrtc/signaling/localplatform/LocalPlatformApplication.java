package io.webrtc.signaling.localplatform;

import java.io.PrintStream;
import java.time.Instant;
import java.util.Map;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

/** Entry point of the standalone LOCAL_TEST_ONLY platform process (plain Java, no Spring). */
public final class LocalPlatformApplication {

  private LocalPlatformApplication() {}

  /** A started platform; the port accessors and shutdown arrive with the server implementation. */
  public static final class Running implements AutoCloseable {

    Running() {}

    public int sourcePort() {
      throw new UnsupportedOperationException();
    }

    public int adminPort() {
      throw new UnsupportedOperationException();
    }

    public int healthPort() {
      throw new UnsupportedOperationException();
    }

    @Override
    public void close() {}
  }

  public static void main(String[] args) {
    System.exit(run(System.getenv(), System.err));
  }

  /** Runs the process; returns the exit code and prints only {@code LOCAL_PLATFORM_REJECTED}. */
  static int run(Map<String, String> env, PrintStream err) {
    LocalPlatformFailure.Category category;
    try {
      start(LocalPlatformConfig.load(env));
      return 0;
    } catch (LocalPlatformFailure failure) {
      category = failure.category();
    } catch (Throwable unexpected) {
      category = LocalPlatformFailure.Category.CONFIG;
    }
    err.println("LOCAL_PLATFORM_REJECTED " + category.name());
    return 1;
  }

  public static Running start(LocalPlatformConfig config) {
    return start(config, System::nanoTime, Instant::now);
  }

  public static Running start(
      LocalPlatformConfig config, LongSupplier mono, Supplier<Instant> wall) {
    throw new UnsupportedOperationException();
  }
}
