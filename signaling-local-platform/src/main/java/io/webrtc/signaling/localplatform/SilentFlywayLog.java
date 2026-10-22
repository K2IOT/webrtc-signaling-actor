package io.webrtc.signaling.localplatform;

import org.flywaydb.core.api.logging.Log;
import org.flywaydb.core.api.logging.LogCreator;

/**
 * Flyway log sink that drops everything. Flyway would otherwise log through whatever SLF4J backend
 * is on the classpath, including the JDBC URL with every driver property and the migration
 * progress, while the process promises to print nothing but a bare failure category. Selected by
 * class name through {@code Flyway.configure().loggers(...)}, hence the public no-argument
 * constructor.
 *
 * <p>Status: LOCAL_TEST_ONLY, NOT_QUALIFIED.
 */
public final class SilentFlywayLog implements LogCreator {

  private static final Log SILENT =
      new Log() {
        @Override
        public boolean isDebugEnabled() {
          return false;
        }

        @Override
        public void debug(String message) {}

        @Override
        public void info(String message) {}

        @Override
        public void warn(String message) {}

        @Override
        public void error(String message) {}

        @Override
        public void error(String message, Exception e) {}

        @Override
        public void notice(String message) {}
      };

  /** Public no-argument constructor required by Flyway's reflective instantiation. */
  public SilentFlywayLog() {}

  @Override
  public Log createLogger(Class<?> clazz) {
    return SILENT;
  }
}
