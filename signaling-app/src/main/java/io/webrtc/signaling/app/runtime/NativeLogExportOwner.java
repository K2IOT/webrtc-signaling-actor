package io.webrtc.signaling.app.runtime;

import ch.qos.logback.classic.LoggerContext;
import io.webrtc.signaling.observability.BoundedAsyncLogAppender;
import org.slf4j.LoggerFactory;

/**
 * Bounded shutdown export follows native work; unresolved stdout writes are never called settled.
 */
public final class NativeLogExportOwner
        implements AutoCloseable, org.springframework.context.SmartLifecycle {
    private volatile boolean running;

    public void start() {
        running = true;
    }

    public boolean isRunning() {
        return running;
    }

    public int getPhase() {
        return Integer.MIN_VALUE;
    }

    public void stop() {
        close();
    }

    public void stop(Runnable stopped) {
        Thread.startVirtualThread(
                () -> {
                    close();
                    stopped.run();
                });
    }

    @Override
    public void close() {
        running = false;
        if (LoggerFactory.getILoggerFactory() instanceof LoggerContext context) {
            var appender = context.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME).getAppender("SAFE_ASYNC");
            if (appender instanceof BoundedAsyncLogAppender async) async.stop();
        }
    }
}
