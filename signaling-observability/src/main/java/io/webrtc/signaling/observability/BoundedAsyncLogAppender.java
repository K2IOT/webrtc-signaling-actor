package io.webrtc.signaling.observability;

import ch.qos.logback.classic.AsyncAppender;
import ch.qos.logback.classic.spi.ILoggingEvent;
import java.util.concurrent.atomic.LongAdder;

/** One bounded export owner; saturated stdout never makes producers wait for a queue credit. */
public final class BoundedAsyncLogAppender extends AsyncAppender {
  private final LongAdder dropped = new LongAdder();
  private final java.util.concurrent.atomic.AtomicInteger unresolved =
      new java.util.concurrent.atomic.AtomicInteger();
  private volatile java.util.concurrent.Semaphore credits;
  private volatile boolean closing;

  @Override
  public void addAppender(ch.qos.logback.core.Appender<ILoggingEvent> delegate) {
    // AsyncAppender accepts one sink. Only its worker calls this wrapper, including slow stdout.
    var sink =
        new ch.qos.logback.core.AppenderBase<ILoggingEvent>() {
          protected void append(ILoggingEvent event) {
            try {
              delegate.doAppend(event);
            } finally {
              credits.release();
            }
          }

          @Override
          public void stop() {
            super.stop();
            delegate.stop();
          }
        };
    sink.setContext(getContext());
    sink.setName("SAFE_EXPORT");
    sink.start();
    super.addAppender(sink);
  }

  @Override
  public void start() {
    if (isStarted()) return;
    if (credits != null)
      throw new IllegalStateException("Original log export owner cannot restart");
    credits = new java.util.concurrent.Semaphore(getQueueSize());
    setNeverBlock(true);
    setDiscardingThreshold(0);
    setIncludeCallerData(false);
    setMaxFlushTime(2000);
    super.start();
  }

  @Override
  protected void append(ILoggingEvent event) {
    // Credits include queued, currently exporting and producer-reserved originals. A reservation
    // proves the underlying nonblocking offer has space, without a producer monitor or a wait.
    if (closing || !credits.tryAcquire()) {
      dropped.increment();
      return;
    }
    try {
      // Copy only bounded approved metadata before retention. Free text, arguments, tokens and
      // throwable stacks are never retained by the asynchronous queue.
      var safe = new ch.qos.logback.classic.spi.LoggingEvent();
      safe.setLevel(event.getLevel());
      safe.setTimeStamp(event.getTimeStamp());
      safe.setThreadName("native");
      safe.setMessage("native-event");
      String logger = event.getLoggerName();
      String component = "PLATFORM";
      for (String prefix :
          java.util.List.of(
              "io.webrtc.signaling.gateway",
              "io.webrtc.signaling.auth",
              "io.webrtc.signaling.rpc",
              "io.webrtc.signaling.actors",
              "io.webrtc.signaling.storage",
              "org.apache.pekko",
              "org.hibernate",
              "org.flywaydb",
              "com.zaxxer"))
        if (logger != null && logger.startsWith(prefix)) {
          component = prefix;
          break;
        }
      safe.setLoggerName(component);
      var context = new java.util.HashMap<String, String>();
      try {
        TraceContext.attributes(event.getMDCPropertyMap())
            .forEach((key, value) -> context.put(key, value.toString()));
      } catch (RuntimeException unavailable) {
        /* Startup still emits safe metadata. */
      }
      if (event.getThrowableProxy() != null) context.put("errorCode", "INTERNAL_ERROR");
      safe.setMDCPropertyMap(context);
      super.append(safe);
    } catch (RuntimeException unavailable) {
      credits.release();
      dropped.increment();
    }
  }

  @Override
  public void stop() {
    if (!isStarted()) return;
    closing = true;
    super.stop(); // Native export owner waits at most 2s. A blocked sink never extends authority
    // drain.
    // A timed-out worker can still export later; queued originals are unresolved, not proven lost.
    unresolved.set(getQueueSize() - credits.availablePermits());
  }

  public long droppedEvents() {
    return dropped.sum();
  }

  public int unresolvedEvents() {
    return unresolved.get();
  }
}
