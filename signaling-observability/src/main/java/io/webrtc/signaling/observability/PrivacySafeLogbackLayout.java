package io.webrtc.signaling.observability;

import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.LayoutBase;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.*;

/** Production logs exclude SDK free text, formatted arguments and throwable messages/stacks. */
public final class PrivacySafeLogbackLayout extends LayoutBase<ILoggingEvent> {
  private static final ObjectMapper JSON = new ObjectMapper();

  @Override
  public String doLayout(ILoggingEvent event) {
    var row = new LinkedHashMap<String, Object>();
    row.put("timestampMillis", event.getTimeStamp());
    row.put("level", event.getLevel().toString());
    row.put("component", component(event.getLoggerName()));
    Map<String, String> context = Map.of();
    try {
      if (event.getMDCPropertyMap() != null) context = event.getMDCPropertyMap();
    } catch (RuntimeException missingContext) {
      /* Early startup still emits only safe metadata. */
    }
    row.put("attributes", TraceContext.attributes(context));
    if (event.getThrowableProxy() != null) row.put("errorCode", "INTERNAL_ERROR");
    try {
      return JSON.writeValueAsString(row) + "\n";
    } catch (Exception encodingFailure) {
      return "{\"level\":\"ERROR\",\"errorCode\":\"INTERNAL_ERROR\"}\n";
    }
  }

  private static String component(String logger) {
    if (logger == null) return "PLATFORM";
    if (logger.startsWith("io.webrtc.signaling.gateway")) return "GATEWAY";
    if (logger.startsWith("io.webrtc.signaling.auth")) return "AUTH";
    if (logger.startsWith("io.webrtc.signaling.rpc")) return "RPC";
    if (logger.startsWith("io.webrtc.signaling.actors") || logger.startsWith("org.apache.pekko"))
      return "ACTOR";
    if (logger.startsWith("io.webrtc.signaling.storage")
        || logger.startsWith("org.hibernate")
        || logger.startsWith("org.flywaydb")
        || logger.startsWith("com.zaxxer")) return "STORAGE";
    return "PLATFORM";
  }
}
