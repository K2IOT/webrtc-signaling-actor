package io.webrtc.signaling.observability;

import org.slf4j.MDC;

import java.util.*;

/**
 * W3C v00 only; baggage, tracestate and arbitrary public attributes are never copied.
 */
public record TraceContext(String traceId, String spanId, boolean sampled) {
    public TraceContext {
        if (SensitiveDataRedactor.approved("traceId", traceId) == null
                || SensitiveDataRedactor.approved("spanId", spanId) == null)
            throw new IllegalArgumentException("Invalid trace identity");
    }

    public static Optional<TraceContext> parse(String header) {
        if (header == null || !header.matches("00-[a-f0-9]{32}-[a-f0-9]{16}-(00|01)"))
            return Optional.empty();
        try {
            return Optional.of(
                    new TraceContext(
                            header.substring(3, 35), header.substring(36, 52), header.endsWith("01")));
        } catch (IllegalArgumentException invalid) {
            return Optional.empty();
        }
    }

    public static TraceContext root(boolean sampled) {
        return new TraceContext(
                UUID.randomUUID().toString().replace("-", ""),
                UUID.randomUUID().toString().replace("-", "").substring(0, 16),
                sampled);
    }

    /**
     * Capture only bounded approved identity, never the complete MDC or baggage.
     */
    public static TraceContext current() {
        String trace = MDC.get("traceId");
        String span = MDC.get("spanId");
        if (SensitiveDataRedactor.approved("traceId", trace) == null) return null;
        if (SensitiveDataRedactor.approved("spanId", span) == null)
            span = UUID.randomUUID().toString().replace("-", "").substring(0, 16);
        return new TraceContext(trace, span, false);
    }

    public static Scope activate(TraceContext context) {
        return new Scope(context);
    }

    public static Map<String, Object> attributes(Map<String, ?> input) {
        var safe = SensitiveDataRedactor.redact(input);
        var result = new LinkedHashMap<String, Object>();
        safe.forEach(
                (key, value) -> {
                    if (!SensitiveDataRedactor.REDACTED.equals(value)) result.put(key, value);
                });
        return Collections.unmodifiableMap(result);
    }

    public TraceContext child() {
        return new TraceContext(
                traceId, UUID.randomUUID().toString().replace("-", "").substring(0, 16), sampled);
    }

    public String header() {
        return "00-" + traceId + "-" + spanId + (sampled ? "-01" : "-00");
    }

    /**
     * Restores both fields, including absence, on the same callback thread.
     */
    public static final class Scope implements AutoCloseable {
        private final String priorTrace = MDC.get("traceId"), priorSpan = MDC.get("spanId");

        private Scope(TraceContext context) {
            restore("traceId", context == null ? null : context.traceId());
            restore("spanId", context == null ? null : context.spanId());
        }

        private static void restore(String field, String value) {
            if (value == null) MDC.remove(field);
            else MDC.put(field, value);
        }

        @Override
        public void close() {
            restore("traceId", priorTrace);
            restore("spanId", priorSpan);
        }
    }
}
