package io.webrtc.signaling.rpc;

import io.grpc.*;
import io.webrtc.signaling.observability.OperationObservation;
import io.webrtc.signaling.observability.SignalingMetrics;
import io.webrtc.signaling.observability.TraceContext;
import org.slf4j.MDC;

/**
 * Only W3C trace identity crosses authenticated RPC; no baggage or user-supplied attributes.
 */
public final class NativeRpcTelemetry {
    public static final CallOptions.Key<String> TRACE_ID = CallOptions.Key.create("native-trace-id");
    private static final Metadata.Key<String> TRACE =
            Metadata.Key.of("traceparent", Metadata.ASCII_STRING_MARSHALLER);
    private NativeRpcTelemetry() {
    }

    public static ClientInterceptor client() {
        return new ClientInterceptor() {
            public <Q, A> ClientCall<Q, A> interceptCall(
                    MethodDescriptor<Q, A> method, CallOptions options, Channel next) {
                String traceId = options.getOption(TRACE_ID);
                if (traceId == null) traceId = MDC.get("traceId");
                var trace =
                        traceId != null && traceId.matches("[a-f0-9]{32}") && !traceId.equals("0".repeat(32))
                                ? new TraceContext(traceId, TraceContext.root(false).spanId(), false)
                                : TraceContext.root(false);
                return new ForwardingClientCall.SimpleForwardingClientCall<>(
                        next.newCall(method, options)) {
                    public void start(Listener<A> listener, Metadata headers) {
                        headers.put(TRACE, trace.header());
                        super.start(listener, headers);
                    }
                };
            }
        };
    }

    public static ServerInterceptor server() {
        return new ServerInterceptor() {
            public <Q, A> ServerCall.Listener<Q> interceptCall(
                    ServerCall<Q, A> call, Metadata headers, ServerCallHandler<Q, A> next) {
                var trace =
                        TraceContext.parse(headers.get(TRACE))
                                .orElseGet(() -> TraceContext.root(false))
                                .child();
                var listener = next.startCall(call, headers);
                return new ForwardingServerCallListener.SimpleForwardingServerCallListener<>(listener) {
                    private void scoped(Runnable action) {
                        var prior = MDC.getCopyOfContextMap();
                        try {
                            MDC.put("traceId", trace.traceId());
                            MDC.put("spanId", trace.spanId());
                            action.run();
                        } finally {
                            if (prior == null) MDC.clear();
                            else MDC.setContextMap(prior);
                        }
                    }

                    public void onMessage(Q value) {
                        scoped(() -> super.onMessage(value));
                    }

                    public void onHalfClose() {
                        scoped(() -> super.onHalfClose());
                    }

                    public void onCancel() {
                        scoped(() -> super.onCancel());
                    }

                    public void onComplete() {
                        scoped(() -> super.onComplete());
                    }

                    public void onReady() {
                        scoped(() -> super.onReady());
                    }
                };
            }
        };
    }

    public static <T> RpcOperation<T> observe(
            RpcOperation<T> original,
            OperationObservation.Receipt receipt,
            java.util.function.Function<T, SignalingMetrics.Outcome> outcome) {
        original
                .logical()
                .whenComplete(
                        (value, failure) -> {
                            SignalingMetrics.Outcome classification = SignalingMetrics.Outcome.OUTCOME_UNKNOWN;
                            if (failure == null)
                                try {
                                    classification = outcome.apply(value);
                                } catch (RuntimeException ignored) {
                                }
                            receipt.logical(classification);
                        });
        original
                .physicalCompletion()
                .whenComplete(
                        (value, failure) -> {
                            if (failure == null) receipt.physical(true);
                        });
        return original;
    }

    public static SignalingMetrics.Outcome outcome(String code) {
        if (code == null || code.isBlank()) return SignalingMetrics.Outcome.OUTCOME_UNKNOWN;
        return switch (code) {
            case "COMMITTED", "ACK_COMMITTED", "REGISTERED", "REFRESHED", "CLOSED" ->
                    SignalingMetrics.Outcome.COMMITTED;
            case "READ", "ROUTE", "WRITE_COMPLETED", "APPLICATION_RECEIVED", "PROOF_READ" ->
                    SignalingMetrics.Outcome.READ;
            case "USER_BUSY" -> SignalingMetrics.Outcome.USER_BUSY;
            case "CALL_ENDED" -> SignalingMetrics.Outcome.CALL_ENDED;
            case "TOKEN_EXPIRED" -> SignalingMetrics.Outcome.TOKEN_EXPIRED;
            case "UNAUTHORIZED", "FORBIDDEN", "INVALID_MESSAGE" -> SignalingMetrics.Outcome.FORBIDDEN;
            case "STALE_VERSION" -> SignalingMetrics.Outcome.STALE_VERSION;
            case "OVERLOADED", "BACKPRESSURE" -> SignalingMetrics.Outcome.BACKPRESSURE;
            case "OUTCOME_UNKNOWN", "PENDING", "WORKFLOW_PENDING" -> SignalingMetrics.Outcome.OUTCOME_UNKNOWN;
            case "UNAVAILABLE", "WRONG_CELL", "DIRECTORY_REDIRECT_REQUIRED" -> SignalingMetrics.Outcome.UNAVAILABLE;
            default -> SignalingMetrics.Outcome.INTERNAL_ERROR;
        };
    }
}
