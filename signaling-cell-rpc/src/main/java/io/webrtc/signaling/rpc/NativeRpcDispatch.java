package io.webrtc.signaling.rpc;

import io.grpc.*;

import java.util.*;
import java.util.concurrent.*;

/**
 * Bounded pre-call dispatch for the audited grpc-core 1.75.0 schedule contract. Public tracer and
 * executor APIs attribute synchronous lookup/cancellation submissions; business callbacks still use
 * ServerCallExecutorSupplier. No work executes directly on a transport thread.
 */
final class NativeRpcDispatch implements Executor {
    private static final long MARKER_LIFETIME = TimeUnit.SECONDS.toNanos(2);
    private static final StackWalker STACK = StackWalker.getInstance();
    private final Set<String> relayMethods, controlMethods;
    private final EnumMap<Lane, Marks> marks = new EnumMap<>(Lane.class);
    private final EnumMap<Lane, EnumMap<Phase, ExecutorService>> pools = new EnumMap<>(Lane.class);
    private final ScheduledThreadPoolExecutor expiry;
    private volatile boolean closing;

    NativeRpcDispatch(String name, Set<String> relayMethods, List<ServiceDescriptor> services) {
        requirePinnedRuntime();
        this.relayMethods = Set.copyOf(relayMethods);
        var all = new HashSet<String>();
        services.forEach(
                service -> service.getMethods().forEach(method -> all.add(method.getFullMethodName())));
        if (all.size() > 32 || !all.containsAll(relayMethods))
            throw new IllegalArgumentException("Native dispatch method set outside bound");
        all.removeAll(relayMethods);
        controlMethods = Set.copyOf(all);
        for (var lane : Lane.values()) {
            int bound = lane == Lane.UNCLASSIFIED ? 32 : 128;
            marks.put(lane, new Marks(bound));
            var phases = new EnumMap<Phase, ExecutorService>(Phase.class);
            for (var phase : Phase.values()) {
                int threads = lane == Lane.UNCLASSIFIED ? 1 : 2;
                phases.put(
                        phase,
                        new ThreadPoolExecutor(
                                threads,
                                threads,
                                0,
                                TimeUnit.SECONDS,
                                new ArrayBlockingQueue<>(bound),
                                Thread.ofPlatform()
                                        .daemon()
                                        .name(name + "-" + lane + "-" + phase + "-", 0)
                                        .factory(),
                                new ThreadPoolExecutor.AbortPolicy()));
            }
            pools.put(lane, phases);
        }
        expiry =
                new ScheduledThreadPoolExecutor(
                        1, Thread.ofPlatform().daemon().name(name + "-markers").factory());
        expiry.setRemoveOnCancelPolicy(true);
        expiry.scheduleWithFixedDelay(
                () -> marks.values().forEach(m -> m.expire(System.nanoTime())),
                100,
                100,
                TimeUnit.MILLISECONDS);
    }

    private static void requirePinnedRuntime() {
        try {
            var runtime =
                    Class.forName(
                            "io.grpc.internal.ServerImpl", false, NativeRpcDispatch.class.getClassLoader());
            if (!"1.75.0".equals(runtime.getPackage().getImplementationVersion()))
                throw new IllegalStateException(
                        "Native pre-call dispatch requires audited grpc-core 1.75.0");
        } catch (ClassNotFoundException unavailable) {
            throw new IllegalStateException("Native pre-call dispatch runtime unavailable", unavailable);
        }
    }

    private static boolean synchronousSubmission(Runnable command, Phase phase) {
        String task = command.getClass().getName();
        String owner, method;
        if (phase == Phase.LOOKUP) {
            if (!task.equals("io.grpc.internal.SerializingExecutor")) return false;
            owner = "io.grpc.internal.ServerImpl$ServerTransportListenerImpl";
            method = "streamCreatedInternal";
        } else {
            if (!task.equals("io.grpc.internal.ServerImpl$ContextCloser")) return false;
            owner = "io.grpc.internal.ServerImpl$JumpToApplicationThreadServerStreamListener";
            method = "closedInternal";
        }
        return STACK.walk(
                frames ->
                        frames
                                .limit(64)
                                .anyMatch(
                                        frame ->
                                                frame.getClassName().equals(owner)
                                                        && frame.getMethodName().equals(method)));
    }

    private static long scopeDepth(Phase phase) {
        String owner =
                phase == Phase.LOOKUP
                        ? "io.grpc.internal.ServerImpl$ServerTransportListenerImpl"
                        : "io.grpc.internal.AbstractServerStream$TransportState";
        String method = phase == Phase.LOOKUP ? "streamCreatedInternal" : "closeListener";
        return STACK.walk(
                frames -> {
                    var bounded = frames.limit(65).toList();
                    if (bounded.size() > 64) return 0L;
                    return bounded.stream()
                            .filter(
                                    frame ->
                                            frame.getClassName().equals(owner) && frame.getMethodName().equals(method))
                            .count();
                });
    }

    ServerStreamTracer.Factory tracer() {
        return new ServerStreamTracer.Factory() {
            @Override
            public ServerStreamTracer newServerStreamTracer(String method, Metadata headers) {
                var lane =
                        relayMethods.contains(method)
                                ? Lane.RELAY
                                : controlMethods.contains(method) ? Lane.CONTROL : Lane.UNCLASSIFIED;
                return new ServerStreamTracer() {
                    @Override
                    public Context filterContext(Context context) {
                        mark(lane, Phase.LOOKUP);
                        return context;
                    }

                    @Override
                    public void streamClosed(Status status) {
                        // closeListener calls this immediately before listener.closed. On normal completion
                        // no cancellation submission follows, so any unused marker expires without authority.
                        mark(lane, Phase.CANCEL);
                    }
                };
            }
        };
    }

    private void mark(Lane lane, Phase phase) {
        long depth = scopeDepth(phase);
        if (depth == 0) return;
        var scope = new Scope(Thread.currentThread(), phase, depth);
        marks.values().forEach(m -> m.remove(scope));
        if (closing) return;
        var selected = marks.get(lane);
        if (!selected.capacity.tryAcquire()) return;
        var marker = new Marker(System.nanoTime() + MARKER_LIFETIME);
        var previous = selected.entries.putIfAbsent(scope, marker);
        if (previous != null) selected.capacity.release();
        else if (closing && selected.entries.remove(scope, marker)) selected.capacity.release();
        // These records contain no request, route, context, headers, or payload. Exceptions before
        // submission cannot leak a ThreadLocal; the bounded registry expires orphaned thread keys.
    }

    @Override
    public void execute(Runnable command) {
        Objects.requireNonNull(command);
        Lane lane = Lane.UNCLASSIFIED;
        Phase phase =
                command.getClass().getName().equals("io.grpc.internal.ServerImpl$ContextCloser")
                        ? Phase.CANCEL
                        : Phase.LOOKUP;
        long depth = scopeDepth(phase);
        if (depth > 0 && synchronousSubmission(command, phase)) {
            var scope = new Scope(Thread.currentThread(), phase, depth);
            for (var candidate : Lane.values()) {
                var marker = marks.get(candidate).remove(scope);
                if (marker != null && marker.expires() > System.nanoTime()) lane = candidate;
            }
        }
        if (closing) throw new RejectedExecutionException("Native pre-call dispatch draining");
        pools.get(lane).get(phase).execute(command);
    }

    /**
     * Call only after original transport termination: queued and running tasks remain physical
     * owners.
     */
    void shutdown() {
        closing = true;
        expiry.shutdown();
        // Attribution is metadata, not physical task ownership. Remove it even if a later join fails.
        marks.values().forEach(m -> m.entries.keySet().forEach(m::remove));
        pools.values().forEach(phases -> phases.values().forEach(ExecutorService::shutdown));
    }

    void shutdownAndAwait(long deadline) throws InterruptedException, TimeoutException {
        shutdown();
        var executors = new ArrayList<ExecutorService>();
        pools.values().forEach(phases -> executors.addAll(phases.values()));
        executors.add(expiry);
        for (var executor : executors) {
            long remaining = deadline - System.nanoTime();
            if (remaining <= 0 || !executor.awaitTermination(remaining, TimeUnit.NANOSECONDS))
                throw new TimeoutException("Native pre-call dispatch cleanup unproven");
        }
    }

    private enum Lane {
        CONTROL,
        RELAY,
        UNCLASSIFIED
    }

    private enum Phase {
        LOOKUP,
        CANCEL
    }

    private record Scope(Thread thread, Phase phase, long depth) {
    }

    private record Marker(long expires) {
    }

    private static final class Marks {
        final Semaphore capacity;
        final ConcurrentHashMap<Scope, Marker> entries = new ConcurrentHashMap<>();

        Marks(int bound) {
            capacity = new Semaphore(bound);
        }

        Marker remove(Scope scope) {
            var old = entries.remove(scope);
            if (old != null) capacity.release();
            return old;
        }

        void expire(long now) {
            entries.forEach(
                    (scope, marker) -> {
                        if (now >= marker.expires() && entries.remove(scope, marker)) capacity.release();
                    });
        }
    }
}
