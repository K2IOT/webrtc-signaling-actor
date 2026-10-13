package io.webrtc.signaling.app.runtime;

import org.springframework.context.SmartLifecycle;

import java.util.List;
import java.util.concurrent.CompletionStage;

/**
 * Independent bootstrap owner; shutdown callback is released only by original physical cleanup.
 */
public final class NativeCompatibilityObservationLifecycle implements SmartLifecycle {
    private final NativeWorkerScheduler worker;
    private volatile boolean running;

    public NativeCompatibilityObservationLifecycle(
            List<NativeWorkerScheduler.Job> jobs, NativeTelemetry telemetry) {
        worker = new NativeWorkerScheduler(jobs, event -> {
        }).observe(telemetry::workerStarted);
    }

    @Override
    public void start() {
        worker.start();
        running = true;
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    @Override
    public int getPhase() {
        return Integer.MAX_VALUE;
    }

    public CompletionStage<Void> drain() {
        running = false;
        return worker.drain();
    }

    @Override
    public void stop(Runnable callback) {
        drain().thenRun(callback);
    }

    @Override
    public void stop() {
        drain();
    }
}
