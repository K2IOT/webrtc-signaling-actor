package io.webrtc.signaling.app.runtime;

import java.util.*;
import java.util.function.Consumer;

/** Explicit attested clock endpoint/PKI, process binding and native worker observations. */
public record NativeControlSourceEnrollment(String cell,long storageEpoch,UUID podUid,UUID processBoot,
        NativeActorSourceEnrollment.Endpoint clock,Consumer<NativeWorkerScheduler.Event> events) {
    public NativeControlSourceEnrollment {
        if(cell==null||!cell.matches("[a-z][a-z0-9-]{0,23}")||storageEpoch<1)throw new IllegalArgumentException("Invalid control source authority");
        Objects.requireNonNull(podUid);Objects.requireNonNull(processBoot);Objects.requireNonNull(clock);Objects.requireNonNull(events);
    }
}
