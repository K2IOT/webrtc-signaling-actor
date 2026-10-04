package io.webrtc.signaling.loadgen;

import java.util.OptionalInt;

/** One bounded local ICE trace. END is emitted once after the actual final candidate. */
final class NegotiationTrace {
    private final long deadline;
    private final int limit;
    private int sequence;
    private boolean answered, sealed;
    NegotiationTrace(long start,long duration,int limit) {
        if(duration<=0||limit<1||limit>256)throw new IllegalArgumentException("Invalid trace bounds");
        deadline=Math.addExact(start,duration);this.limit=limit;
    }
    synchronized OptionalInt nextCandidate(long now) {
        if(sealed||now>=deadline||sequence>=limit)return OptionalInt.empty();
        return OptionalInt.of(++sequence);
    }
    synchronized void answerObserved(){answered=true;}
    synchronized OptionalInt seal(long now) {
        if(sealed||!answered||now<deadline&&sequence<limit)return OptionalInt.empty();
        sealed=true;return OptionalInt.of(sequence);
    }
}
