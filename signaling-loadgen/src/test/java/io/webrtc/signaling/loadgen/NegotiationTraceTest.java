package io.webrtc.signaling.loadgen;

import static org.assertj.core.api.Assertions.*;
import org.junit.jupiter.api.Test;

class NegotiationTraceTest {
    @Test void terminalRequiresAnswerAndCutsOffLaterCandidateWrites() {
        var trace=new NegotiationTrace(100,10,3);
        assertThat(trace.nextCandidate(101)).hasValue(1);
        assertThat(trace.seal(111)).isEmpty();
        trace.answerObserved();
        assertThat(trace.seal(111)).hasValue(1);
        assertThat(trace.nextCandidate(112)).isEmpty();
        assertThat(trace.seal(112)).isEmpty();
        trace.answerObserved();
        assertThat(trace.seal(113)).isEmpty();
    }
    @Test void boundedTraceSealsAtItsLastActualSequenceWithoutEndingBeforeAnswer() {
        var trace=new NegotiationTrace(100,1000,2);
        assertThat(trace.nextCandidate(101)).hasValue(1);
        assertThat(trace.nextCandidate(102)).hasValue(2);
        assertThat(trace.nextCandidate(103)).isEmpty();
        assertThat(trace.seal(103)).isEmpty();
        trace.answerObserved();
        assertThat(trace.seal(103)).hasValue(2);
        assertThat(trace.seal(104)).isEmpty();
    }
    @Test void expiredTraceCannotAdmitCandidateBeforeDelayedAnswer() {
        var trace=new NegotiationTrace(100,10,256);
        assertThat(trace.nextCandidate(110)).isEmpty();
        trace.answerObserved();
        assertThat(trace.seal(111)).hasValue(0);
    }
}
