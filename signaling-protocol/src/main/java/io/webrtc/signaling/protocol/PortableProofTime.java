package io.webrtc.signaling.protocol;

import java.time.*;

/** Qualified maximum pair bound250ms plus <=1000ppm drift over a <=5s proof window. */
public final class PortableProofTime {
    private static final Duration PAIR_UNCERTAINTY=Duration.ofMillis(250);
    private static final Duration CONSERVATIVE_MARGIN=Duration.ofMillis(255);
    private PortableProofTime(){}
    public static boolean valid(Instant issued,Instant until,Instant receiverNow){
        if(issued==null||until==null||receiverNow==null)return false;
        try{return until.isAfter(issued)&&!until.isAfter(issued.plusSeconds(5))
            &&!issued.isAfter(receiverNow.plus(PAIR_UNCERTAINTY))
            &&receiverNow.plus(CONSERVATIVE_MARGIN).isBefore(until);}
        catch(DateTimeException|ArithmeticException invalid){return false;}
    }
}
