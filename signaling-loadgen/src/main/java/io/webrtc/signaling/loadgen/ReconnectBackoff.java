package io.webrtc.signaling.loadgen;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.*;
/** Reproducible source jitter; Retry-After remains a minimum from its original observation. */
final class ReconnectBackoff {
    static long delayNanos(long seed,long socket,int attempt,long serverMinimumNanos){
        if(socket<0||attempt<0||serverMinimumNanos<0||serverMinimumNanos>Duration.ofDays(1).toNanos())throw new IllegalArgumentException("Invalid reconnect budget");
        long ceiling=Math.min(30_000_000_000L,500_000_000L<<Math.min(attempt,6));
        long jitter=new SplittableRandom(seed^Long.rotateLeft(socket,29)^((long)attempt*0x9e3779b97f4a7c15L)).nextLong(ceiling+1);
        return Math.max(jitter,serverMinimumNanos);
    }
    static Optional<Duration> retryAfter(String text,Instant now){
        try{
            if(text==null||text.length()>64)return Optional.empty();final Duration delay;
            if(text.matches("[0-9]{1,5}"))delay=Duration.ofSeconds(Long.parseLong(text));
            else delay=Duration.between(now,ZonedDateTime.parse(text,DateTimeFormatter.RFC_1123_DATE_TIME).toInstant());
            if(delay.compareTo(Duration.ofDays(1))>0)return Optional.empty();return Optional.of(delay.isNegative()?Duration.ZERO:delay);
        }catch(RuntimeException invalid){return Optional.empty();}
    }
    private ReconnectBackoff(){}
}
