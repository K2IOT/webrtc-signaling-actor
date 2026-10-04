package io.webrtc.signaling.rpc;

import java.util.OptionalLong;

/** Single retry owner, at most two retries, exponential full jitter within the original deadline. */
final class RpcRetryBackoff {
    private RpcRetryBackoff(){}
    static OptionalLong delayNanos(int attempt,double unit,long remaining){
        if(attempt<0||!Double.isFinite(unit)||unit<0||unit>=1)throw new IllegalArgumentException("Invalid retry jitter");
        if(attempt>=2)return OptionalLong.empty();
        long delay=Math.max(1_000_000,(long)((25_000_000L<<attempt)*unit));
        return remaining>delay+5_000_000?OptionalLong.of(delay):OptionalLong.empty();
    }
}
