package io.webrtc.signaling.auth;
import java.security.interfaces.RSAPublicKey;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.function.Supplier;

/** Only configured trusted public keys; tokens cannot choose network endpoints. */
public final class TrustedRsaKeys {
    private final AtomicReference<Map<String,RSAPublicKey>> keys;
    private final Set<String> retired=ConcurrentHashMap.newKeySet();
    private final Supplier<CompletionStage<Map<String,RSAPublicKey>>> refresh;
    private final long intervalMillis;
    private final AtomicBoolean refreshing=new AtomicBoolean();
    private final AtomicLong lastRefresh=new AtomicLong(Long.MIN_VALUE);
    public TrustedRsaKeys(Map<String,RSAPublicKey> keys,Supplier<CompletionStage<Map<String,RSAPublicKey>>> refresh,Duration interval) {
        this.keys=new AtomicReference<>(validated(keys));this.refresh=refresh;
        if(interval==null||interval.isNegative()||interval.isZero())throw new IllegalArgumentException("refresh interval");intervalMillis=interval.toMillis();
    }
    private static Map<String,RSAPublicKey> validated(Map<String,RSAPublicKey> candidate) {
        if(candidate==null||candidate.isEmpty()||candidate.size()>64)throw new IllegalArgumentException("bounded public key set required");
        candidate.forEach((id,key)->{new io.webrtc.signaling.protocol.Identity.SessionKey("kid",id);if(key==null||key.getModulus().bitLength()<2048)throw new IllegalArgumentException("RSA modulus below 2048 bits");});
        return Map.copyOf(candidate);
    }
    public Optional<RSAPublicKey> lookup(String kid,Instant now) {
        if(kid==null){var available=keys.get();if(available.size()!=1)return Optional.empty();kid=available.keySet().iterator().next();}
        if(retired.contains(kid))return Optional.empty();
        RSAPublicKey known=keys.get().get(kid);if(known!=null)return Optional.of(known);
        long previous=lastRefresh.get(), current=now.toEpochMilli();
        if(refresh!=null&&(previous==Long.MIN_VALUE||current-previous>=intervalMillis)&&refreshing.compareAndSet(false,true)) {
            lastRefresh.set(current);
            try {refresh.get().whenComplete((updated,error)->{try{if(error==null)keys.set(validated(updated));}finally{refreshing.set(false);}});}
            catch(RuntimeException e){refreshing.set(false);}
        }
        return Optional.empty();
    }
    public void retire(String kid){retired.add(kid);}
    public boolean retired(String kid){return retired.contains(kid);}
}
