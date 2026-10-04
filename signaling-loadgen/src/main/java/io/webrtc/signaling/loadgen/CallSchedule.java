package io.webrtc.signaling.loadgen;

import java.util.*;
import java.util.function.LongUnaryOperator;

/** Disjoint primary caller/callee pairs and globally permuted open-loop arrival slots. */
final class CallSchedule {
    record Assignment(long callerSocket,long targetUser,long ordinal,long intendedNanos){}
    private record Slot(long caller,long target,long ordinal){}
    private final List<Slot> slots;
    private final long pairs;
    private long cycle;
    private int cursor;
    CallSchedule(long seed,long users,DistributedLoadGenerator.Range sockets){
        if(users<1||users>10000000||sockets.size()>200000)throw new IllegalArgumentException("Invalid bounded call partition");
        pairs=users/2;
        if(pairs==0){slots=List.of();return;}
        long multiplier=Math.floorMod(seed,pairs)+1;
        for(int attempt=0;gcd(multiplier,pairs)!=1;attempt++)multiplier=attempt>=63?1:multiplier+1;
        long offset=Math.floorMod(seed^0x5deece66dL,pairs),targetRotation=Math.floorMod(Long.rotateLeft(seed,17),pairs);
        var owned=new ArrayList<Slot>();for(long socket=sockets.start();socket<Math.min(sockets.end(),pairs);socket++)
            owned.add(new Slot(socket,pairs+Math.floorMod(socket+targetRotation,pairs),Math.floorMod(multiplier*socket+offset,pairs)));
        owned.sort(Comparator.comparingLong(Slot::ordinal));slots=List.copyOf(owned);
    }
    Optional<Assignment> nextDue(LongUnaryOperator arrival,long now){
        if(slots.isEmpty())return Optional.empty();var slot=slots.get(cursor);long ordinal=Math.addExact(slot.ordinal(),Math.multiplyExact(cycle,pairs));long intended=arrival.applyAsLong(ordinal);
        if(intended>now)return Optional.empty();cursor++;if(cursor==slots.size()){cursor=0;cycle=Math.incrementExact(cycle);}
        return Optional.of(new Assignment(slot.caller(),slot.target(),ordinal,intended));
    }
    private static long gcd(long a,long b){while(b!=0){long remainder=a%b;a=b;b=remainder;}return a;}
}
