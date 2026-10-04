package io.webrtc.signaling.loadgen;

import java.nio.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.security.*;
import java.util.*;

/** Same deterministic partition on every source machine; start one bounded worker per approved source IP. */
public final class DistributedLoadGenerator {
    public record Range(long start,long end){public long size(){return end-start;}}
    public static long userIndex(long socket,long users){
        if(socket<0||users<1||users>10000000)throw new IllegalArgumentException("Invalid identity range");
        if(socket<users)return socket;
        long half=users/2;return half==0?0:half+Math.floorMod(socket-users,users-half);
    }
    public static Range partition(long total,int index,int workers){
        if(total<0||workers<1||workers>4096||index<0||index>=workers)throw new IllegalArgumentException("Invalid worker partition");
        long quotient=total/workers,remainder=total%workers;long start=Math.addExact(Math.multiplyExact(quotient,index),Math.min(index,remainder));return new Range(start,Math.addExact(start,quotient+(index<remainder?1:0)));
    }
    public static UUID operation(long seed,long socketIndex,String type,long ordinal){
        if(socketIndex<0||ordinal<0||type==null||!type.matches("[A-Z_]{1,32}"))throw new IllegalArgumentException("Invalid deterministic operation");
        try{byte[] value=MessageDigest.getInstance("SHA-256").digest(("signaling-loadgen-v1\n"+seed+"\n"+socketIndex+"\n"+type+"\n"+ordinal).getBytes(StandardCharsets.UTF_8));value[6]=(byte)((value[6]&15)|0x80);value[8]=(byte)((value[8]&63)|0x80);var b=ByteBuffer.wrap(value);return new UUID(b.getLong(),b.getLong());}catch(NoSuchAlgorithmException impossible){throw new IllegalStateException(impossible);}
    }
    public static void main(String[] args)throws Exception {
        if(args.length!=6||!args[0].equals("--scenario")||!args[2].equals("--config")||!args[4].equals("--evidence"))throw new IllegalArgumentException("Use --scenario PATH --config PATH --evidence DIRECTORY");
        try{new ScenarioRunner().run(Path.of(args[1]),Path.of(args[3]),Path.of(args[5]));}catch(Exception failed){throw new IllegalStateException("Worker failed; inspect redacted evidence or approved input configuration");}
    }
}
