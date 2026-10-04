package io.webrtc.signaling.loadgen;

import java.lang.management.ManagementFactory;
import java.nio.file.*;
import java.io.IOException;
import java.util.*;
import com.sun.management.UnixOperatingSystemMXBean;

/** Actual process FD soft limit and traffic on the assigned source interface only. */
final class GeneratorResources {
    record Counters(long received,long transmitted){}
    private final String sourceInterface;
    private final long nicCapacity;
    private final UnixOperatingSystemMXBean operatingSystem;
    private Counters previous;
    private long previousAt;
    GeneratorResources(String sourceInterface,long nicCapacity)throws IOException {
        this.sourceInterface=Objects.requireNonNull(sourceInterface);this.nicCapacity=nicCapacity;
        if(nicCapacity<=0||!(ManagementFactory.getOperatingSystemMXBean() instanceof UnixOperatingSystemMXBean))throw new IllegalStateException("Actual generator resource measurements unavailable");
        operatingSystem=(UnixOperatingSystemMXBean)ManagementFactory.getOperatingSystemMXBean();
        operatingSystem.getProcessCpuLoad();previous=read();previousAt=System.nanoTime();
    }
    static Counters counters(List<String> lines,String sourceInterface){
        for(String line:lines){int colon=line.indexOf(':');if(colon<0||!line.substring(0,colon).trim().equals(sourceInterface))continue;
            String[] values=line.substring(colon+1).trim().split("\\s+");if(values.length<16)break;
            long receive=Long.parseLong(values[0]),transmit=Long.parseLong(values[8]);if(receive<0||transmit<0)break;return new Counters(receive,transmit);
        }
        throw new IllegalStateException("Assigned source NIC counters unavailable");
    }
    private Counters read()throws IOException{return counters(Files.readAllLines(Path.of("/proc/net/dev")),sourceInterface);}
    Map<String,Object> sample(long elapsed,long lag,int pending,int maxPending,long pendingBytes,long maxBytes,double cpuLimit)throws IOException {
        var current=read();long now=System.nanoTime(),interval=now-previousAt;
        if(interval<=0||current.received()<previous.received()||current.transmitted()<previous.transmitted())throw new IllegalStateException("Source NIC measurement continuity lost");
        double rx=(current.received()-previous.received())*1_000_000_000.0/interval,tx=(current.transmitted()-previous.transmitted())*1_000_000_000.0/interval;
        previous=current;previousAt=now;double cpu=operatingSystem.getProcessCpuLoad();long fd=operatingSystem.getOpenFileDescriptorCount(),fdLimit=operatingSystem.getMaxFileDescriptorCount();
        var measured=new LinkedHashMap<String,Object>();measured.put("elapsedNanos",elapsed);measured.put("sampleIntervalNanos",interval);measured.put("cpu",cpu);measured.put("sourceInterface",sourceInterface);measured.put("nicReceiveBytes",current.received());measured.put("nicTransmitBytes",current.transmitted());measured.put("nicReceiveBytesPerSecond",rx);measured.put("nicTransmitBytesPerSecond",tx);measured.put("nicCapacityBytesPerSecond",nicCapacity);measured.put("fd",fd);measured.put("fdSoftLimit",fdLimit);measured.put("eventLoopLagNanos",lag);measured.put("pendingOperations",pending);measured.put("pendingBytes",pendingBytes);
        var headroom=new LinkedHashMap<>(headroom(cpu,rx,tx,nicCapacity,fd,fdLimit,lag,pending,maxPending,pendingBytes,maxBytes));headroom.put("cpu",Double.isFinite(cpu)&&cpu>=0&&cpu<=cpuLimit);measured.put("headroom",Map.copyOf(headroom));return measured;
    }
    static Map<String,Boolean> headroom(double cpu,double rx,double tx,long nic,long fd,long fdLimit,long lag,int pending,int maxPending,long bytes,long maxBytes){
        return Map.of("cpu",Double.isFinite(cpu)&&cpu>=0&&cpu<=.8,"nic",nic>0&&Double.isFinite(rx)&&Double.isFinite(tx)&&rx>=0&&tx>=0&&Math.max(rx,tx)<=nic*.8,"fd",fd>=0&&fdLimit>0&&fd<=fdLimit*.8,"eventLoop",lag>=0&&lag<=5_000_000,"pendingOperations",pending>=0&&maxPending>0&&pending<=maxPending*.8,"pendingBytes",bytes>=0&&maxBytes>0&&bytes<=maxBytes*.8);
    }
}
