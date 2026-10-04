package io.webrtc.signaling.observability;
import java.util.*;
/** W3C v00 only; baggage, tracestate and arbitrary public attributes are never copied. */
public record TraceContext(String traceId,String spanId,boolean sampled) {
    public TraceContext{if(SensitiveDataRedactor.approved("traceId",traceId)==null||SensitiveDataRedactor.approved("spanId",spanId)==null)throw new IllegalArgumentException("Invalid trace identity");}
    public static Optional<TraceContext> parse(String header){if(header==null||!header.matches("00-[a-f0-9]{32}-[a-f0-9]{16}-(00|01)"))return Optional.empty();try{return Optional.of(new TraceContext(header.substring(3,35),header.substring(36,52),header.endsWith("01")));}catch(IllegalArgumentException invalid){return Optional.empty();}}
    public static TraceContext root(boolean sampled){return new TraceContext(UUID.randomUUID().toString().replace("-",""),UUID.randomUUID().toString().replace("-","").substring(0,16),sampled);}
    public TraceContext child(){return new TraceContext(traceId,UUID.randomUUID().toString().replace("-","").substring(0,16),sampled);}
    public String header(){return "00-"+traceId+"-"+spanId+(sampled?"-01":"-00");}
    public static Map<String,Object> attributes(Map<String,?> input){var safe=SensitiveDataRedactor.redact(input);var result=new LinkedHashMap<String,Object>();safe.forEach((key,value)->{if(!SensitiveDataRedactor.REDACTED.equals(value))result.put(key,value);});return Collections.unmodifiableMap(result);}
}
