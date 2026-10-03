package io.webrtc.signaling.storage;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Instant;
/** Only the deadline relevant to the committed phase can expire that phase. */
public final class DurableDeadlines {
    private static final ObjectMapper JSON=new ObjectMapper();
    private DurableDeadlines(){}
    public static Instant due(CallSnapshotRepository.Snapshot snapshot){
        String field=switch(snapshot.state()){case "PREPARING"->"prepareUntil";case "RINGING"->"ringUntil";case "ACCEPTED","ACTIVATING"->"activationUntil";case "CONNECTING"->"negotiationUntil";default->null;};
        if(field==null)return null;
        try{var value=JSON.readTree(snapshot.deadlines()).get(field);return value==null?null:Instant.parse(value.asText());}catch(Exception invalid){throw new IllegalArgumentException("Invalid durable deadline",invalid);}
    }
}
