package io.webrtc.signaling.storage;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Instant;
/** Only the deadline relevant to the committed phase can expire that phase. */
public final class DurableDeadlines {
    private static final ObjectMapper JSON=new ObjectMapper();
    private DurableDeadlines(){}
    public static Instant due(CallSnapshotRepository.Snapshot snapshot){
        String field=switch(snapshot.state()){case "PREPARING"->"prepareUntil";case "RINGING"->"ringUntil";case "ACCEPTED","ACTIVATING"->"activationUntil";case "CONNECTING"->"negotiationUntil";default->null;};
        if(snapshot.terminalAt()!=null)return null;
        try{var metadata=JSON.readTree(snapshot.deadlines());Instant due=null;for(String name:new String[]{field,"callerGraceUntil","winnerGraceUntil","mediaRecoveryUntil"}){if(name==null)continue;var value=metadata.get(name);if(value!=null){var candidate=Instant.parse(value.asText());if(due==null||candidate.isBefore(due))due=candidate;}}return due;}catch(Exception invalid){throw new IllegalArgumentException("Invalid durable deadline",invalid);}
    }
}
