package io.webrtc.signaling.rpc;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Objects;
/** Auth-only source binding plus two independently signed ACTIVE participant projections. */
public record CriticalCommandProof(String session,String callerActive,String winnerActive) {
    private static final ObjectMapper JSON=new ObjectMapper();
    public CriticalCommandProof {for(String value:new String[]{session,callerActive,winnerActive})if(value==null||value.isBlank()||value.length()>4096)throw new IllegalArgumentException("Invalid critical proof bundle");}
    public String encode(){try{String encoded=JSON.writeValueAsString(this);if(encoded.length()>8192)throw new IllegalArgumentException("Oversized critical proof bundle");return encoded;}catch(java.io.IOException error){throw new IllegalArgumentException("Invalid proof bundle",error);}}
    @Override public String toString(){return "CriticalCommandProof[redacted]";}
}
