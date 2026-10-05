package io.webrtc.signaling.rpc;
import io.webrtc.signaling.protocol.Identity.AuthenticatedSession;
import java.util.*;
/** Exact destination from a verified, versioned home ACTIVE proof. */
public record RelayDestination(String cell,String gatewayId,UUID bootId,AuthenticatedSession recipient){
    public RelayDestination {if(cell==null||!cell.matches("[a-z][a-z0-9-]{0,23}")||gatewayId==null||!gatewayId.matches("[A-Za-z0-9_.-]{1,128}")||bootId==null||recipient==null)throw new IllegalArgumentException("Invalid relay destination");}
}
