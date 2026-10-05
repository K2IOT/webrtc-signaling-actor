package io.webrtc.signaling.rpc;
import io.webrtc.signaling.protocol.CallCommand;
import io.webrtc.signaling.storage.*;
import io.webrtc.signaling.protocol.Identity.*;
import io.webrtc.signaling.protocol.PortableProofTime;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.core.*;
import java.security.*;
import java.time.*;
import java.util.*;
/** Reusable auth-only proof for one call/negotiation/ICE generation. It conveys no active participation or group tenure. */
public final class RelaySessionAuthorizationProof {
    public record Claims(int schema,String destinationCell,CallId call,long negotiationId,long iceGeneration,SessionRegistryService.SessionProofView nativeView) {
        public Claims {Objects.requireNonNull(call);Objects.requireNonNull(nativeView);var view=nativeView;
            if(schema!=1||!call.coordinatorCell().equals(destinationCell)||negotiationId<1||iceGeneration<1||view.route()==null||view.checkedAt()==null||view.proofUntil()==null||!view.proofUntil().isAfter(view.checkedAt())||view.proofUntil().isAfter(view.checkedAt().plusSeconds(5))||view.sourceStorageEpoch()<1||view.directoryEpoch()<1||view.sourceCell()==null||!view.sourceCell().matches("[a-z][a-z0-9-]{0,23}")||view.proofUntil().isAfter(view.route().tokenExpiresAt()))throw new IllegalArgumentException("Invalid auth-only proof");
        }
        @Override public String toString(){return "RelaySessionProof[schema="+schema+"]";}
    }
    private static final ObjectMapper JSON=new ObjectMapper(JsonFactory.builder().streamReadConstraints(StreamReadConstraints.builder().maxNestingDepth(16).maxStringLength(4096).build()).enable(StreamReadFeature.STRICT_DUPLICATE_DETECTION).build()).findAndRegisterModules().enable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES,DeserializationFeature.FAIL_ON_TRAILING_TOKENS,DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES).disable(MapperFeature.ALLOW_COERCION_OF_SCALARS);
    private final String cell,keyId;private final PrivateKey privateKey;private final Map<String,PublicKey> trusted;
    public RelaySessionAuthorizationProof(String cell,String keyId,PrivateKey privateKey,Map<String,PublicKey> trusted){if(cell==null||!cell.matches("[a-z][a-z0-9-]{0,23}")||keyId==null||!keyId.matches("[A-Za-z0-9_-]{1,64}")||trusted.size()>256||!("EdDSA".equals(privateKey.getAlgorithm())||"Ed25519".equals(privateKey.getAlgorithm())))throw new IllegalArgumentException("Invalid proof key configuration");this.cell=cell;this.keyId=keyId;this.privateKey=Objects.requireNonNull(privateKey);this.trusted=Map.copyOf(trusted);}
    public RelaySessionAuthorizationProof(Map<String,PublicKey> trusted){if(trusted==null||trusted.size()>256)throw new IllegalArgumentException("Invalid proof trust configuration");this.cell=null;this.keyId=null;this.privateKey=null;this.trusted=Map.copyOf(trusted);}
    public static boolean supports(CallCommand command){return command!=null&&command.callId()!=null&&command.scope().equals(CommandScope.call(command.callId()))&&command.negotiationId()!=null&&command.iceGeneration()!=null&&Set.of(io.webrtc.signaling.protocol.SignalEnvelope.Type.OFFER,io.webrtc.signaling.protocol.SignalEnvelope.Type.ANSWER,io.webrtc.signaling.protocol.SignalEnvelope.Type.ICE_CANDIDATES,io.webrtc.signaling.protocol.SignalEnvelope.Type.END_OF_CANDIDATES).contains(command.type());}
    public String issue(SessionRegistryService.SessionProofView view,CallCommand command){
        if(!supports(command)||privateKey==null)throw new IllegalArgumentException("Relay proof purpose required");var route=view.route();if(!cell.equals(view.sourceCell())||command.callId()==null||!same(route,command.sender()))throw new AuthoritySql.FencedException();
        var claims=new Claims(1,command.callId().coordinatorCell(),command.callId(),command.negotiationId().value(),command.iceGeneration().value(),view);
        try{String unsigned=cell+"."+keyId+".R1."+Base64.getUrlEncoder().withoutPadding().encodeToString(JSON.writeValueAsBytes(claims));var signer=Signature.getInstance("Ed25519");signer.initSign(privateKey);signer.update(unsigned.getBytes(java.nio.charset.StandardCharsets.US_ASCII));String signed=unsigned+"."+Base64.getUrlEncoder().withoutPadding().encodeToString(signer.sign());if(signed.length()>4096)throw new IllegalArgumentException("Oversized session proof");return signed;}catch(GeneralSecurityException|java.io.IOException failure){throw new IllegalArgumentException("Cannot sign native session proof",failure);}
    }
    public Optional<Claims> decode(String signed,String source,Instant now){try{
        if(signed==null||signed.length()>4096||now==null)return Optional.empty();var pieces=signed.split("\\.",-1);if(pieces.length!=5||!pieces[0].equals(source)||!pieces[2].equals("R1")||!pieces[3].matches("[A-Za-z0-9_-]+")||!pieces[4].matches("[A-Za-z0-9_-]{86}"))return Optional.empty();var key=trusted.get(pieces[0]+"/"+pieces[1]);if(key==null)return Optional.empty();var verifier=Signature.getInstance("Ed25519");verifier.initVerify(key);verifier.update(String.join(".",Arrays.copyOf(pieces,4)).getBytes(java.nio.charset.StandardCharsets.US_ASCII));if(!verifier.verify(Base64.getUrlDecoder().decode(pieces[4])))return Optional.empty();var claims=JSON.readValue(Base64.getUrlDecoder().decode(pieces[3]),Claims.class);var view=claims.nativeView();if(!source.equals(view.sourceCell())||!PortableProofTime.valid(view.checkedAt(),view.proofUntil(),now))return Optional.empty();return Optional.of(claims);
    }catch(Exception invalid){return Optional.empty();}}
    public boolean verify(String signed,CallCommand command,ProofBindings.TrustedHome home,Instant now){if(home==null||!supports(command))return false;return decode(signed,home.cell(),now).filter(p->p.destinationCell().equals(command.callId().coordinatorCell())&&p.call().equals(command.callId())&&p.negotiationId()==command.negotiationId().value()&&p.iceGeneration()==command.iceGeneration().value()&&p.nativeView().directoryEpoch()==home.directoryEpoch()&&p.nativeView().sourceStorageEpoch()==home.storageEpoch()&&same(p.nativeView().route(),command.sender())).isPresent();}
    private static boolean same(SessionRepository.Route route,AuthenticatedSession sender){return route.user().equals(sender.userId())&&route.key().equals(sender.key())&&route.incarnation().equals(sender.incarnation())&&route.connectionGeneration()==sender.connectionGeneration()&&route.connectionId().equals(sender.connectionId());}
}
