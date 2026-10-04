package io.webrtc.signaling.gateway;
import io.webrtc.signaling.auth.*;
import io.webrtc.signaling.protocol.CallCommand;
import io.webrtc.signaling.storage.SessionRepository;
import java.time.*;
import java.util.UUID;
import java.util.concurrent.CompletionStage;
/** Non-blocking ports. cachedSecurity and currentBoot must be local bounded reads, never JDBC/JPA. */
public interface GatewayServices {
    boolean currentBoot();
    CompletionStage<AuthPrincipal> verify(String token,Instant now);
    AuthorizationStatus cachedSecurity(AuthPrincipal principal,Instant now);
    CompletionStage<SessionRepository.Route> register(AuthPrincipal principal,UUID connection,Duration budget);
    CompletionStage<SessionRepository.Route> refresh(SessionRepository.Route route,AuthPrincipal principal,Duration budget);
    default CompletionStage<SessionRepository.Route> register(AuthPrincipal principal,String originalVerifiedToken,UUID connection,Duration budget){return register(principal,connection,budget);}
    default CompletionStage<SessionRepository.Route> refresh(SessionRepository.Route route,AuthPrincipal principal,String originalVerifiedToken,Duration budget){return refresh(route,principal,budget);}
    CompletionStage<Void> close(SessionRepository.Route route);
    CompletionStage<String> command(CallCommand command,Duration budget);
    default CompletionStage<String> command(CallCommand command,SessionRepository.Route route,String originalVerifiedToken,Duration budget){return command(command,budget);}

}
