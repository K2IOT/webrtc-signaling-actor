package io.webrtc.signaling.gateway;
import io.webrtc.signaling.auth.*;
import io.webrtc.signaling.protocol.*;
import io.webrtc.signaling.protocol.Identity.UserId;
import io.webrtc.signaling.protocol.internal.*;
import io.webrtc.signaling.rpc.*;
import io.webrtc.signaling.storage.*;
import com.google.protobuf.ByteString;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.*;
/** Native non-blocking gateway adapter. Original JWT is independently verified at session home. */
public final class NativeGatewayServices implements GatewayServices {
    public record Home(String cell,long directoryEpoch){public Home{Objects.requireNonNull(cell);if(directoryEpoch<=0)throw new IllegalArgumentException("Invalid directory epoch");}}
    @FunctionalInterface public interface Commands {CompletionStage<String> execute(CallCommand command,Duration budget);}
    private static final ObjectMapper JSON=new ObjectMapper().findAndRegisterModules();
    private final GatewayBootController boot;private final BoundedTokenVerifier verifier;private final BiFunction<AuthPrincipal,Instant,AuthorizationStatus> security;private final Function<UserId,Home> homes;private final CellRpcClient client;private final Commands commands;
    public NativeGatewayServices(GatewayBootController boot,BoundedTokenVerifier verifier,BiFunction<AuthPrincipal,Instant,AuthorizationStatus> cachedSecurity,Function<UserId,Home> cachedHomes,CellRpcClient client,Commands commands){this.boot=Objects.requireNonNull(boot);this.verifier=Objects.requireNonNull(verifier);security=Objects.requireNonNull(cachedSecurity);homes=Objects.requireNonNull(cachedHomes);this.client=Objects.requireNonNull(client);this.commands=Objects.requireNonNull(commands);}
    public boolean currentBoot(){return boot.current();}
    public CompletionStage<AuthPrincipal> verify(String token,Instant now){return verifier.verify(token,now);}
    public AuthorizationStatus cachedSecurity(AuthPrincipal principal,Instant now){return security.apply(principal,now);}
    public CompletionStage<SessionRepository.Route> register(AuthPrincipal p,UUID connection,Duration budget){return CompletableFuture.failedFuture(new IllegalStateException("Original verified JWT required"));}
    public CompletionStage<SessionRepository.Route> refresh(SessionRepository.Route route,AuthPrincipal p,Duration budget){return CompletableFuture.failedFuture(new IllegalStateException("Original verified JWT required"));}
    public CompletionStage<SessionRepository.Route> register(AuthPrincipal p,String token,UUID connection,Duration budget){return mutate("REGISTER",p.userId(),token,null,connection,budget).thenApply(value->decode(value,SessionRepository.Route.class));}
    public CompletionStage<SessionRepository.Route> refresh(SessionRepository.Route route,AuthPrincipal p,String token,Duration budget){return mutate("REFRESH",p.userId(),token,route,null,budget).thenApply(value->decode(value,SessionRepository.Route.class));}
    public CompletionStage<Void> close(SessionRepository.Route route){return mutate("CLOSE",route.user(),null,route,null,Duration.ofSeconds(2)).thenApply(value->null);}
    public CompletionStage<String> command(CallCommand command,Duration budget){if(!currentBoot())return CompletableFuture.failedFuture(new IllegalStateException("Gateway boot unavailable"));return commands.execute(command,budget);}
    private CompletionStage<SessionReply> mutate(String type,UserId user,String token,SessionRepository.Route route,UUID connection,Duration budget){
        if(!type.equals("CLOSE")&&!currentBoot())return CompletableFuture.failedFuture(new IllegalStateException("Gateway boot unavailable"));var home=Objects.requireNonNull(homes.apply(user));if(!home.cell().equals(boot.identity().cell()))return CompletableFuture.failedFuture(new IllegalStateException("DIRECTORY_REDIRECT_REQUIRED"));
        var request=new NativeSessionHandler.Request(type,boot.identity(),token,route,connection,home.directoryEpoch(),0,UUID.randomUUID());var command=SessionCommand.newBuilder().setSchemaMajor(1).setDestinationCell(home.cell()).setType(type).setOperationId(request.operation().toString()).setRemainingBudgetMs(Math.max(1,budget.toMillis())).setPayload(ByteString.copyFrom(RpcBusinessHandler.encode(request))).build();
        return client.session(command,budget).thenApply(value->{if(!value.getAckCommitted()||!value.getStatus().equals("COMMITTED")||!value.getOperationId().equals(command.getOperationId()))throw new CompletionException(new IllegalStateException("Native session outcome unavailable"));return value;});
    }
    private static <T>T decode(SessionReply reply,Class<T> type){try{if(reply.getResult().size()>81920)throw new IllegalArgumentException();return JSON.readValue(reply.getResult().toByteArray(),type);}catch(Exception invalid){throw new CompletionException(new IllegalStateException("Invalid native session reply"));}}
}
