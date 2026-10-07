package io.webrtc.signaling.control;
import io.webrtc.signaling.auth.*;
import java.time.*;
import java.util.concurrent.*;
import java.util.function.*;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Mono;
@RestController
public final class BootstrapController {
    public record BootstrapResponse(String wssUrl,String protocol,
        @com.fasterxml.jackson.annotation.JsonFormat(shape=com.fasterxml.jackson.annotation.JsonFormat.Shape.STRING) long directoryEpoch,int retryAfterMillis){}
    private final DirectoryService directory;private final BoundedTokenVerifier verifier;
    private final BiFunction<AuthPrincipal,Instant,AuthorizationStatus> security;private final Clock clock;private final BooleanSupplier trustedClock;
    public BootstrapController(DirectoryService directory,BoundedTokenVerifier verifier,RevocationState security,Clock clock){this(directory,verifier,security::checkRevocation,clock,()->true);}
    public BootstrapController(DirectoryService directory,BoundedTokenVerifier verifier,BiFunction<AuthPrincipal,Instant,AuthorizationStatus> security,Clock clock,BooleanSupplier trustedClock){this.directory=java.util.Objects.requireNonNull(directory);this.verifier=java.util.Objects.requireNonNull(verifier);this.security=java.util.Objects.requireNonNull(security);this.clock=java.util.Objects.requireNonNull(clock);this.trustedClock=java.util.Objects.requireNonNull(trustedClock);}
    @PostMapping("/v1/signaling/bootstrap") public Mono<BootstrapResponse> bootstrapHttp(@RequestHeader(value="Authorization",required=false) String authorization){
        if(authorization==null||!authorization.startsWith("Bearer ")||!trustedClock.getAsBoolean())return Mono.error(new AuthException());
        return Mono.fromCompletionStage(verifier.verify(authorization.substring(7),clock.instant()).thenCompose(this::bootstrap));
    }
    private boolean allowed(AuthPrincipal principal){
        try{var now=clock.instant();return trustedClock.getAsBoolean()&&principal.expiresAt().isAfter(now)&&security.apply(principal,now)==AuthorizationStatus.ALLOWED&&trustedClock.getAsBoolean();}
        catch(RuntimeException unavailable){return false;}
    }
    public CompletionStage<BootstrapResponse> bootstrap(AuthPrincipal principal){
        if(!allowed(principal))return CompletableFuture.failedFuture(new AuthException());
        return directory.resolveHome(principal.userId(),clock.instant()).thenApply(route->{
            if(!allowed(principal))throw new AuthException();
            return new BootstrapResponse(route.wssUrl(),"webrtc-signaling.v1",route.epoch(),0);
        });
    }
}
