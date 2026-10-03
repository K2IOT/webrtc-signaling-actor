package io.webrtc.signaling.control;
import io.webrtc.signaling.auth.*;
import java.time.*;
import java.util.concurrent.*;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Mono;
@RestController
public final class BootstrapController {
    public record BootstrapResponse(String wssUrl,String protocol,long directoryEpoch,int retryAfterMillis){}
    private final DirectoryService directory;private final BoundedTokenVerifier verifier;private final RevocationState security;private final Clock clock;
    public BootstrapController(DirectoryService directory,BoundedTokenVerifier verifier,RevocationState security,Clock clock){this.directory=directory;this.verifier=verifier;this.security=security;this.clock=clock;}
    @PostMapping("/v1/signaling/bootstrap") public Mono<BootstrapResponse> bootstrapHttp(@RequestHeader("Authorization") String authorization){if(authorization==null||!authorization.startsWith("Bearer "))return Mono.error(new AuthException());return Mono.fromCompletionStage(verifier.verify(authorization.substring(7),clock.instant()).thenCompose(this::bootstrap));}
    public CompletionStage<BootstrapResponse> bootstrap(AuthPrincipal principal){if(security.checkRevocation(principal,clock.instant())!=AuthorizationStatus.ALLOWED)return CompletableFuture.failedFuture(new AuthException());return directory.resolveHome(principal.userId(),clock.instant()).thenApply(r->new BootstrapResponse(r.wssUrl(),"webrtc-signaling.v1",r.epoch(),0));}
}
