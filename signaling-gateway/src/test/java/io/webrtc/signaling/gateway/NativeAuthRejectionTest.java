package io.webrtc.signaling.gateway;

import static org.assertj.core.api.Assertions.*;
import io.webrtc.signaling.auth.*;
import io.webrtc.signaling.protocol.*;
import io.webrtc.signaling.protocol.Identity.*;
import io.webrtc.signaling.storage.SessionRepository;
import io.netty.channel.*;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.websocketx.*;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/** UNIT_ONLY native handler decisions; source enrollment is tested separately. */
class NativeAuthRejectionTest {
    final Instant now=Instant.parse("2026-10-03T00:00:00Z");final Clock clock=Clock.fixed(now,ZoneOffset.UTC);final UUID boot=UUID.randomUUID();
    final AuthPrincipal principal=new AuthPrincipal(new UserId("TEST_ONLY_user"),new SessionKey("TEST_ONLY_ISSUER","TEST_ONLY_jti"),now.plusSeconds(600),now,"TEST_ONLY_key",1);
    final ConnectionRegistry registry=new ConnectionRegistry("gw",boot,10);
    final GatewayIngressBudget credits=new GatewayIngressBudget(8,81920);
    final AtomicReference<AuthorizationStatus> status=new AtomicReference<>(AuthorizationStatus.ALLOWED);
    final AtomicReference<Throwable> verificationFailure=new AtomicReference<>();
    final CompletableFuture<SessionRepository.Route> registration=new CompletableFuture<>();final AtomicInteger registrations=new AtomicInteger(),closed=new AtomicInteger();
    final GatewayServices services=new GatewayServices(){
        public boolean currentBoot(){return true;}
        public CompletionStage<AuthPrincipal> verify(String token,Instant at){var error=verificationFailure.get();return error==null?CompletableFuture.completedFuture(principal):CompletableFuture.failedFuture(error);}
        public AuthorizationStatus cachedSecurity(AuthPrincipal p,Instant at){return status.get();}
        public CompletionStage<SessionRepository.Route> register(AuthPrincipal p,UUID id,Duration budget){registrations.incrementAndGet();return registration;}
        public CompletionStage<SessionRepository.Route> refresh(SessionRepository.Route r,AuthPrincipal p,Duration budget){return CompletableFuture.failedFuture(new UnsupportedOperationException("TEST_ONLY"));}
        public CompletionStage<Void> close(SessionRepository.Route route){closed.incrementAndGet();return CompletableFuture.completedFuture(null);}
        public CompletionStage<String> command(CallCommand command,Duration budget){throw new AssertionError("Rejected authentication must not admit commands");}
    };
    EmbeddedChannel channel(ChannelHandler... before){var pipeline=new ArrayList<ChannelHandler>(Arrays.asList(before));pipeline.add(new FrameAdmissionHandler(new ProtocolValidator(ProtocolLimits.v1()),Runnable::run,credits));pipeline.add(new AuthHandler(registry,services,clock,Runnable::run));pipeline.add(new HeartbeatHandler(registry,services,clock));return new EmbeddedChannel(pipeline.toArray(ChannelHandler[]::new));}
    void authenticate(EmbeddedChannel channel){channel.writeInbound(new TextWebSocketFrame("{\"v\":1,\"type\":\"AUTH\",\"payload\":{\"token\":\"TEST_ONLY_PRIVATE_TOKEN\"}}"));channel.runPendingTasks();}
    SessionRepository.Route route(Channel channel,long generation,SessionIncarnation incarnation){return new SessionRepository.Route(principal.userId(),principal.key(),incarnation,generation,"gw",boot,channel.attr(ConnectionRegistry.CONNECTION).get(),principal.expiresAt(),principal.signingKeyId(),1);}
    void rejected(EmbeddedChannel channel,String reason){Object message=channel.readOutbound();assertThat(message).isInstanceOf(CloseWebSocketFrame.class);var close=(CloseWebSocketFrame)message;try{assertThat(close.statusCode()).isEqualTo(1008);assertThat(close.reasonText()).isEqualTo(reason);assertThat(close.reasonText()).doesNotContain("PRIVATE","jti","TEST_ONLY");}finally{close.release();}assertThat(channel.isActive()).isFalse();assertThat(credits.count()).isZero();assertThat(credits.bytes()).isZero();}
    @ParameterizedTest @EnumSource(value=AuthorizationStatus.class,names={"REVOKED","FRESHNESS_UNKNOWN","TOKEN_EXPIRED"})
    void knownNativeSecurityStatusHasExactBoundedClose(AuthorizationStatus original){status.set(original);var channel=channel();try{authenticate(channel);rejected(channel,"AUTH_"+original.name());assertThat(registrations).hasValue(0);}finally{channel.finishAndReleaseAll();}}
    @ParameterizedTest @EnumSource(value=AuthorizationStatus.class,names={"REVOKED","FRESHNESS_UNKNOWN","TOKEN_EXPIRED"})
    void authenticatedHeartbeatRejectionPreservesOriginalNativeStatus(AuthorizationStatus original){var channel=channel();try{authenticate(channel);registration.complete(route(channel,1,new SessionIncarnation(UUID.randomUUID())));channel.runPendingTasks();((TextWebSocketFrame)channel.readOutbound()).release();status.set(original);channel.pipeline().get(HeartbeatHandler.class).tick(now);channel.runPendingTasks();rejected(channel,"AUTH_"+original.name());assertThat(closed).hasValue(1);}finally{channel.finishAndReleaseAll();}}
    @Test void definiteAuthenticationRejectionDoesNotClaimRevocationOrKeyRetirement(){verificationFailure.set(new CompletionException(new AuthException()));var channel=channel();try{authenticate(channel);rejected(channel,"AUTHORIZATION_REJECTED");assertThat(registrations).hasValue(0);}finally{channel.finishAndReleaseAll();}}
    @Test void unknownSourceFailureCannotClaimDefiniteAuthenticationRejection(){verificationFailure.set(new IllegalStateException("TEST_ONLY_PRIVATE_SOURCE"));var channel=channel();try{authenticate(channel);assertThat((Object)channel.readOutbound()).isNull();assertThat(channel.isActive()).isFalse();assertThat(credits.count()).isZero();}finally{channel.finishAndReleaseAll();}}
    @Test void overloadedVerifierCannotClaimAuthenticationRejection(){verificationFailure.set(new RejectedExecutionException("TEST_ONLY_PRIVATE_SOURCE"));var channel=channel();try{authenticate(channel);assertThat((Object)channel.readOutbound()).isNull();assertThat(channel.isActive()).isFalse();}finally{channel.finishAndReleaseAll();}}
    @Test void revocationAfterNativeRegistrationStillClosesCommittedPresence(){var channel=channel();try{authenticate(channel);assertThat(registrations).hasValue(1);status.set(AuthorizationStatus.REVOKED);registration.complete(route(channel,1,new SessionIncarnation(UUID.randomUUID())));channel.runPendingTasks();rejected(channel,"AUTH_REVOKED");assertThat(closed).hasValue(1);assertThat(registry.authenticatedCount()).isZero();}finally{channel.finishAndReleaseAll();}}
    @Test void nativeGenerationReplacementClosesOldSocketWithOriginalReason(){var old=channel();var replacement=new EmbeddedChannel();try{authenticate(old);var original=route(old,1,new SessionIncarnation(UUID.randomUUID()));registration.complete(original);old.runPendingTasks();((TextWebSocketFrame)old.readOutbound()).release();registry.attach(replacement);var newer=route(replacement,2,original.incarnation());assertThat(registry.bind(newer.connectionId(),newer,principal)).isTrue();old.runPendingTasks();rejected(old,"STALE_CONNECTION");assertThat(replacement.isActive()).isTrue();assertThat(registry.current(newer.connectionId(),newer)).isTrue();}finally{old.finishAndReleaseAll();replacement.finishAndReleaseAll();}}
    @Test void unknownCloseWriteKeepsSocketBoundedAndCannotAdmitAnotherAuth(){
        var frame=new AtomicReference<CloseWebSocketFrame>();var promise=new AtomicReference<ChannelPromise>();
        var channel=channel(new ChannelOutboundHandlerAdapter(){@Override public void write(ChannelHandlerContext ctx,Object message,ChannelPromise original){if(message instanceof CloseWebSocketFrame close){frame.set(close);promise.set(original);}else ctx.write(message,original);}});
        try{status.set(AuthorizationStatus.REVOKED);authenticate(channel);assertThat(frame.get()).isNotNull();assertThat(channel.isActive()).isTrue();status.set(AuthorizationStatus.ALLOWED);authenticate(channel);assertThat(registrations).hasValue(0);assertThat(credits.count()).isZero();channel.advanceTimeBy(1,TimeUnit.SECONDS);channel.runScheduledPendingTasks();assertThat(channel.isActive()).isFalse();assertThat(promise.get().isDone()).isFalse();}finally{var retained=frame.get();if(retained!=null)retained.release();if(promise.get()!=null&&!promise.get().isDone())promise.get().tryFailure(new IllegalStateException("TEST_ONLY cleanup"));channel.finishAndReleaseAll();}
    }
}
