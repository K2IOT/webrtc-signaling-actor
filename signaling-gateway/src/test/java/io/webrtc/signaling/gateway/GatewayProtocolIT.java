package io.webrtc.signaling.gateway;
import static org.assertj.core.api.Assertions.*;
import io.webrtc.signaling.auth.*;
import io.webrtc.signaling.protocol.*;
import io.webrtc.signaling.protocol.Identity.*;
import io.webrtc.signaling.storage.*;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.*;
import io.netty.handler.codec.http.websocketx.*;
import io.netty.buffer.Unpooled;
import java.nio.charset.StandardCharsets;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import org.junit.jupiter.api.Test;
class GatewayProtocolIT {
    static final String AUTH="{\"v\":1,\"type\":\"AUTH\",\"payload\":{\"token\":\"TEST_ONLY\"}}";
    final UUID boot=UUID.randomUUID();final Instant now=Instant.parse("2026-10-03T00:00:00Z");final Clock clock=Clock.fixed(now,ZoneOffset.UTC);
    final AuthPrincipal principal=new AuthPrincipal(new UserId("alice"),new SessionKey("TEST_ONLY","jti"),now.plusSeconds(600),now,"TEST_ONLY",1);
    final AtomicReference<String> commandToken=new AtomicReference<>();final AtomicReference<SessionRepository.Route> commandRoute=new AtomicReference<>();
    final AtomicBoolean validBoot=new AtomicBoolean(true);final AtomicInteger commands=new AtomicInteger();final CompletableFuture<SessionRepository.Route> registration=new CompletableFuture<>();
    final ConnectionRegistry registry=new ConnectionRegistry("gw",boot,1000);
    final GatewayServices services=new GatewayServices(){
        public boolean currentBoot(){return validBoot.get();}
        public CompletionStage<AuthPrincipal> verify(String token,Instant checkedAt){return CompletableFuture.completedFuture(principal);}
        public AuthorizationStatus cachedSecurity(AuthPrincipal p,Instant checkedAt){return AuthorizationStatus.ALLOWED;}
        public CompletionStage<SessionRepository.Route> register(AuthPrincipal p,UUID connection,Duration budget){return registration;}
        public CompletionStage<SessionRepository.Route> refresh(SessionRepository.Route route,AuthPrincipal p,Duration budget){return CompletableFuture.completedFuture(route);}
        public CompletionStage<Void> close(SessionRepository.Route route){return CompletableFuture.completedFuture(null);}
        @Override public CompletionStage<String> command(CallCommand command,SessionRepository.Route route,String token,Duration budget){commandToken.set(token);commandRoute.set(route);return command(command,budget);}
        public CompletionStage<String> command(CallCommand command,Duration budget){commands.incrementAndGet();return CompletableFuture.completedFuture("{\"v\":1,\"type\":\"ACK_COMMITTED\"}");}
    };
    EmbeddedChannel channel(){return new EmbeddedChannel(new WebSocketFrameAggregator(81920),new FrameAdmissionHandler(new ProtocolValidator(ProtocolLimits.v1()),Runnable::run),new AuthHandler(registry,services,clock,Runnable::run),new HeartbeatHandler(registry,services,clock));}
    SessionRepository.Route route(EmbeddedChannel channel,long generation){UUID connection=channel.attr(ConnectionRegistry.CONNECTION).get();return new SessionRepository.Route(principal.userId(),principal.key(),new SessionIncarnation(UUID.randomUUID()),generation,"gw",boot,connection,principal.expiresAt(),principal.signingKeyId(),1);}
    void authenticate(EmbeddedChannel channel){channel.writeInbound(new TextWebSocketFrame(AUTH));registration.complete(route(channel,1));channel.runPendingTasks();}
    @Test void authenticationAcknowledgesOnlyCommittedRegistrationAndGenerationReplacementClosesOldChannel(){
        var channel=channel();try{channel.writeInbound(new TextWebSocketFrame(AUTH));assertThat((Object)channel.readOutbound()).isNull();var route=route(channel,1);registration.complete(route);channel.runPendingTasks();var reply=(TextWebSocketFrame)channel.readOutbound();assertThat(reply.text()).contains("AUTH_OK");reply.release();
            var replacement=new EmbeddedChannel();UUID connection=registry.attach(replacement);var newer=new SessionRepository.Route(route.user(),route.key(),route.incarnation(),2,"gw",boot,connection,route.tokenExpiresAt(),route.signingKeyId(),1);registry.bind(connection,newer,principal);channel.runPendingTasks();assertThat(channel.isActive()).isFalse();assertThat(registry.current(route.connectionId(),route)).isFalse();replacement.finishAndReleaseAll();
        }finally{channel.finishAndReleaseAll();}
    }
    @Test void noCommandsBeforeAuthenticationAndAuthDeadlineIsFiveSeconds(){
        var channel=channel();channel.writeInbound(new TextWebSocketFrame("{\"v\":1,\"type\":\"INVITE\",\"requestId\":\""+UUID.randomUUID()+"\",\"payload\":{\"targetUserId\":\"bob\"}}"));assertThat(channel.isActive()).isFalse();assertThat(commands).hasValue(0);channel.finishAndReleaseAll();
        var waiting=channel();waiting.advanceTimeBy(5,TimeUnit.SECONDS);waiting.runScheduledPendingTasks();assertThat(waiting.isActive()).isFalse();waiting.finishAndReleaseAll();
    }
    @Test void expiredBootCannotAuthenticateOrContinueIngress(){var channel=channel();validBoot.set(false);channel.writeInbound(new TextWebSocketFrame(AUTH));assertThat(channel.isActive()).isFalse();assertThat(registry.authenticatedCount()).isZero();channel.finishAndReleaseAll();}
    @Test void strictUpgradeRequiresAllowedOriginAndNoUrlTokenWithSeparateNativePolicy(){
        var policy=new GatewayServer.UpgradePolicy(Set.of("https://app.example"),headers->headers.contains("X-Test-Native"));
        var request=new DefaultFullHttpRequest(HttpVersion.HTTP_1_1,HttpMethod.GET,"/ws");request.headers().set(HttpHeaderNames.ORIGIN,"https://app.example");request.headers().set(HttpHeaderNames.SEC_WEBSOCKET_PROTOCOL,"webrtc-signaling.v1");assertThat(policy.accepts(request)).isTrue();request.setUri("/ws?token=secret");assertThat(policy.accepts(request)).isFalse();request.setUri("/ws");request.headers().set(HttpHeaderNames.ORIGIN,"https://app.example.evil");assertThat(policy.accepts(request)).isFalse();request.headers().remove(HttpHeaderNames.ORIGIN);assertThat(policy.accepts(request)).isFalse();request.headers().set("X-Test-Native","yes");assertThat(policy.accepts(request)).isTrue();request.release();
    }
    @Test void heartbeatStaysLocalAndClosesAfterTenSecondsWithoutPong(){
        var channel=channel();try{authenticate(channel);((TextWebSocketFrame)channel.readOutbound()).release();var heartbeat=channel.pipeline().get(HeartbeatHandler.class);heartbeat.tick(now.plusSeconds(30));var ping=(PingWebSocketFrame)channel.readOutbound();assertThat(ping).isNotNull();channel.writeInbound(new PongWebSocketFrame(ping.content().retainedDuplicate()));ping.release();assertThat(commands).hasValue(0);heartbeat.tick(now.plusSeconds(60));((PingWebSocketFrame)channel.readOutbound()).release();heartbeat.tick(now.plusSeconds(70));assertThat(channel.isActive()).isFalse();}finally{channel.finishAndReleaseAll();}
    }
    @Test void aggregateFramesAreBoundedIncludingFragments(){var channel=channel();try{channel.writeInbound(new TextWebSocketFrame(false,0,Unpooled.wrappedBuffer(new byte[40960])));channel.writeInbound(new ContinuationWebSocketFrame(true,0,Unpooled.wrappedBuffer(new byte[40961])));assertThat(channel.isActive()).isFalse();}finally{channel.finishAndReleaseAll();}}
    @Test void unauthenticatedCapacityCannotGrowPastConfiguredBound(){var limited=new ConnectionRegistry("gw",boot,1);var a=new EmbeddedChannel();var b=new EmbeddedChannel();try{limited.attach(a);assertThatThrownBy(()->limited.attach(b)).isInstanceOf(RejectedExecutionException.class);}finally{a.finishAndReleaseAll();b.finishAndReleaseAll();}}
    @Test void registrationCommittedAfterSocketCloseIsConditionallyClosedInsteadOfLeavingLivePresence(){
        var closed=new AtomicInteger();GatewayServices tracked=new GatewayServices(){
            public boolean currentBoot(){return true;}public CompletionStage<AuthPrincipal> verify(String t,Instant n){return CompletableFuture.completedFuture(principal);}public AuthorizationStatus cachedSecurity(AuthPrincipal p,Instant n){return AuthorizationStatus.ALLOWED;}
            public CompletionStage<SessionRepository.Route> register(AuthPrincipal p,UUID c,Duration b){return registration;}public CompletionStage<SessionRepository.Route> refresh(SessionRepository.Route r,AuthPrincipal p,Duration b){throw new AssertionError();}
            public CompletionStage<Void> close(SessionRepository.Route r){closed.incrementAndGet();return CompletableFuture.completedFuture(null);}public CompletionStage<String> command(CallCommand c,Duration b){throw new AssertionError();}
        };
        var channel=new EmbeddedChannel(new FrameAdmissionHandler(new ProtocolValidator(ProtocolLimits.v1()),Runnable::run),new AuthHandler(registry,tracked,clock,Runnable::run));channel.writeInbound(new TextWebSocketFrame(AUTH));var nativeRoute=route(channel,1);org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(3)).until(()->{channel.runPendingTasks();return !channel.isActive();});registration.complete(nativeRoute);channel.runPendingTasks();assertThat(closed.get()).isEqualTo(1);assertThat(registry.authenticatedCount()).isZero();channel.finishAndReleaseAll();
    }
    @Test void fragmentDeadlineClosesAuthenticatedSocketAndReleasesAggregateBudget(){
        var budget=new GatewayIngressBudget(4,81920);var channel=channel();authenticate(channel);((TextWebSocketFrame)channel.readOutbound()).release();channel.pipeline().addBefore(channel.pipeline().context(WebSocketFrameAggregator.class).name(),"fragment-timeout",new FragmentAdmissionHandler(budget));
        channel.writeInbound(new TextWebSocketFrame(false,0,Unpooled.wrappedBuffer(new byte[1024])));assertThat(budget.bytes()).isEqualTo(1024);channel.advanceTimeBy(10,TimeUnit.SECONDS);channel.runScheduledPendingTasks();assertThat(channel.isActive()).isFalse();assertThat(budget.bytes()).isZero();channel.finishAndReleaseAll();
    }

    @Test void anEmptyFinalContinuationIsValidAndReleasesTheFragmentBudget(){
        var budget=new GatewayIngressBudget(10,262144);var channel=new EmbeddedChannel(new FragmentAdmissionHandler(budget),new WebSocketFrameAggregator(81920),new FrameAdmissionHandler(new ProtocolValidator(ProtocolLimits.v1()),Runnable::run,budget),new AuthHandler(registry,services,clock,Runnable::run));
        try{channel.writeInbound(new TextWebSocketFrame(false,0,AUTH));channel.writeInbound(new ContinuationWebSocketFrame(true,0,Unpooled.EMPTY_BUFFER));registration.complete(route(channel,1));channel.runPendingTasks();assertThat(channel.isActive()).isTrue();var reply=(TextWebSocketFrame)channel.readOutbound();assertThat(reply.text()).contains("AUTH_OK");reply.release();assertThat(budget.count()).isZero();assertThat(budget.bytes()).isZero();}finally{channel.finishAndReleaseAll();}
    }
    @Test void parsingLaterHangupDoesNotWaitForAnUnfinishedInviteBusinessFuture(){
        var invite=new CompletableFuture<String>();var seen=new java.util.ArrayList<SignalEnvelope.Type>();GatewayServices concurrent=new GatewayServices(){
            public boolean currentBoot(){return true;}public CompletionStage<AuthPrincipal> verify(String token,Instant checked){return CompletableFuture.completedFuture(principal);}public AuthorizationStatus cachedSecurity(AuthPrincipal p,Instant checked){return AuthorizationStatus.ALLOWED;}
            public CompletionStage<SessionRepository.Route> register(AuthPrincipal p,UUID c,Duration b){return registration;}public CompletionStage<SessionRepository.Route> refresh(SessionRepository.Route r,AuthPrincipal p,Duration b){throw new AssertionError();}public CompletionStage<Void> close(SessionRepository.Route r){return CompletableFuture.completedFuture(null);}
            public CompletionStage<String> command(CallCommand command,Duration budget){seen.add(command.type());return command.type()==SignalEnvelope.Type.INVITE?invite:CompletableFuture.completedFuture("{\"v\":1,\"type\":\"FINAL\"}");}
        };
        var budget=new GatewayIngressBudget(10,262144);var channel=new EmbeddedChannel(new FrameAdmissionHandler(new ProtocolValidator(ProtocolLimits.v1()),Runnable::run,budget),new AuthHandler(registry,concurrent,clock,Runnable::run));try{
            channel.writeInbound(new TextWebSocketFrame(AUTH));registration.complete(route(channel,1));channel.runPendingTasks();((TextWebSocketFrame)channel.readOutbound()).release();
            channel.writeInbound(new TextWebSocketFrame("{\"v\":1,\"type\":\"INVITE\",\"requestId\":\""+UUID.randomUUID()+"\",\"payload\":{\"targetUserId\":\"bob\"}}"));
            channel.writeInbound(new TextWebSocketFrame("{\"v\":1,\"type\":\"HANGUP\",\"requestId\":\""+UUID.randomUUID()+"\",\"callId\":\"c001.e1.00000000-0000-0000-0000-000000000001\",\"payload\":{}}"));channel.runPendingTasks();assertThat(seen).containsExactly(SignalEnvelope.Type.INVITE,SignalEnvelope.Type.HANGUP);assertThat(budget.count()).isEqualTo(1);invite.complete("{\"v\":1,\"type\":\"FINAL\"}");channel.runPendingTasks();assertThat(budget.count()).isZero();
        }finally{channel.finishAndReleaseAll();}
    }
    @Test void deliveryRechecksFullCurrentRouteAndNeverReturnsCommittedCommandAck(){
        var channel=channel();try{authenticate(channel);((TextWebSocketFrame)channel.readOutbound()).release();var route=registry.binding(channel.attr(ConnectionRegistry.CONNECTION).get()).route();var event=io.webrtc.signaling.protocol.internal.ControlEvent.newBuilder().setEventId(UUID.randomUUID().toString()).setCallId("c001.e1.00000000-0000-0000-0000-000000000001").setCallVersion(2).setAuthorityBucketId(1).setType("CALL_READY").setMetadata(com.google.protobuf.ByteString.copyFromUtf8("{}" )).setDestination(io.webrtc.signaling.protocol.internal.SessionIdentity.newBuilder().setIssuer(route.key().issuer()).setJti(route.key().jti()).setUserId(route.user().value()).setConnectionGeneration(route.connectionGeneration()).setIncarnation(com.google.protobuf.ByteString.copyFrom(java.nio.ByteBuffer.allocate(16).putLong(route.incarnation().value().getMostSignificantBits()).putLong(route.incarnation().value().getLeastSignificantBits()).array())).setConnectionId(com.google.protobuf.ByteString.copyFrom(java.nio.ByteBuffer.allocate(16).putLong(route.connectionId().getMostSignificantBits()).putLong(route.connectionId().getLeastSignificantBits()).array()))).build();
            var stream=new GatewayDeliveryStream(registry,services,clock,Runnable::run);var result=stream.send(event);channel.runPendingTasks();assertThat(result.toCompletableFuture().join().getStatus()).isEqualTo("WRITE_COMPLETED");assertThat(result.toCompletableFuture().join().getAckCommitted()).isFalse();var message=(TextWebSocketFrame)channel.readOutbound();assertThat(message.text()).contains("CALL_READY","eventId");message.release();
            var stale=stream.send(event.toBuilder().setDestination(event.getDestination().toBuilder().setConnectionGeneration(2)).build());channel.runPendingTasks();assertThat(stale.toCompletableFuture().join().getErrorCode()).isEqualTo("STALE_BINDING");assertThat((Object)channel.readOutbound()).isNull();
        }finally{channel.finishAndReleaseAll();}
    }
    @Test void commandCarriesTheOriginalVerifiedTokenAndExactCommittedRouteForIndependentNativeProofRead(){var channel=channel();try{authenticate(channel);((TextWebSocketFrame)channel.readOutbound()).release();channel.writeInbound(new TextWebSocketFrame("{\"v\":1,\"type\":\"SYNC_CALL\",\"requestId\":\""+UUID.randomUUID()+"\",\"callId\":\"c001.e1.00000000-0000-0000-0000-000000000001\",\"payload\":{}}"));channel.runPendingTasks();assertThat(commandToken).hasValue("TEST_ONLY");assertThat(commandRoute.get().connectionId()).isEqualTo(channel.attr(ConnectionRegistry.CONNECTION).get());assertThat(commandRoute.get().connectionGeneration()).isEqualTo(1);}finally{channel.finishAndReleaseAll();}}
    @Test void upgradeRequiresOneBoundedHeaderOfferingTheSupportedSubprotocol(){
        var policy=new GatewayServer.UpgradePolicy(Set.of("https://app.example"),headers->false);
        var request=new DefaultFullHttpRequest(HttpVersion.HTTP_1_1,HttpMethod.GET,"/ws");request.headers().set(HttpHeaderNames.ORIGIN,"https://app.example");
        try{
            assertThat(policy.accepts(request)).isFalse();
            request.headers().set(HttpHeaderNames.SEC_WEBSOCKET_PROTOCOL,"unsupported.v2");assertThat(policy.accepts(request)).isFalse();
            request.headers().set(HttpHeaderNames.SEC_WEBSOCKET_PROTOCOL,"unsupported.v2, webrtc-signaling.v1");assertThat(policy.accepts(request)).isTrue();
            request.headers().add(HttpHeaderNames.SEC_WEBSOCKET_PROTOCOL,"webrtc-signaling.v1");assertThat(policy.accepts(request)).isFalse();
            request.headers().set(HttpHeaderNames.SEC_WEBSOCKET_PROTOCOL,"x".repeat(257)+",webrtc-signaling.v1");assertThat(policy.accepts(request)).isFalse();
        }finally{request.release();}
    }

}
