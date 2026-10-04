package io.webrtc.signaling.it;

import static org.assertj.core.api.Assertions.*;
import io.webrtc.signaling.actors.relay.*;
import io.webrtc.signaling.actors.admission.ActorOperation;
import io.webrtc.signaling.protocol.Identity.*;
import io.webrtc.signaling.storage.AuthoritySql;
import com.fasterxml.jackson.databind.*;
import com.sun.net.httpserver.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import org.junit.jupiter.api.Test;

/** Real Chromium/native ICE agents. Loopback authority fixtures do not qualify production routing. */
class WebRtcInteropIT {
    static final ObjectMapper JSON=new ObjectMapper();
    static final class Boundary implements AutoCloseable {
        final UUID activation=UUID.randomUUID();
        final CallId call=CallId.create("c001",1);
        final AuthenticatedSession browser=session("TEST_ONLY_BROWSER"),nativePeer=session("TEST_ONLY_NATIVE");
        final AuthoritySql.GroupToken group=new AuthoritySql.GroupToken("c001",1,1,685,1,"TEST_ONLY_OWNER",UUID.randomUUID());
        final RelayBufferBudget memory=new RelayBufferBudget(1048576);
        final Map<String,IceReceiveWindow> windows=new HashMap<>();
        final Map<String,IceReceiveWindow.Item> pending=new HashMap<>();
        final Map<String,CompletableFuture<Void>> applied=new HashMap<>();
        final Map<String,Long> calls=new HashMap<>();
        final Map<String,Set<Long>> sequences=new HashMap<>();
        final NegotiationRelay relay;
        NegotiationRelay.Description transported;
        long round;
        Boundary(){var cache=new RelayAuthorizationCache(4,2,System::nanoTime,()->true,c->Optional.of(group));relay=new NegotiationRelay(4,System::nanoTime,memory,cache,(c,s,r,i,b)->{
            long now=System.nanoTime(),until=now+Duration.ofSeconds(25).toNanos();var snapshot=new RelayAuthorizationCache.Snapshot(call,activation,3,r,i,"CONNECTING",s,s.equals(browser)?nativePeer:browser,group,now,until,until,until,until);
            return new ActorOperation<>(CompletableFuture.completedFuture(snapshot),CompletableFuture.completedFuture(null));
        },description->{transported=description;return new ActorOperation<>(CompletableFuture.completedFuture(null),CompletableFuture.completedFuture(null));});}
        static AuthenticatedSession session(String id){return new AuthenticatedSession(new UserId(id),new SessionKey("TEST_ONLY",id),new SessionIncarnation(UUID.randomUUID()),1,UUID.randomUUID());}
        IceReceiveWindow.Key key(String side,long generation){return new IceReceiveWindow.Key(call,generation,generation,(side.equals("browser")?browser:nativePeer).incarnation().value());}
        synchronized Object request(String path,JsonNode body){String side=body.path("side").asText();return switch(path){
            case "/init"->{round=body.path("round").asLong();windows.values().forEach(IceReceiveWindow::close);windows.clear();pending.clear();applied.clear();
                if(!relay.install(new NegotiationRelay.Grant(call,activation,3,round,round,browser,nativePeer,System.nanoTime()+Duration.ofSeconds(20).toNanos(),group)))throw new IllegalStateException("round conflict");
                for(String participant:List.of("browser","native")){String scope=participant+round;windows.put(participant,new IceReceiveWindow(key(participant,round),256,262144,Duration.ofSeconds(10),System::nanoTime,item->{synchronized(this){
                    if(!sequences.computeIfAbsent(scope,k->new HashSet<>()).add(item.sequence()))throw new AssertionError("duplicate ICE-agent call");
                    calls.merge(scope,1L,Long::sum);pending.put(participant,item);var completion=new CompletableFuture<Void>();applied.put(participant,completion);return completion;
                }},memory));}yield Map.of("ready",true);}
            case "/description"->{var description=new NegotiationRelay.Description(call,round,round,side.equals("browser")?browser:nativePeer,UUID.fromString(body.path("requestId").asText()),side.equals("browser")?NegotiationRelay.Kind.OFFER:NegotiationRelay.Kind.ANSWER,body.path("sdp").asText());
                relay.send(description,Duration.ofSeconds(1)).toCompletableFuture().join();yield Map.of("sdp",transported.body());}
            case "/ready"->{windows.get(side).remoteDescriptionReady(body.path("ufrag").asText());yield Map.of("ready",true);}
            case "/candidate"->{var items=new ArrayList<IceReceiveWindow.Item>();for(var item:body.path("items"))items.add(new IceReceiveWindow.Item(item.path("sequence").asLong(),item.hasNonNull("candidate")?item.get("candidate").asText():null,item.hasNonNull("sdpMid")?item.get("sdpMid").asText():null,item.hasNonNull("sdpMLineIndex")?item.get("sdpMLineIndex").asInt():null,item.hasNonNull("usernameFragment")?item.get("usernameFragment").asText():null,item.path("end").asBoolean()));yield windows.get(side).accept(key(side,body.path("round").asLong()),items);}
            case "/next"->pending.containsKey(side)?pending.get(side):Map.of("empty",true);
            case "/applied"->{var item=pending.remove(side);if(item==null||item.sequence()!=body.path("sequence").asLong())throw new IllegalArgumentException("wrong ICE completion");applied.remove(side).complete(null);yield windows.get(side).tick();}
            case "/stats"->{var counts=new TreeMap<String,Object>();counts.putAll(calls);counts.put("round",round);counts.put("browserEnded",windows.get("browser").ended());counts.put("nativeEnded",windows.get("native").ended());yield counts;}
            default->throw new IllegalArgumentException("unknown test endpoint");};}
        public synchronized void close(){relay.close();windows.values().forEach(IceReceiveWindow::close);applied.values().forEach(f->f.completeExceptionally(new CancellationException()));assertThat(memory.retainedBytes()).isZero();}
    }
    @Test void browserNativeOfferAnswerOrderedTrickleRestartAndEndMarkers()throws Exception {
        var executor=Executors.newFixedThreadPool(2);var server=HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(),0),8);
        try(var boundary=new Boundary()){
            server.setExecutor(executor);server.createContext("/",exchange->{try{if(exchange.getRequestMethod().equals("GET")&&exchange.getRequestURI().getPath().equals("/client")){byte[] html="<!doctype html><title>TEST_ONLY WebRTC interop</title>".getBytes(StandardCharsets.UTF_8);exchange.getResponseHeaders().set("Content-Type","text/html");exchange.sendResponseHeaders(200,html.length);exchange.getResponseBody().write(html);return;}byte[] bytes=exchange.getRequestBody().readNBytes(81921);if(bytes.length>81920)throw new IllegalArgumentException("test payload too large");Object answer=boundary.request(exchange.getRequestURI().getPath(),JSON.readTree(bytes));byte[] response=JSON.writeValueAsBytes(answer);exchange.sendResponseHeaders(200,response.length);exchange.getResponseBody().write(response);}catch(Throwable failure){byte[] response="TEST_BOUNDARY_REJECTED".getBytes(StandardCharsets.UTF_8);exchange.sendResponseHeaders(400,response.length);exchange.getResponseBody().write(response);}finally{exchange.close();}});server.start();
            Path script=Path.of("src/test/webrtc/interop.cjs").toAbsolutePath();assertThat(script).exists();Path output=Path.of("target/webrtc-interop.log");Files.createDirectories(output.getParent());
            Process process=new ProcessBuilder("node",script.toString(),"http://127.0.0.1:"+server.getAddress().getPort()).redirectErrorStream(true).redirectOutput(output.toFile()).start();
            if(!process.waitFor(50,TimeUnit.SECONDS)){process.destroyForcibly();throw new AssertionError("real WebRTC interop exceeded bounded deadline");}
            assertThat(process.exitValue()).as(Files.readString(output)).isZero();JsonNode evidence=JSON.readTree(Files.readString(output).strip());assertThat(evidence.path("rounds").asInt()).isEqualTo(2);assertThat(evidence.path("dataChannelMessages").asInt()).isEqualTo(2);assertThat(evidence.path("nativeEnded").asBoolean()).isTrue();assertThat(evidence.path("browserEnded").asBoolean()).isTrue();assertThat(evidence.path("browser").asText()).isNotBlank();
        }finally{server.stop(0);executor.shutdownNow();}
    }
}
