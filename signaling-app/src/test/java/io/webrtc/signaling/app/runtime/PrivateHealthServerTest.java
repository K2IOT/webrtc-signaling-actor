package io.webrtc.signaling.app.runtime;
import static org.assertj.core.api.Assertions.*;
import java.net.*;
import java.net.http.*;
import java.time.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import org.junit.jupiter.api.Test;
class PrivateHealthServerTest {
    static HttpRequest request(int port,String path){return HttpRequest.newBuilder(URI.create("http://127.0.0.1:"+port+path)).timeout(Duration.ofSeconds(2)).GET().build();}
    @Test void actualPrivateListenerSeparatesLivenessAndReadyAdmissionWithoutDatabaseOnIoThreads()throws Exception {
        var ready=new AtomicBoolean(false);var live=new AtomicBoolean(true);var client=HttpClient.newHttpClient();
        try(var health=new PrivateHealthServer(new InetSocketAddress("127.0.0.1",0),live::get,ready::get,()->"test_only_metric 1\n")){
            health.start().toCompletableFuture().get(2,TimeUnit.SECONDS);int port=health.port();
            assertThat(client.send(request(port,"/live"),HttpResponse.BodyHandlers.ofString()).statusCode()).isEqualTo(200);
            assertThat(client.send(request(port,"/ready"),HttpResponse.BodyHandlers.ofString()).statusCode()).isEqualTo(503);
            ready.set(true);assertThat(client.send(request(port,"/ready"),HttpResponse.BodyHandlers.ofString()).statusCode()).isEqualTo(200);
            ready.set(false);assertThat(client.send(request(port,"/ready"),HttpResponse.BodyHandlers.ofString()).statusCode()).isEqualTo(503);
            assertThat(client.send(request(port,"/ws?token=TEST_ONLY"),HttpResponse.BodyHandlers.ofString()).statusCode()).isEqualTo(404);
            live.set(false);assertThat(client.send(request(port,"/live"),HttpResponse.BodyHandlers.ofString()).statusCode()).isEqualTo(503);
        }
        assertThatThrownBy(()->new PrivateHealthServer(new InetSocketAddress("0.0.0.0",8559),()->true,()->false,()->"")).isInstanceOf(IllegalArgumentException.class);
    }
    @Test void boundedMetricsScrapeRunsOffIoAndCannotBlockHealthOrQueueMoreScrapes()throws Exception {
        var entered=new CountDownLatch(1);var release=new CountDownLatch(1);var thread=new AtomicReference<String>();var client=HttpClient.newHttpClient();
        try(var health=new PrivateHealthServer(new InetSocketAddress("127.0.0.1",0),()->true,()->false,()->{thread.set(Thread.currentThread().getName());entered.countDown();try{if(!release.await(3,TimeUnit.SECONDS))throw new IllegalStateException();}catch(InterruptedException e){Thread.currentThread().interrupt();throw new IllegalStateException(e);}return "test_only_metric 1\n";})){
            health.start().toCompletableFuture().get(2,TimeUnit.SECONDS);int port=health.port();
            var first=client.sendAsync(request(port,"/metrics"),HttpResponse.BodyHandlers.ofString());assertThat(entered.await(1,TimeUnit.SECONDS)).isTrue();
            assertThat(client.send(request(port,"/metrics"),HttpResponse.BodyHandlers.ofString()).statusCode()).isEqualTo(429);
            assertThat(client.send(request(port,"/ready"),HttpResponse.BodyHandlers.ofString()).statusCode()).isEqualTo(503);
            release.countDown();var body=first.get(2,TimeUnit.SECONDS);assertThat(body.statusCode()).isEqualTo(200);assertThat(body.body()).isEqualTo("test_only_metric 1\n");assertThat(thread.get()).startsWith("private-metrics");
        }finally{release.countDown();}
    }
}
