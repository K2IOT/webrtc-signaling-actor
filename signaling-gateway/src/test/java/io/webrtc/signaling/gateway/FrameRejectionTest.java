package io.webrtc.signaling.gateway;

import static org.assertj.core.api.Assertions.*;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.websocketx.*;
import io.webrtc.signaling.protocol.*;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class FrameRejectionTest {
    @Test void malformedJsonReturnsBoundedProtocolCloseWithoutReflectingPayload(){
        var credit=new GatewayIngressBudget(8,81920);var channel=new EmbeddedChannel(new FrameAdmissionHandler(new ProtocolValidator(ProtocolLimits.v1()),Runnable::run,credit));
        try {
            channel.writeInbound(new TextWebSocketFrame("{TEST_ONLY_PRIVATE_TOKEN"));channel.runPendingTasks();
            Object original=channel.readOutbound();assertThat(original).isInstanceOf(CloseWebSocketFrame.class);
            var close=(CloseWebSocketFrame)original;try{assertThat(close.statusCode()).isEqualTo(1002);assertThat(close.reasonText()).isEqualTo("PROTOCOL_REJECTED");}finally{close.release();}
            assertThat(channel.isActive()).isFalse();assertThat(credit.count()).isZero();assertThat(credit.bytes()).isZero();
        }finally{channel.finishAndReleaseAll();}
    }
    @Test void expiredCpuQueueCannotClaimProtocolRejection()throws Exception{
        var retained=new AtomicReference<Runnable>();var credit=new GatewayIngressBudget(8,81920);var channel=new EmbeddedChannel(new FrameAdmissionHandler(new ProtocolValidator(ProtocolLimits.v1()),retained::set,credit));
        try {
            channel.writeInbound(new TextWebSocketFrame("{"));assertThat(credit.count()).isEqualTo(1);Thread.sleep(270);retained.get().run();channel.runPendingTasks();
            assertThat((Object)channel.readOutbound()).isNull();assertThat(channel.isActive()).isFalse();assertThat(credit.count()).isZero();
        }finally{channel.finishAndReleaseAll();}
    }
}
