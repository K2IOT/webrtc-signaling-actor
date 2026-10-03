package io.webrtc.signaling.actors.cluster;
import static org.assertj.core.api.Assertions.*;
import io.webrtc.signaling.actors.user.*;
import io.webrtc.signaling.protocol.Identity.*;
import io.webrtc.signaling.storage.*;
import io.webrtc.signaling.protocol.internal.ActorEnvelope;
import org.apache.pekko.actor.testkit.typed.javadsl.ActorTestKit;
import org.apache.pekko.actor.typed.javadsl.Adapter;
import java.time.*;
import java.util.*;
import org.junit.jupiter.api.Test;
class ApplicationSerializerTest {
    @Test void protobufRoundTripsTypedOperationAndReplyActorRefWithinAdjacentSchemaWindow()throws Exception {
        var kit=ActorTestKit.create();try{var serializer=new ApplicationSerializer(Adapter.toClassic(kit.system()));var probe=kit.<UserCommand.Result>createTestProbe();Instant now=Instant.now();UUID operation=UUID.randomUUID();
            var request=new HomeParticipationService.Request(new UserId("alice"),new CallId("c001.e1.00000000-0000-0000-0000-000000000001"),operation,"a".repeat(64),1,HomeParticipationService.Phase.PREPARING,new HomeParticipationService.Grant("c001",1,1,685,2,1,operation,now,now.plusSeconds(5),"TEST_ONLY"));
            var value=new UserCommand.Mutate(new UserCommand.Reserve(request),probe.ref(),now.plusSeconds(2),1024);byte[] encoded=serializer.toBinary(value);var envelope=ActorEnvelope.parseFrom(encoded);assertThat(envelope.getSchemaMajor()).isEqualTo(1);assertThat(envelope.getKind()).isEqualTo("user.mutate");assertThat(encoded.length).isLessThanOrEqualTo(98304);assertThat(serializer.fromBinary(encoded,"v1")).isEqualTo(value);
            assertThat(serializer.fromBinary(envelope.toBuilder().setSchemaMinor(1).build().toByteArray(),"v1")).isEqualTo(value);assertThatThrownBy(()->serializer.fromBinary(envelope.toBuilder().setSchemaMajor(2).build().toByteArray(),"v1")).isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(()->serializer.toBinary(new Object())).isInstanceOf(IllegalArgumentException.class);assertThatThrownBy(()->serializer.fromBinary(new byte[98305],"v1")).isInstanceOf(IllegalArgumentException.class);
        }finally{kit.shutdownTestKit();}
    }
}
