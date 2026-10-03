package io.webrtc.signaling.actors.cluster;
import io.webrtc.signaling.protocol.Identity.CallId;
import io.webrtc.signaling.storage.HomeParticipationService;
import org.apache.pekko.cluster.sharding.typed.*;
public final class CallShardExtractor<M> extends ShardingMessageExtractor<ShardingEnvelope<M>,M> {
    @Override public String entityId(ShardingEnvelope<M> envelope){return new CallId(envelope.entityId()).value();}
    @Override public String shardId(String entityId){return Integer.toString(HomeParticipationService.group(new CallId(entityId)));}
    @Override public M unwrapMessage(ShardingEnvelope<M> envelope){return envelope.message();}
}
