package io.webrtc.signaling.actors.cluster;
import io.webrtc.signaling.protocol.Identity.UserId;
import io.webrtc.signaling.storage.SessionRegistryService;
import org.apache.pekko.cluster.sharding.typed.*;
public final class UserShardExtractor<M> extends ShardingMessageExtractor<ShardingEnvelope<M>,M> {
    @Override public String entityId(ShardingEnvelope<M> envelope){return new UserId(envelope.entityId()).value();}
    @Override public String shardId(String entityId){return Integer.toString(SessionRegistryService.bucket(new UserId(entityId))&1023);}
    @Override public M unwrapMessage(ShardingEnvelope<M> envelope){return envelope.message();}
}
