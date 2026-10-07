package io.webrtc.signaling.gateway;
import io.webrtc.signaling.auth.AuthPrincipal;
import io.webrtc.signaling.protocol.Identity.SessionKey;
import io.webrtc.signaling.storage.SessionRepository;
import io.netty.channel.Channel;
import io.netty.util.AttributeKey;
import java.util.*;
import java.util.concurrent.RejectedExecutionException;
/** One local index per process. Closing an old channel cannot remove a replacement binding. */
public final class ConnectionRegistry {
    public static final AttributeKey<UUID> CONNECTION=AttributeKey.valueOf("signaling.connection");
    public record Binding(Channel channel,SessionRepository.Route route,AuthPrincipal principal) {}
    private final String gateway;private final UUID boot;private final int maxUnauthenticated,maxConnections;
    private final Map<UUID,Channel> channels=new HashMap<>();private final Map<UUID,Binding> bindings=new HashMap<>();private final Map<SessionKey,UUID> sessions=new HashMap<>();private int unauthenticated;
    public ConnectionRegistry(String gateway,UUID boot,int maxUnauthenticated){this(gateway,boot,maxUnauthenticated,200_000);}
    public ConnectionRegistry(String gateway,UUID boot,int maxUnauthenticated,int maxConnections){this.gateway=Objects.requireNonNull(gateway);this.boot=Objects.requireNonNull(boot);if(maxUnauthenticated<1||maxUnauthenticated>1000||maxConnections<maxUnauthenticated)throw new IllegalArgumentException("Bounded connection admission required");this.maxUnauthenticated=maxUnauthenticated;this.maxConnections=maxConnections;}
    public synchronized UUID attach(Channel channel){if(channels.size()>=maxConnections||unauthenticated>=maxUnauthenticated)throw new RejectedExecutionException("SOCKET_OVERLOADED");UUID id=UUID.randomUUID();channels.put(id,channel);unauthenticated++;channel.attr(CONNECTION).set(id);channel.closeFuture().addListener(f->remove(id));return id;}
    public synchronized boolean bind(UUID id,SessionRepository.Route route,AuthPrincipal principal){Channel channel=channels.get(id);if(channel==null||!channel.isActive()||GatewayRejection.rejecting(channel)||!route.connectionId().equals(id)||!gateway.equals(route.gatewayId())||!boot.equals(route.bootId())||!principal.key().equals(route.key())||!principal.userId().equals(route.user())||route.connectionGeneration()<1||!route.tokenExpiresAt().equals(principal.expiresAt()))return false;
        UUID previousId=sessions.get(route.key());Binding previous=bindings.get(previousId);if(previous!=null&&previous.route().incarnation().equals(route.incarnation())&&previous.route().connectionGeneration()>route.connectionGeneration())return false;
        if(!bindings.containsKey(id))unauthenticated--;bindings.put(id,new Binding(channel,route,principal));sessions.put(route.key(),id);
        if(previous!=null&&!Objects.equals(id,previousId))previous.channel().eventLoop().execute(()->GatewayRejection.close(previous.channel(),GatewayRejection.Reason.STALE_CONNECTION));return true;
    }
    public synchronized Binding binding(UUID id){return bindings.get(id);}
    public synchronized boolean current(UUID id,SessionRepository.Route route){var binding=bindings.get(id);return binding!=null&&binding.channel().isActive()&&!GatewayRejection.rejecting(binding.channel())&&binding.route().equals(route)&&id.equals(sessions.get(route.key()));}
    public synchronized void remove(UUID id){if(channels.remove(id)==null)return;var binding=bindings.remove(id);if(binding==null)unauthenticated--;else sessions.remove(binding.route().key(),id);}
    public synchronized int authenticatedCount(){return bindings.size();}
    public synchronized int unauthenticatedCount(){return unauthenticated;}
    public synchronized List<Binding> snapshot(){return List.copyOf(bindings.values());}
    /** Detach at most one fixed reconnect batch; native routes are closed by channelInactive. */
    public synchronized List<Channel> detachBatch(int maximum){
        if(maximum<1||maximum>128)throw new IllegalArgumentException("Reconnect batch outside bound");
        var ids=new ArrayList<UUID>(maximum);var iterator=channels.keySet().iterator();
        while(iterator.hasNext()&&ids.size()<maximum)ids.add(iterator.next());
        var selected=new ArrayList<Channel>(ids.size());
        for(var id:ids){selected.add(channels.get(id));remove(id);}
        return List.copyOf(selected);
    }
    public synchronized int connectionCount(){return channels.size();}
}
