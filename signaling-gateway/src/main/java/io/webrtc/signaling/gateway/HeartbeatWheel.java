package io.webrtc.signaling.gateway;
import io.netty.channel.Channel;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;
/** One scheduler and bounded wheel records for the process; no per-socket scheduled executor tasks. */
public final class HeartbeatWheel implements AutoCloseable {
    private record Key(UUID connection,int kind) {}
    private record Entry(Key key,Channel channel,long due) {}
    private final List<Map<Key,Entry>> buckets=new ArrayList<>(64);private final Map<Key,Integer> slots=new HashMap<>();private final Clock clock;private final ScheduledExecutorService scheduler=Executors.newSingleThreadScheduledExecutor(Thread.ofPlatform().daemon().name("gateway-heartbeat-wheel").factory());private final int maximum;
    public HeartbeatWheel(Clock clock,int maxConnections){this.clock=Objects.requireNonNull(clock);maximum=Math.multiplyExact(maxConnections,4);for(int i=0;i<64;i++)buckets.add(new HashMap<>());scheduler.scheduleAtFixedRate(this::advance,1,1,TimeUnit.SECONDS);}
    public void register(ConnectionRegistry.Binding binding){var channel=binding.channel();long now=clock.instant().getEpochSecond();long first=now+1+Math.floorMod(binding.route().connectionId().hashCode(),30);channel.pipeline().get(HeartbeatHandler.class).initialPing(Instant.ofEpochSecond(first));put(channel,0,first);put(channel,2,Math.max(now,binding.principal().expiresAt().getEpochSecond()-60));put(channel,3,binding.principal().expiresAt().getEpochSecond());channel.closeFuture().addListener(f->remove(binding.route().connectionId()));}
    private synchronized void put(Channel channel,int kind,long due){UUID id=channel.attr(ConnectionRegistry.CONNECTION).get();var key=new Key(id,kind);Integer previous=slots.remove(key);if(previous!=null)buckets.get(previous).remove(key);if(slots.size()>=maximum){channel.eventLoop().execute(channel::close);return;}int slot=Math.floorMod(due,64);buckets.get(slot).put(key,new Entry(key,channel,due));slots.put(key,slot);}
    private synchronized void remove(UUID id){for(int kind=0;kind<4;kind++){var key=new Key(id,kind);var slot=slots.remove(key);if(slot!=null)buckets.get(slot).remove(key);}}
    private void advance(){long now=clock.instant().getEpochSecond();List<Entry> due=new ArrayList<>();synchronized(this){for(var bucket:buckets){var iterator=bucket.entrySet().iterator();while(iterator.hasNext()){var item=iterator.next();if(item.getValue().due()<=now){due.add(item.getValue());slots.remove(item.getKey());iterator.remove();}}}}for(var entry:due)if(entry.channel().isActive())entry.channel().eventLoop().execute(()->{var heartbeat=entry.channel().pipeline().get(HeartbeatHandler.class);if(heartbeat==null)return;heartbeat.tick(clock.instant());if(entry.key().kind()==0&&entry.channel().isActive()){put(entry.channel(),0,now+30);put(entry.channel(),1,now+10);}});}
    public synchronized int retainedEvents(){return slots.size();}
    @Override public void close(){scheduler.shutdown();synchronized(this){slots.clear();buckets.forEach(Map::clear);}}
}
