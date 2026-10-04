package io.webrtc.signaling.actors.cluster;
import java.util.concurrent.atomic.AtomicReference;
public final class ClusterReadiness {
    public record Snapshot(boolean localUp,boolean regionsRegistered,boolean fingerprintValid,boolean cellActive,boolean safetyPoolUsable,boolean clockBoundValid,int upActors,int reachableAzCount,boolean draining) {
        public Snapshot{if(upActors<0||reachableAzCount<0)throw new IllegalArgumentException("Invalid membership counts");}
    }
    private final AtomicReference<Snapshot> state=new AtomicReference<>(new Snapshot(false,false,false,false,false,false,0,0,false));
    public void update(Snapshot snapshot){java.util.Objects.requireNonNull(snapshot);state.updateAndGet(previous->new Snapshot(snapshot.localUp(),snapshot.regionsRegistered(),snapshot.fingerprintValid(),snapshot.cellActive(),snapshot.safetyPoolUsable(),snapshot.clockBoundValid(),snapshot.upActors(),snapshot.reachableAzCount(),previous.draining()||snapshot.draining()));}
    public void beginDrain(){state.updateAndGet(s->new Snapshot(s.localUp(),s.regionsRegistered(),s.fingerprintValid(),s.cellActive(),s.safetyPoolUsable(),s.clockBoundValid(),s.upActors(),s.reachableAzCount(),true));}
    public Snapshot snapshot(){return state.get();}
    private static boolean safety(Snapshot s){return s.localUp()&&s.regionsRegistered()&&s.fingerprintValid()&&s.cellActive()&&s.safetyPoolUsable()&&s.clockBoundValid();}
    public boolean safetyReady(){return safety(state.get());}
    public boolean businessReady(){var s=state.get();return safety(s)&&!s.draining()&&s.upActors()>=4&&s.reachableAzCount()>=2;}
    void registered(){state.updateAndGet(s->new Snapshot(s.localUp(),true,s.fingerprintValid(),s.cellActive(),s.safetyPoolUsable(),s.clockBoundValid(),s.upActors(),s.reachableAzCount(),s.draining()));}
}
