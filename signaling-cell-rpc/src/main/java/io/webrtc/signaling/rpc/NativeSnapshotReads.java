package io.webrtc.signaling.rpc;
import io.webrtc.signaling.protocol.*;
import io.webrtc.signaling.protocol.Identity.CommandScope;
import io.webrtc.signaling.storage.*;
import java.time.Duration;
import java.util.Objects;
/** Auth-only primary reads retain their independent database cleanup handle across RPC. */
public final class NativeSnapshotReads implements RpcBusinessHandler.SnapshotReads {
    private final CallCommandService commands;
    public NativeSnapshotReads(CallCommandService commands){this.commands=Objects.requireNonNull(commands);}
    public RpcOperation<CallSnapshotRepository.Snapshot> read(RpcBusinessHandler.SyncRead request,Duration budget){
        var command=new CallCommand(SignalEnvelope.Type.SYNC_CALL,request.sender(),request.request(),request.call(),CommandScope.call(request.call()),null,null,null,"{}",request.intentHash());
        var operation=commands.loadCallSnapshotAuthorized(command,request.proof(),budget);return new RpcOperation<>(operation.logical(),operation.physicalCompletion());
    }
    public RpcOperation<java.util.Optional<CallCommandService.Outcome>> result(CallCommand request,String proof,Duration budget){
        var operation=commands.getCommandResultAuthorized(request,proof,budget);return new RpcOperation<>(operation.logical(),operation.physicalCompletion());
    }

}
