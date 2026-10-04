package io.webrtc.signaling.storage;
import java.util.*;
import java.util.concurrent.CompletionStage;
public interface GroupOwnership {
    DbOperation<Optional<GroupOwnerRepository.Grant>> acquireTracked(int group,String node,UUID incarnation,UUID operation);
    DbOperation<GroupOwnerRepository.Grant> pulseTracked(GroupOwnerRepository.Grant grant,long sequence,UUID operation);
    DbOperation<Boolean> releaseTracked(AuthoritySql.GroupToken token);
    DbOperation<Optional<GroupOwnerRepository.Grant>> reconcileTracked(int group,String node,UUID incarnation);
    CompletionStage<Optional<GroupOwnerRepository.Grant>> reconcile(int group,String node,UUID incarnation);
}
