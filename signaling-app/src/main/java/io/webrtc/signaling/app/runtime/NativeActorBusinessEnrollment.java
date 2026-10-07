package io.webrtc.signaling.app.runtime;

import io.webrtc.signaling.auth.BoundedTokenVerifier;
import io.webrtc.signaling.auth.CallAuthorizationPolicy;
import io.webrtc.signaling.protocol.Identity.UserId;
import io.webrtc.signaling.rpc.HomeAuthorizationProof;
import io.webrtc.signaling.rpc.ProofBindings;
import io.webrtc.signaling.storage.HomeParticipationService;
import java.util.UUID;
import java.util.function.Function;
import org.apache.pekko.actor.typed.ActorSystem;

/** Explicit native process and business dependencies; security callbacks are supplied by Main. */
public record NativeActorBusinessEnrollment(ActorSystem<?> system, String cell, long storageEpoch, long routingEpoch,
        UUID podUid, HomeAuthorizationProof proofs, Function<UserId,ProofBindings.TrustedHome> homes,
        HomeParticipationService.EpochAdoptionVerifier epochAdoption, BoundedTokenVerifier tokens,
        CallAuthorizationPolicy callingPolicy) {}
