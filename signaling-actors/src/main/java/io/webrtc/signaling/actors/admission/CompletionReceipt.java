package io.webrtc.signaling.actors.admission;

import io.webrtc.signaling.protocol.ApplicationSerializable;
import org.apache.pekko.actor.typed.ActorRef;

import java.util.Objects;
import java.util.UUID;

/**
 * A cleanup notification only; never business authority or a COMMIT acknowledgment.
 */
public record CompletionReceipt(UUID operation, ActorRef<PhysicalDone> recipient) {
    public CompletionReceipt {
        Objects.requireNonNull(operation);
        Objects.requireNonNull(recipient);
    }

    public void signal() {
        recipient.tell(new PhysicalDone(operation));
    }

    public record PhysicalDone(UUID operation) implements ApplicationSerializable {
        public PhysicalDone {
            Objects.requireNonNull(operation);
        }
    }

    public record PhysicalOwner(
            UUID operation, ActorRef<io.webrtc.signaling.actors.call.RecoveryWakeRegistry.Query> owner)
            implements ApplicationSerializable {
        public PhysicalOwner {
            Objects.requireNonNull(operation);
            Objects.requireNonNull(owner);
        }
    }
}
