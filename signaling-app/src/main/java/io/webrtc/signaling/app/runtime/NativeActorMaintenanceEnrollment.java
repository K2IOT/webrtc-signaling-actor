package io.webrtc.signaling.app.runtime;

import io.webrtc.signaling.storage.OutboxRepository;
import io.webrtc.signaling.storage.worker.OutboxDispatcher;
import io.webrtc.signaling.storage.worker.PrivacyDeletionWorker;

import java.util.Objects;
import java.util.function.BiPredicate;

/**
 * Production adapters: client application receipts and authenticated privacy detachment.
 */
public record NativeActorMaintenanceEnrollment(
        OutboxDispatcher.Delivery delivery,
        BiPredicate<OutboxRepository.Claim, OutboxDispatcher.Receipt> authenticatedReceipt,
        byte[] privacyPseudonymKey,
        PrivacyDeletionWorker.Detachment detachment) {
    public NativeActorMaintenanceEnrollment(
            byte[] privacyPseudonymKey, PrivacyDeletionWorker.Detachment detachment) {
        this(null, null, privacyPseudonymKey, detachment);
    }

    public NativeActorMaintenanceEnrollment {
        // Legacy callbacks are retained as source-compatible enrollment fields only. Production
        // maintenance binds NativeOutboxDelivery and its exact-issued receipt verifier directly.
        Objects.requireNonNull(detachment);
        if (privacyPseudonymKey == null
                || privacyPseudonymKey.length < 32
                || privacyPseudonymKey.length > 64)
            throw new IllegalArgumentException("Enrolled privacy pseudonym key required");
        privacyPseudonymKey = privacyPseudonymKey.clone();
    }

    @Override
    public byte[] privacyPseudonymKey() {
        return privacyPseudonymKey.clone();
    }
}
