package io.webrtc.signaling.app.runtime;

import io.webrtc.signaling.control.PostgresBucketTransfer;

import java.util.Objects;

/**
 * Both enrolled native primaries and their original physical owners are required.
 */
public record NativeBucketTransferEnrollment(
        PostgresBucketTransfer.Store source, PostgresBucketTransfer.Store target) {
    public NativeBucketTransferEnrollment {
        Objects.requireNonNull(source);
        Objects.requireNonNull(target);
        if (source.sql() == target.sql() || source.cell().equals(target.cell()))
            throw new IllegalArgumentException("Distinct bucket transfer authorities required");
    }
}
