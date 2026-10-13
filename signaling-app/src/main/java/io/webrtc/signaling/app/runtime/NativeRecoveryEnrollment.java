package io.webrtc.signaling.app.runtime;

/**
 * Explicit trusted fault source and stable privacy key. No environment fallback or public API.
 */
public record NativeRecoveryEnrollment(String sourceHash, String privacyKeyId, byte[] privacyKey) {
    public NativeRecoveryEnrollment {
        if (sourceHash == null
                || !sourceHash.matches("[a-f0-9]{64}")
                || privacyKeyId == null
                || !privacyKeyId.matches("[A-Za-z0-9_-]{1,64}")
                || privacyKey == null
                || privacyKey.length < 32
                || privacyKey.length > 64)
            throw new IllegalArgumentException("Recovery enrollment required");
        privacyKey = privacyKey.clone();
    }

    @Override
    public byte[] privacyKey() {
        return privacyKey.clone();
    }

    @Override
    public String toString() {
        return "NativeRecoveryEnrollment[redacted]";
    }
}
