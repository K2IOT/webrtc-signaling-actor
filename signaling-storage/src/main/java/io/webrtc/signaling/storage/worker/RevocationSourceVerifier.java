package io.webrtc.signaling.storage.worker;

import io.webrtc.signaling.protocol.Identity.SessionKey;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.security.*;
import java.security.interfaces.EdECPublicKey;
import java.time.Instant;
import java.util.*;
import java.util.function.Predicate;

/** Enrolled source attests complete cell-filtered coverage. Native SQL independently enforces age/cursor order. */
public final class RevocationSourceVerifier implements Predicate<RevocationReconciler.Batch> {
    private final String cell, issuer;
    private final Map<String, PublicKey> keys;
    public RevocationSourceVerifier(String cell, String issuer, Map<String, PublicKey> keys) {
        if (cell == null || !cell.matches("[a-z][a-z0-9-]{0,23}") || keys == null || keys.isEmpty() || keys.size() > 256
                || keys.entrySet().stream().anyMatch(e -> e.getKey() == null || !e.getKey().matches("[A-Za-z0-9_-]{1,64}")
                    || !(e.getValue() instanceof EdECPublicKey key) || !key.getParams().getName().equals("Ed25519")))
            throw new IllegalArgumentException("Enrolled Ed25519 revocation source required");
        new SessionKey(issuer, "revocation-source"); this.cell = cell; this.issuer = issuer; this.keys = Map.copyOf(keys);
    }
    @Override public boolean test(RevocationReconciler.Batch batch) {
        if (batch == null || batch.events().stream().anyMatch(e -> !issuer.equals(e.issuer()))
                || batch.retiredKeys().stream().anyMatch(e -> !issuer.equals(e.issuer()))) return false;
        String proof = batch.sourceProof();
        if (proof.length() > 151) return false;
        var parts = proof.split("\\.", -1);
        if (parts.length != 2 || !parts[0].matches("[A-Za-z0-9_-]{1,64}") || !parts[1].matches("[A-Za-z0-9_-]{86}")) return false;
        var key = keys.get(parts[0]); if (key == null) return false;
        try {
            byte[] encoded = Base64.getUrlDecoder().decode(parts[1]);
            if (!Base64.getUrlEncoder().withoutPadding().encodeToString(encoded).equals(parts[1])) return false;
            var signature = Signature.getInstance("Ed25519"); signature.initVerify(key); signature.update(signingBytes(cell, issuer, batch));
            return signature.verify(encoded);
        } catch (GeneralSecurityException | RuntimeException invalid) { return false; }
    }
    public static byte[] signingBytes(String cell, String issuer, RevocationReconciler.Batch batch) {
        try {
            var bytes = new ByteArrayOutputStream(); var out = new DataOutputStream(bytes);
            text(out, "signaling-revocation-source-v1"); text(out, cell); text(out, issuer);
            out.writeLong(batch.fromOffset()); out.writeLong(batch.highWater()); out.writeLong(batch.currentSourceHighWater());
            time(out, batch.checkedAt()); out.writeInt(batch.events().size());
            for (var event : batch.events()) {
                text(out, event.issuer()); text(out, event.userId().value()); out.writeBoolean(event.jti() != null);
                if (event.jti() != null) text(out, event.jti());
                out.writeLong(event.epoch()); out.writeLong(event.sourceOffset()); time(out, event.committedAt());
            }
            out.writeInt(batch.retiredKeys().size());
            for (var key : batch.retiredKeys()) {
                text(out, key.issuer()); text(out, key.signingKeyId()); out.writeLong(key.sourceOffset()); time(out, key.committedAt());
            }
            out.flush(); return bytes.toByteArray();
        } catch (IOException impossible) { throw new IllegalStateException(impossible); }
    }
    private static void text(DataOutputStream out, String value) throws IOException { byte[] bytes = value.getBytes(StandardCharsets.UTF_8); out.writeInt(bytes.length); out.write(bytes); }
    private static void time(DataOutputStream out, Instant value) throws IOException { out.writeLong(value.getEpochSecond()); out.writeInt(value.getNano()); }
}
