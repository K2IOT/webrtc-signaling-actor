package io.webrtc.signaling.rpc;

import com.fasterxml.jackson.core.*;
import com.fasterxml.jackson.databind.*;
import io.webrtc.signaling.protocol.CallCommand;
import io.webrtc.signaling.protocol.Identity.*;
import io.webrtc.signaling.protocol.PortableProofTime;
import io.webrtc.signaling.storage.*;
import java.security.*;
import java.time.*;
import java.util.*;

/** Separate auth-only proof namespace. It conveys no group tenure or active-call participation. */
public final class SessionAuthorizationProof {
  public record Claims(
      int schema,
      String destinationCell,
      CallId call,
      UUID operation,
      String intentHash,
      SessionRegistryService.SessionProofView nativeView) {
    public Claims {
      Objects.requireNonNull(call);
      Objects.requireNonNull(operation);
      Objects.requireNonNull(nativeView);
      var view = nativeView;
      if (schema != 1
          || !call.coordinatorCell().equals(destinationCell)
          || intentHash == null
          || !intentHash.matches("[0-9a-f]{64}")
          || view.route() == null
          || view.checkedAt() == null
          || view.proofUntil() == null
          || !view.proofUntil().isAfter(view.checkedAt())
          || view.proofUntil().isAfter(view.checkedAt().plusSeconds(5))
          || view.sourceStorageEpoch() < 1
          || view.directoryEpoch() < 1
          || view.sourceCell() == null
          || !view.sourceCell().matches("[a-z][a-z0-9-]{0,23}")
          || view.proofUntil().isAfter(view.route().tokenExpiresAt()))
        throw new IllegalArgumentException("Invalid auth-only proof");
    }

    @Override
    public String toString() {
      return "SessionProof[schema=" + schema + "]";
    }
  }

  private static final ObjectMapper JSON =
      new ObjectMapper(
              JsonFactory.builder()
                  .streamReadConstraints(
                      StreamReadConstraints.builder()
                          .maxNestingDepth(16)
                          .maxStringLength(4096)
                          .build())
                  .enable(StreamReadFeature.STRICT_DUPLICATE_DETECTION)
                  .build())
          .findAndRegisterModules()
          .enable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
  private final String cell, keyId;
  private final PrivateKey privateKey;
  private final Map<String, PublicKey> trusted;

  public SessionAuthorizationProof(
      String cell, String keyId, PrivateKey privateKey, Map<String, PublicKey> trusted) {
    if (cell == null
        || !cell.matches("[a-z][a-z0-9-]{0,23}")
        || keyId == null
        || !keyId.matches("[A-Za-z0-9_-]{1,64}")
        || trusted.size() > 256
        || !("EdDSA".equals(privateKey.getAlgorithm())
            || "Ed25519".equals(privateKey.getAlgorithm())))
      throw new IllegalArgumentException("Invalid proof key configuration");
    this.cell = cell;
    this.keyId = keyId;
    this.privateKey = Objects.requireNonNull(privateKey);
    this.trusted = Map.copyOf(trusted);
  }

  public String issue(SessionRegistryService.SessionProofView view, CallCommand command) {
    var route = view.route();
    if (!cell.equals(view.sourceCell())
        || command.callId() == null
        || !same(route, command.sender())) throw new AuthoritySql.FencedException();
    var claims =
        new Claims(
            1,
            command.callId().coordinatorCell(),
            command.callId(),
            command.requestId().value(),
            command.intentHash(),
            view);
    try {
      String unsigned =
          cell
              + "."
              + keyId
              + ".S1."
              + Base64.getUrlEncoder()
                  .withoutPadding()
                  .encodeToString(JSON.writeValueAsBytes(claims));
      var signer = Signature.getInstance("Ed25519");
      signer.initSign(privateKey);
      signer.update(unsigned.getBytes(java.nio.charset.StandardCharsets.US_ASCII));
      String signed =
          unsigned + "." + Base64.getUrlEncoder().withoutPadding().encodeToString(signer.sign());
      if (signed.length() > 4096) throw new IllegalArgumentException("Oversized session proof");
      return signed;
    } catch (GeneralSecurityException | java.io.IOException failure) {
      throw new IllegalArgumentException("Cannot sign native session proof", failure);
    }
  }

  public Optional<Claims> decode(String signed, String source, Instant now) {
    try {
      if (signed == null || signed.length() > 4096 || now == null) return Optional.empty();
      var pieces = signed.split("\\.", -1);
      if (pieces.length != 5
          || !pieces[0].equals(source)
          || !pieces[2].equals("S1")
          || !pieces[3].matches("[A-Za-z0-9_-]+")
          || !pieces[4].matches("[A-Za-z0-9_-]{86}")) return Optional.empty();
      var key = trusted.get(pieces[0] + "/" + pieces[1]);
      if (key == null) return Optional.empty();
      var verifier = Signature.getInstance("Ed25519");
      verifier.initVerify(key);
      verifier.update(
          String.join(".", Arrays.copyOf(pieces, 4))
              .getBytes(java.nio.charset.StandardCharsets.US_ASCII));
      if (!verifier.verify(Base64.getUrlDecoder().decode(pieces[4]))) return Optional.empty();
      var claims = JSON.readValue(Base64.getUrlDecoder().decode(pieces[3]), Claims.class);
      var view = claims.nativeView();
      if (!source.equals(view.sourceCell())
          || !PortableProofTime.valid(view.checkedAt(), view.proofUntil(), now))
        return Optional.empty();
      return Optional.of(claims);
    } catch (Exception invalid) {
      return Optional.empty();
    }
  }

  public boolean verify(
      String signed, CallCommand command, ProofBindings.TrustedHome home, Instant now) {
    if (home == null || command.callId() == null) return false;
    return decode(signed, home.cell(), now)
        .filter(
            p ->
                p.destinationCell().equals(command.callId().coordinatorCell())
                    && p.call().equals(command.callId())
                    && p.operation().equals(command.requestId().value())
                    && p.intentHash().equals(command.intentHash())
                    && p.nativeView().directoryEpoch() == home.directoryEpoch()
                    && p.nativeView().sourceStorageEpoch() == home.storageEpoch()
                    && same(p.nativeView().route(), command.sender()))
        .isPresent();
  }

  private static boolean same(SessionRepository.Route route, AuthenticatedSession sender) {
    return route.user().equals(sender.userId())
        && route.key().equals(sender.key())
        && route.incarnation().equals(sender.incarnation())
        && route.connectionGeneration() == sender.connectionGeneration()
        && route.connectionId().equals(sender.connectionId());
  }
}
