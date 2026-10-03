package io.webrtc.signaling.protocol;

import static io.webrtc.signaling.protocol.Identity.*;
import static io.webrtc.signaling.protocol.SignalEnvelope.Type.*;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.StreamReadConstraints;
import com.fasterxml.jackson.core.StreamReadFeature;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;

/** Strict bounded public decoder; transport aggregation must enforce the same bound before allocation. */
public final class ProtocolValidator {
    private static final Set<String> TOP = Set.of("v","type","requestId","callId","negotiationId","iceGeneration","payload");
    private final ProtocolLimits limits;
    private final ObjectMapper json;
    public ProtocolValidator(ProtocolLimits limits) {
        this.limits = java.util.Objects.requireNonNull(limits);
        this.json = new ObjectMapper(JsonFactory.builder()
            .enable(StreamReadFeature.STRICT_DUPLICATE_DETECTION)
            .streamReadConstraints(StreamReadConstraints.builder().maxNestingDepth(8)
                .maxStringLength(limits.frameBytes()).maxNumberLength(20).build()).build())
            .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS);
    }

    public SignalEnvelope decodePublic(byte[] frame) {
        if (frame == null || frame.length == 0 || frame.length > limits.frameBytes()) throw invalid();
        try {
            String utf8 = StandardCharsets.UTF_8.newDecoder()
                .onMalformedInput(java.nio.charset.CodingErrorAction.REPORT)
                .onUnmappableCharacter(java.nio.charset.CodingErrorAction.REPORT)
                .decode(java.nio.ByteBuffer.wrap(frame)).toString();
            JsonNode root = json.readTree(utf8);
            fields(root, TOP);
            if (!root.path("v").isIntegralNumber() || !root.path("v").canConvertToInt() || root.path("v").intValue()!=1)
                throw new ProtocolException(ErrorCode.UNSUPPORTED_PROTOCOL);
            var type = SignalEnvelope.Type.valueOf(string(root,"type",64));
            RequestId request = root.has("requestId") ? new RequestId(uuid(string(root,"requestId",36))) : null;
            CallId call = root.has("callId") ? new CallId(string(root,"callId",96)) : null;
            NegotiationId round = root.has("negotiationId") ? new NegotiationId(counter(root,"negotiationId")) : null;
            IceGeneration generation = root.has("iceGeneration") ? new IceGeneration(counter(root,"iceGeneration")) : null;
            if (type == AUTH || type == AUTH_REFRESH) {
                if (call != null || round != null || generation != null) throw invalid();
            } else if (request == null) throw invalid();
            if (type == INVITE && (call != null || round != null || generation != null)) throw invalid();
            if (type != INVITE && type != AUTH && type != AUTH_REFRESH && type != GET_COMMAND_RESULT && call == null) throw invalid();
            if ((type == OFFER || type == ANSWER || type == ICE_CANDIDATES || type == END_OF_CANDIDATES
                || type == MEDIA_CONNECTED || type == MEDIA_RECOVERED) && round == null) throw invalid();
            if ((type == ICE_CANDIDATES || type == END_OF_CANDIDATES) && generation == null) throw invalid();
            JsonNode payload = root.path("payload");
            validatePayload(type,payload);
            return new SignalEnvelope(1,type,request,call,round,generation,canonical(payload));
        } catch (ProtocolException e) { throw e; }
        catch (IOException | IllegalArgumentException e) { throw invalid(); }
    }

    /** Decode-only contract; mTLS, operation proofs and authority checks belong to RPC ingress. */
    public io.webrtc.signaling.protocol.internal.InternalCommand decodeInternal(byte[] encoded) {
        if (encoded == null || encoded.length == 0 || encoded.length > limits.envelopeBytes()) throw invalid();
        try {
            var command = io.webrtc.signaling.protocol.internal.InternalCommand.parseFrom(encoded);
            if (command.getSchemaMajor() != 1 || command.getSchemaMinor() < 0 || command.getSchemaMinor() > 1)
                throw new ProtocolException(ErrorCode.UNSUPPORTED_PROTOCOL);
            return command;
        } catch (com.google.protobuf.InvalidProtocolBufferException e) { throw invalid(); }
    }

    public CallCommand bind(SignalEnvelope envelope, AuthenticatedSession sender) {
        if (envelope.type() == AUTH || envelope.type() == AUTH_REFRESH) throw new ProtocolException(ErrorCode.UNAUTHENTICATED);
        // Revalidate arbitrary programmatic envelopes too; no bypass through public record construction.
        var node = json.createObjectNode().put("v",envelope.version()).put("type",envelope.type().name());
        if (envelope.requestId()!=null) node.put("requestId",envelope.requestId().value().toString());
        if (envelope.callId()!=null) node.put("callId",envelope.callId().value());
        if (envelope.negotiationId()!=null) node.put("negotiationId",envelope.negotiationId().decimal());
        if (envelope.iceGeneration()!=null) node.put("iceGeneration",envelope.iceGeneration().decimal());
        try { node.set("payload",json.readTree(envelope.payloadJson())); }
        catch (IOException e) { throw invalid(); }
        var checked = decodePublic(bytes(node));
        JsonNode payload;
        try { payload = json.readTree(checked.payloadJson()); }
        catch (IOException impossible) { throw invalid(); }
        UserId target = checked.type() == INVITE ? new UserId(payload.path("targetUserId").textValue()) : null;
        if (target != null && target.equals(sender.userId())) throw new ProtocolException(ErrorCode.SELF_CALL_NOT_ALLOWED);
        CommandScope scope = checked.callId() == null ? CommandScope.invite() : CommandScope.call(checked.callId());
        ObjectNode intent = json.createObjectNode().put("type",checked.type().name()).put("scope",scope.value())
            .put("issuer",sender.key().issuer()).put("jti",sender.key().jti()).put("userId",sender.userId().value());
        if (target != null) ((ObjectNode)payload).put("targetUserId",target.value());
        if (checked.negotiationId()!=null) intent.put("negotiationId",checked.negotiationId().decimal());
        if (checked.iceGeneration()!=null) intent.put("iceGeneration",checked.iceGeneration().decimal());
        intent.set("payload",payload);
        return new CallCommand(checked.type(),sender,checked.requestId(),checked.callId(),scope,target,
            checked.negotiationId(),checked.iceGeneration(),canonical(payload),sha256(canonical(intent)));
    }

    public void requireSameIntent(String recorded, String incoming) {
        if (recorded == null || incoming == null || !recorded.matches("[a-f0-9]{64}") || !incoming.matches("[a-f0-9]{64}")
            || !MessageDigest.isEqual(recorded.getBytes(StandardCharsets.US_ASCII),incoming.getBytes(StandardCharsets.US_ASCII)))
            throw new ProtocolException(ErrorCode.IDEMPOTENCY_CONFLICT);
    }

    private void validatePayload(SignalEnvelope.Type type, JsonNode payload) {
        switch (type) {
            case AUTH, AUTH_REFRESH -> {
                fields(payload,Set.of("token")); string(payload,"token",limits.jwtBytes());
            }
            case INVITE -> {
                fields(payload,Set.of("targetUserId")); new UserId(string(payload,"targetUserId",256));
            }
            case OFFER, ANSWER -> {
                fields(payload,Set.of("sdp")); string(payload,"sdp",limits.sdpBytes());
            }
            case ICE_CANDIDATES -> {
                fields(payload,Set.of("startSequence","candidates"));
                long start = counter(payload,"startSequence");
                JsonNode candidates = payload.path("candidates");
                if (!candidates.isArray() || candidates.isEmpty() || candidates.size()>limits.iceBatchCount()
                    || start > Long.MAX_VALUE-candidates.size() || bytes(sorted(payload)).length>limits.iceBatchBytes()) throw invalid();
                for (JsonNode c : candidates) {
                    fields(c,Set.of("candidate","sdpMid","sdpMLineIndex","usernameFragment"));
                    string(c,"candidate",limits.iceCandidateBytes());
                    boolean mid = c.hasNonNull("sdpMid"), index = c.hasNonNull("sdpMLineIndex");
                    if (!mid && !index) throw invalid();
                    if (mid) string(c,"sdpMid",256);
                    if (index && (!c.path("sdpMLineIndex").isIntegralNumber() || !c.path("sdpMLineIndex").canConvertToInt()
                        || c.path("sdpMLineIndex").intValue()<0 || c.path("sdpMLineIndex").intValue()>65535)) throw invalid();
                    if (c.has("usernameFragment")) string(c,"usernameFragment",256);
                }
            }
            case END_OF_CANDIDATES -> {
                fields(payload,Set.of("terminalSequence")); counter(payload,"terminalSequence");
            }
            case EVENT_RECEIVED -> {
                fields(payload,Set.of("eventId")); uuid(string(payload,"eventId",36));
            }
            case NEGOTIATE_REQUEST -> {
                fields(payload,Set.of("iceRestart"));
                if (!payload.path("iceRestart").isBoolean()) throw invalid();
            }
            case MEDIA_FAILED -> {
                fields(payload,Set.of("reason"));
                if (!Set.of("ICE_FAILED","TURN_UNAVAILABLE","STUN_UNAVAILABLE","RECOVERY_EXHAUSTED")
                    .contains(string(payload,"reason",64))) throw invalid();
            }
            default -> fields(payload,Set.of());
        }
    }
    private void fields(JsonNode value, Set<String> allowed) {
        if (!value.isObject()) throw invalid();
        value.fieldNames().forEachRemaining(k -> { if (!allowed.contains(k)) throw invalid(); });
    }
    private String string(JsonNode node, String field, int max) {
        var value = node.path(field);
        if (!value.isTextual() || value.textValue().isBlank()
            || value.textValue().getBytes(StandardCharsets.UTF_8).length>max
            || value.textValue().codePoints().anyMatch(c -> c >= 0xd800 && c <= 0xdfff)) throw invalid();
        return value.textValue();
    }
    private long counter(JsonNode node,String field) {
        String value = string(node,field,19);
        if (!value.matches("[1-9][0-9]*")) throw invalid();
        return Long.parseLong(value);
    }
    private UUID uuid(String value) {
        UUID parsed = UUID.fromString(value);
        if (!parsed.toString().equals(value)) throw invalid();
        return parsed;
    }
    private JsonNode sorted(JsonNode node) {
        if (node.isObject()) {
            var fields = new TreeMap<String,JsonNode>();
            node.fields().forEachRemaining(e -> fields.put(e.getKey(), sorted(e.getValue())));
            ObjectNode result = json.createObjectNode(); fields.forEach(result::set); return result;
        }
        if (node.isArray()) {
            var result = json.createArrayNode(); node.forEach(n -> result.add(sorted(n))); return result;
        }
        return node;
    }
    private byte[] bytes(JsonNode value) {
        try { return json.writeValueAsBytes(value); } catch (IOException e) { throw invalid(); }
    }
    private String canonical(JsonNode value) { return new String(bytes(sorted(value)),StandardCharsets.UTF_8); }
    private String sha256(String value) {
        try { return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(value.getBytes(StandardCharsets.UTF_8))); }
        catch (NoSuchAlgorithmException impossible) { throw new IllegalStateException("SHA-256 unavailable",impossible); }
    }
    private ProtocolException invalid() { return new ProtocolException(ErrorCode.INVALID_FRAME); }
}
