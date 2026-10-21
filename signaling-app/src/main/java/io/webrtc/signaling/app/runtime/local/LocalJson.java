package io.webrtc.signaling.app.runtime.local;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.StreamReadConstraints;
import com.fasterxml.jackson.core.StreamReadFeature;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

/** Bounded, strict reading shared by the profile loader and the contract failure diagnosis. */
final class LocalJson {
    static final int MAX_BYTES = 65536;

    /** Same strictness as the mounted contract parser. */
    static final ObjectMapper MAPPER =
            new ObjectMapper(
                    JsonFactory.builder()
                            .enable(StreamReadFeature.STRICT_DUPLICATE_DETECTION)
                            .streamReadConstraints(
                                    StreamReadConstraints.builder()
                                            .maxNestingDepth(8)
                                            .maxStringLength(8192)
                                            .maxNumberLength(20)
                                            .build())
                            .build())
                    .findAndRegisterModules()
                    .disable(MapperFeature.ALLOW_COERCION_OF_SCALARS)
                    .enable(
                            DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES,
                            DeserializationFeature.FAIL_ON_MISSING_CREATOR_PROPERTIES,
                            DeserializationFeature.FAIL_ON_TRAILING_TOKENS);

    private LocalJson() {
    }

    static String read(Path file, LocalRuntimeFailure.Role role) {
        byte[] bytes;
        try {
            if (!Files.isRegularFile(file)) throw failure(role, LocalRuntimeFailure.Category.MISSING);
            try (var input = Files.newInputStream(file)) {
                bytes = input.readNBytes(MAX_BYTES + 1);
            }
        } catch (IOException unreadable) {
            throw failure(role, LocalRuntimeFailure.Category.MISSING);
        }
        if (bytes.length > MAX_BYTES) throw failure(role, LocalRuntimeFailure.Category.TOO_LARGE);
        if (bytes.length == 0) throw failure(role, LocalRuntimeFailure.Category.MALFORMED);
        try {
            return StandardCharsets.UTF_8
                    .newDecoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .decode(ByteBuffer.wrap(bytes))
                    .toString();
        } catch (CharacterCodingException notUtf8) {
            throw failure(role, LocalRuntimeFailure.Category.MALFORMED);
        }
    }

    static JsonNode tree(String text, LocalRuntimeFailure.Role role) {
        try {
            var node = MAPPER.readTree(text);
            if (node == null || node.isMissingNode())
                throw failure(role, LocalRuntimeFailure.Category.MALFORMED);
            return node;
        } catch (JsonProcessingException invalid) {
            throw failure(
                    role,
                    invalid instanceof JsonParseException parse
                            && parse.getOriginalMessage() != null
                            && parse.getOriginalMessage().startsWith("Duplicate field")
                            ? LocalRuntimeFailure.Category.DUPLICATE_KEY
                            : LocalRuntimeFailure.Category.MALFORMED);
        }
    }

    static LocalRuntimeFailure failure(
            LocalRuntimeFailure.Role role, LocalRuntimeFailure.Category category) {
        return new LocalRuntimeFailure(role, category);
    }
}
