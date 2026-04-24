package com.example.stateful.processor.serde;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

public final class JsonSerde<T> implements Serde<T> {

    private final ObjectMapper objectMapper;
    private final Class<T> type;

    public JsonSerde(ObjectMapper objectMapper, Class<T> type) {
        this.objectMapper = objectMapper;
        this.type = type;
    }

    @Override
    public Serializer<T> serializer() {
        return (topic, data) -> {
            try {
                return data == null ? null : objectMapper.writeValueAsBytes(data);
            } catch (Exception exception) {
                throw new IllegalStateException("Failed to serialize " + type.getSimpleName(), exception);
            }
        };
    }

    @Override
    public Deserializer<T> deserializer() {
        return (topic, data) -> {
            try {
                return data == null ? null : objectMapper.readValue(data, type);
            } catch (Exception exception) {
                throw new IllegalStateException("Failed to deserialize " + type.getSimpleName(), exception);
            }
        };
    }
}
