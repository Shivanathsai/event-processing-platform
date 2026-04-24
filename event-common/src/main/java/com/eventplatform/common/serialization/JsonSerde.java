package com.eventplatform.common.serialization;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

/**
 * Generic JSON Serde for Kafka Streams topology.
 *
 * Usage:
 *   Serde<Event> eventSerde = new JsonSerde<>(Event.class);
 *   builder.stream("events-raw", Consumed.with(Serdes.String(), eventSerde));
 */
public class JsonSerde<T> implements Serde<T> {

    private static final ObjectMapper MAPPER = new ObjectMapper()
        .registerModule(new JavaTimeModule());

    private final Class<T> targetType;

    public JsonSerde(Class<T> targetType) {
        this.targetType = targetType;
    }

    @Override
    public Serializer<T> serializer() {
        return (topic, data) -> {
            if (data == null) return null;
            try {
                return MAPPER.writeValueAsBytes(data);
            } catch (Exception e) {
                throw new SerializationException("Error serializing " + targetType.getName(), e);
            }
        };
    }

    @Override
    public Deserializer<T> deserializer() {
        return (topic, data) -> {
            if (data == null) return null;
            try {
                return MAPPER.readValue(data, targetType);
            } catch (Exception e) {
                throw new SerializationException("Error deserializing " + targetType.getName(), e);
            }
        };
    }

    @Override public void configure(Map<String, ?> configs, boolean isKey) {}
    @Override public void close() {}
}
