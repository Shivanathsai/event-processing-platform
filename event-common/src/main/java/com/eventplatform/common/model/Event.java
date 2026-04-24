package com.eventplatform.common.model;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

/**
 * Core domain event published to Kafka topic "events-raw".
 * Shared across all three microservices via event-common module.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class Event {

    private String id;
    private String type;
    private String source;
    private Map<String, Object> payload;

    @JsonFormat(shape = JsonFormat.Shape.STRING)
    private Instant timestamp;

    public Event() {}

    public Event(String type, String source, Map<String, Object> payload) {
        this.id        = UUID.randomUUID().toString();
        this.type      = type;
        this.source    = source;
        this.payload   = payload;
        this.timestamp = Instant.now();
    }

    public String getId()                   { return id; }
    public String getType()                 { return type; }
    public String getSource()               { return source; }
    public Map<String, Object> getPayload() { return payload; }
    public Instant getTimestamp()           { return timestamp; }

    public void setId(String id)                        { this.id = id; }
    public void setType(String type)                    { this.type = type; }
    public void setSource(String source)                { this.source = source; }
    public void setPayload(Map<String, Object> payload) { this.payload = payload; }
    public void setTimestamp(Instant timestamp)         { this.timestamp = timestamp; }

    @Override
    public String toString() {
        return "Event{id='" + id + "', type='" + type + "', source='" + source + "', ts=" + timestamp + "}";
    }
}
