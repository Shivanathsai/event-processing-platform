package com.eventplatform.producer.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Size;

import java.util.Map;

public class EventRequest {

    @NotBlank(message = "type is required")
    @Size(max = 100)
    @Pattern(regexp = "^[a-zA-Z0-9_.-]+$", message = "type must be alphanumeric")
    private String type;

    @NotBlank(message = "source is required")
    @Size(max = 100)
    private String source;

    @NotNull(message = "payload is required")
    private Map<String, Object> payload;

    public EventRequest() {}

    public EventRequest(String type, String source, Map<String, Object> payload) {
        this.type    = type;
        this.source  = source;
        this.payload = payload;
    }

    public String              getType()    { return type; }
    public String              getSource()  { return source; }
    public Map<String, Object> getPayload() { return payload; }

    public void setType(String type)                    { this.type = type; }
    public void setSource(String source)                { this.source = source; }
    public void setPayload(Map<String, Object> payload) { this.payload = payload; }
}
