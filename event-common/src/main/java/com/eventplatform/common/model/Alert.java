package com.eventplatform.common.model;

import com.fasterxml.jackson.annotation.JsonFormat;

import java.time.Instant;
import java.util.UUID;

/**
 * Alert fired when windowed event count exceeds configured threshold.
 * Written to topic "events-alerts" by the Kafka Streams anomaly detection branch.
 */
public class Alert {

    public enum Severity { LOW, MEDIUM, HIGH, CRITICAL }

    private String   id;
    private String   eventType;
    private long     count;
    private long     threshold;
    private Severity severity;
    private String   message;

    @JsonFormat(shape = JsonFormat.Shape.STRING)
    private Instant firedAt;

    public Alert() {}

    public Alert(String eventType, long count, long threshold) {
        this.id        = UUID.randomUUID().toString();
        this.eventType = eventType;
        this.count     = count;
        this.threshold = threshold;
        this.severity  = resolveSeverity(count, threshold);
        this.message   = String.format(
            "Event type '%s' rate %d exceeded threshold %d in window", eventType, count, threshold);
        this.firedAt   = Instant.now();
    }

    private static Severity resolveSeverity(long count, long threshold) {
        double ratio = (double) count / threshold;
        if (ratio > 3.0) return Severity.CRITICAL;
        if (ratio > 2.0) return Severity.HIGH;
        if (ratio > 1.5) return Severity.MEDIUM;
        return Severity.LOW;
    }

    public String   getId()        { return id; }
    public String   getEventType() { return eventType; }
    public long     getCount()     { return count; }
    public long     getThreshold() { return threshold; }
    public Severity getSeverity()  { return severity; }
    public String   getMessage()   { return message; }
    public Instant  getFiredAt()   { return firedAt; }

    public void setId(String id)          { this.id = id; }
    public void setEventType(String t)    { this.eventType = t; }
    public void setCount(long c)          { this.count = c; }
    public void setThreshold(long t)      { this.threshold = t; }
    public void setSeverity(Severity s)   { this.severity = s; }
    public void setMessage(String m)      { this.message = m; }
    public void setFiredAt(Instant f)     { this.firedAt = f; }
}
