package com.eventplatform.common.model;

import com.fasterxml.jackson.annotation.JsonFormat;

import java.time.Instant;

/**
 * Windowed aggregation result produced by Kafka Streams topology.
 * Written to topic "events-processed" after each tumbling window closes.
 */
public class EventAggregate {

    private String  eventType;
    private long    count;

    @JsonFormat(shape = JsonFormat.Shape.STRING)
    private Instant windowStart;

    @JsonFormat(shape = JsonFormat.Shape.STRING)
    private Instant windowEnd;

    @JsonFormat(shape = JsonFormat.Shape.STRING)
    private Instant computedAt;

    public EventAggregate() {}

    public EventAggregate(String eventType, long count, Instant windowStart, Instant windowEnd) {
        this.eventType   = eventType;
        this.count       = count;
        this.windowStart = windowStart;
        this.windowEnd   = windowEnd;
        this.computedAt  = Instant.now();
    }

    public String  getEventType()   { return eventType; }
    public long    getCount()       { return count; }
    public Instant getWindowStart() { return windowStart; }
    public Instant getWindowEnd()   { return windowEnd; }
    public Instant getComputedAt()  { return computedAt; }

    public void setEventType(String t)     { this.eventType = t; }
    public void setCount(long c)           { this.count = c; }
    public void setWindowStart(Instant ws) { this.windowStart = ws; }
    public void setWindowEnd(Instant we)   { this.windowEnd = we; }
    public void setComputedAt(Instant ca)  { this.computedAt = ca; }

    @Override
    public String toString() {
        return "EventAggregate{type='" + eventType + "', count=" + count +
               ", window=[" + windowStart + ", " + windowEnd + "]}";
    }
}
