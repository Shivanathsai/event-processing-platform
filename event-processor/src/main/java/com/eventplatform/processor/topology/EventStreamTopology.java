package com.eventplatform.processor.topology;

import java.time.Duration;
import java.time.Instant;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Suppressed;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.eventplatform.common.model.Alert;
import com.eventplatform.common.model.Event;
import com.eventplatform.common.model.EventAggregate;
import com.eventplatform.common.serialization.JsonSerde;

/**
 * Kafka Streams topology for real-time event processing.
 *
 * Topology:
 *   events-raw
 *     ├── filter valid events (null/malformed → DLQ)
 *     ├── tumbling window (60s) → count by event type
 *     │     ├── → events-processed  (EventAggregate per window)
 *     │     └── → events-alerts     (Alert when count > threshold)
 *     └── events-dlq               (unparseable / invalid events)
 */
@Configuration
public class EventStreamTopology {

    private static final Logger log = LoggerFactory.getLogger(EventStreamTopology.class);

    public static final String TOPIC_RAW       = "events-raw";
    public static final String TOPIC_PROCESSED = "events-processed";
    public static final String TOPIC_ALERTS    = "events-alerts";
    public static final String TOPIC_DLQ       = "events-dlq";

    @Value("${processor.window.duration-seconds:60}")
    private long windowDurationSeconds;

    @Value("${processor.alert.threshold:100}")
    private long alertThreshold;

    @Bean
    @SuppressWarnings("unchecked")
    public KStream<String, Event> eventStream(StreamsBuilder builder) {

        JsonSerde<Event>          eventSerde     = new JsonSerde<>(Event.class);
        JsonSerde<EventAggregate> aggregateSerde = new JsonSerde<>(EventAggregate.class);
        JsonSerde<Alert>          alertSerde     = new JsonSerde<>(Alert.class);

        // ── Source stream ──────────────────────────────────────────────────
        KStream<String, Event> rawStream = builder.stream(
            TOPIC_RAW,
            Consumed.with(Serdes.String(), eventSerde)
                .withOffsetResetPolicy(Topology.AutoOffsetReset.EARLIEST)
        );

        // ── Branch: valid events vs DLQ ───────────────────────────────────
        KStream<String, Event>[] branches = rawStream.branch(
            (key, event) -> event != null
                         && event.getType() != null
                         && !event.getType().isBlank(),
            (key, event) -> true
        );

        KStream<String, Event> validStream = branches[0];
        KStream<String, Event> dlqStream   = branches[1];

        dlqStream
            .mapValues(event -> event != null
                ? event : new Event("unknown", "dlq", null))
            .to(TOPIC_DLQ, Produced.with(Serdes.String(), eventSerde));

        // ── Rekey by event type ───────────────────────────────────────────
        KStream<String, Event> rekeyedStream = validStream
            .selectKey((key, event) -> event.getType());

        // ── Tumbling window: count events per type ────────────────────────
        Duration windowSize = Duration.ofSeconds(windowDurationSeconds);

        KTable<Windowed<String>, Long> countTable = rekeyedStream
            .groupByKey(Grouped.with(Serdes.String(), eventSerde))
            .windowedBy(TimeWindows.ofSizeWithNoGrace(windowSize))
            .count(Materialized.as("event-count-store"))
            .suppress(Suppressed.untilWindowCloses(
                Suppressed.BufferConfig.unbounded()));

        // ── Emit aggregates to events-processed ──────────────────────────
        KStream<String, EventAggregate> aggregateStream = countTable
            .toStream()
            .map((windowedKey, count) -> {
                String  eventType = windowedKey.key();
                Instant winStart  = Instant.ofEpochMilli(windowedKey.window().start());
                Instant winEnd    = Instant.ofEpochMilli(windowedKey.window().end());
                EventAggregate agg = new EventAggregate(
                    eventType, count, winStart, winEnd);
                log.info("Window closed: type={} count={} window=[{}, {}]",
                    eventType, count, winStart, winEnd);
                return KeyValue.pair(eventType, agg);
            });

        aggregateStream.to(
            TOPIC_PROCESSED,
            Produced.with(Serdes.String(), aggregateSerde));

        // ── Anomaly detection: fire alert when count > threshold ──────────
        aggregateStream
            .filter((type, agg) -> agg.getCount() > alertThreshold)
            .map((type, agg) -> {
                Alert alert = new Alert(
                    type, agg.getCount(), alertThreshold);
                log.warn("ALERT: type={} count={} threshold={} severity={}",
                    type, agg.getCount(), alertThreshold, alert.getSeverity());
                return KeyValue.pair(type, alert);
            })
            .to(TOPIC_ALERTS,
                Produced.with(Serdes.String(), alertSerde));

        return validStream;
    }
}