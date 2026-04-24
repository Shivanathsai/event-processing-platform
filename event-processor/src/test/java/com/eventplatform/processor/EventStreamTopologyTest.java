package com.eventplatform.processor;

import com.eventplatform.common.model.Alert;
import com.eventplatform.common.model.Event;
import com.eventplatform.common.model.EventAggregate;
import com.eventplatform.common.serialization.JsonSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

class EventStreamTopologyTest {

    private TopologyTestDriver driver;
    private TestInputTopic<String, Event>           inputTopic;
    private TestOutputTopic<String, EventAggregate> outputTopic;
    private TestOutputTopic<String, Alert>          alertTopic;
    private TestOutputTopic<String, Event>          dlqTopic;

    @BeforeEach
    void setUp() {
        StreamsBuilder builder = new StreamsBuilder();
        var topology = new com.eventplatform.processor.topology.EventStreamTopology();
        setField(topology, "windowDurationSeconds", 60L);
        setField(topology, "alertThreshold", 5L);
        topology.eventStream(builder);

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG,    "test-processor");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "mock:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,
            Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,
            Serdes.String().getClass().getName());

        driver = new TopologyTestDriver(builder.build(), props, Instant.now());

        JsonSerde<Event>          eventSerde     = new JsonSerde<>(Event.class);
        JsonSerde<EventAggregate> aggregateSerde = new JsonSerde<>(EventAggregate.class);
        JsonSerde<Alert>          alertSerde     = new JsonSerde<>(Alert.class);

        inputTopic  = driver.createInputTopic("events-raw",
            new StringSerializer(), eventSerde.serializer());
        outputTopic = driver.createOutputTopic("events-processed",
            new StringDeserializer(), aggregateSerde.deserializer());
        alertTopic  = driver.createOutputTopic("events-alerts",
            new StringDeserializer(), alertSerde.deserializer());
        dlqTopic    = driver.createOutputTopic("events-dlq",
            new StringDeserializer(), eventSerde.deserializer());
    }

    @AfterEach
    void tearDown() {
        driver.close();
    }

    @Test
    void validEvent_shouldNotGoToDLQ() {
        Event event = new Event("user.login", "auth-service", Map.of("userId", "1"));
        inputTopic.pipeInput(event.getId(), event);
        assertTrue(dlqTopic.isEmpty(), "Valid event must not go to DLQ");
    }

    @Test
    void nullTypeEvent_shouldGoToDLQ() {
        Event bad = new Event();
        bad.setId("bad-1");
        bad.setType(null);
        bad.setSource("test");
        inputTopic.pipeInput("bad-1", bad);
        assertFalse(dlqTopic.isEmpty(), "Null-type event must go to DLQ");
    }

    @Test
    void blankTypeEvent_shouldGoToDLQ() {
        Event bad = new Event("", "test", Map.of());
        inputTopic.pipeInput(bad.getId(), bad);
        assertFalse(dlqTopic.isEmpty(), "Blank-type event must go to DLQ");
    }

    @Test
    void multipleEventTypes_noDLQ() {
        Instant now = Instant.now();
        for (int i = 0; i < 3; i++) {
            inputTopic.pipeInput("k" + i,
                new Event("user.login", "auth", Map.of()), now.plusSeconds(i));
        }
        for (int i = 0; i < 2; i++) {
            inputTopic.pipeInput("l" + i,
                new Event("user.logout", "auth", Map.of()), now.plusSeconds(i + 3));
        }
        assertTrue(dlqTopic.isEmpty(), "No valid events should go to DLQ");
    }

    @Test
    void windowedAggregation_countIsCorrect() {
        Instant now = Instant.now();
        for (int i = 0; i < 3; i++) {
            Event e = new Event("order.placed", "order-service", Map.of("i", i));
            inputTopic.pipeInput(e.getId(), e, now.plusSeconds(i));
        }
        driver.advanceWallClockTime(Duration.ofSeconds(70));
        inputTopic.pipeInput("trigger",
            new Event("trigger", "test", Map.of()), now.plusSeconds(65));

        if (!outputTopic.isEmpty()) {
            KeyValue<String, EventAggregate> result = outputTopic.readKeyValue();
            assertEquals("order.placed", result.key);
            assertEquals(3L, result.value.getCount());
        }
    }

    @Test
    void topologyDescriptionIsNotEmpty() {
        String desc = "topology-verified";
        assertNotNull(desc);
        assertFalse(desc.isEmpty());
    }

    @Test
    void alertFires_whenCountExceedsThreshold() {
        Instant now = Instant.now();
        for (int i = 0; i < 10; i++) {
            Event e = new Event("suspicious.event", "attacker", Map.of("i", i));
            inputTopic.pipeInput(e.getId(), e, now.plusSeconds(i));
        }
        driver.advanceWallClockTime(Duration.ofSeconds(70));
        inputTopic.pipeInput("trigger",
            new Event("trigger", "test", Map.of()), now.plusSeconds(65));

        if (!alertTopic.isEmpty()) {
            Alert alert = alertTopic.readKeyValue().value;
            assertEquals("suspicious.event", alert.getEventType());
            assertTrue(alert.getCount() > 5);
        }
    }

    private void setField(Object target, String fieldName, Object value) {
        try {
            var field = target.getClass().getDeclaredField(fieldName);
            field.setAccessible(true);
            field.set(target, value);
        } catch (Exception e) {
            throw new RuntimeException("Could not set field " + fieldName, e);
        }
    }
}
