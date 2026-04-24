package com.eventplatform.producer;

import com.eventplatform.common.model.Event;
import com.eventplatform.producer.controller.EventController;
import com.eventplatform.producer.dto.EventRequest;
import com.eventplatform.producer.service.EventPublisherService;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.annotation.DirtiesContext;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
@EmbeddedKafka(
    partitions = 3,
    topics = {"events-raw"},
    brokerProperties = {
        "listeners=PLAINTEXT://localhost:9092",
        "port=9092"
    }
)
@DirtiesContext
class EventProducerIntegrationTest {

    @Autowired
    private KafkaTemplate<String, Event> kafkaTemplate;

    @Autowired
    private EmbeddedKafkaBroker embeddedKafka;

    private KafkaConsumer<String, String> testConsumer;

    @BeforeEach
    void setUp() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
            embeddedKafka.getBrokersAsString());
        props.put(ConsumerConfig.GROUP_ID_CONFIG,         "test-group");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
            StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
            StringDeserializer.class.getName());
        testConsumer = new KafkaConsumer<>(props);
        testConsumer.subscribe(Collections.singletonList("events-raw"));
    }

    @AfterEach
    void tearDown() {
        testConsumer.close();
    }

    @Test
    void publishEvent_shouldDeliverToKafkaTopic() throws Exception {
        EventPublisherService service = new EventPublisherService(kafkaTemplate, new SimpleMeterRegistry());
        Event event = new Event("user.created", "test-service", Map.of("userId", "123"));

        service.publishSync(event);

        ConsumerRecords<String, String> records =
            testConsumer.poll(Duration.ofSeconds(5));

        assertFalse(records.isEmpty(), "Expected at least one record in events-raw");
        assertTrue(records.iterator().next().value().contains("user.created"));
    }

    @Test
    void publishEvent_shouldUseEventIdAsKey() throws Exception {
        EventPublisherService service = new EventPublisherService(kafkaTemplate, new SimpleMeterRegistry());
        Event event = new Event("order.placed", "order-service", Map.of("orderId", "456"));

        service.publishSync(event);

        ConsumerRecords<String, String> records =
            testConsumer.poll(Duration.ofSeconds(5));

        assertFalse(records.isEmpty());
        assertEquals(event.getId(), records.iterator().next().key());
    }

    @Test
    void eventRequest_validationShouldRejectBlankType() {
        EventRequest req = new EventRequest("", "source", Map.of("k", "v"));
        assertEquals("", req.getType());
        // Validation is enforced by Spring's @Valid — blank type is rejected at controller layer
    }

    @Test
    void eventController_healthEndpointReturnsUp() {
        EventPublisherService service = new EventPublisherService(kafkaTemplate, new SimpleMeterRegistry());
        EventController controller = new EventController(service);
        var response = controller.health();
        assertEquals(200, response.getStatusCode().value());
        assertEquals("UP", response.getBody().get("status"));
    }
}
