package com.eventplatform.producer.service;

import com.eventplatform.common.model.Event;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

@Service
public class EventPublisherService {

    private static final Logger log   = LoggerFactory.getLogger(EventPublisherService.class);
    public  static final String TOPIC = "events-raw";

    private final KafkaTemplate<String, Event> kafkaTemplate;
    private final Counter publishedCounter;
    private final Counter failedCounter;
    private final Timer   publishTimer;

    public EventPublisherService(KafkaTemplate<String, Event> kafkaTemplate,
                                  MeterRegistry registry) {
        this.kafkaTemplate      = kafkaTemplate;
        this.publishedCounter   = Counter.builder("kafka.events.published")
            .description("Total events published to Kafka")
            .tag("topic", TOPIC)
            .register(registry);
        this.failedCounter      = Counter.builder("kafka.events.failed")
            .description("Total event publish failures")
            .tag("topic", TOPIC)
            .register(registry);
        this.publishTimer       = Timer.builder("kafka.publish.duration")
            .description("Kafka publish latency")
            .register(registry);
    }

    public CompletableFuture<SendResult<String, Event>> publish(Event event) {
        return publishTimer.record(() -> {
            log.info("Publishing event id={} type={}", event.getId(), event.getType());
            return kafkaTemplate.send(TOPIC, event.getId(), event)
                .thenApply(result -> {
                    publishedCounter.increment();
                    log.debug("Event {} → partition={} offset={}",
                        event.getId(),
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset());
                    return result;
                })
                .exceptionally(ex -> {
                    failedCounter.increment();
                    log.error("Failed to publish event {}: {}", event.getId(), ex.getMessage());
                    throw new RuntimeException("Kafka publish failed", ex);
                });
        });
    }

    public void publishSync(Event event) {
        try {
            publish(event).get();
        } catch (Exception e) {
            throw new RuntimeException("Kafka publish failed synchronously", e);
        }
    }
}
