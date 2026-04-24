package com.eventplatform.consumer.service;

import com.eventplatform.common.model.Alert;
import com.eventplatform.common.model.EventAggregate;
import com.eventplatform.consumer.repository.EventAggregateRepository;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

/**
 * Consumes events-processed and events-alerts topics.
 * Stores results for the query API and fires Prometheus metrics.
 */
@Service
public class EventConsumerService {

    private static final Logger log = LoggerFactory.getLogger(EventConsumerService.class);

    private final EventAggregateRepository repository;
    private final Counter aggregatesConsumed;
    private final Counter alertsConsumed;
    private final Counter consumerErrors;

    public EventConsumerService(EventAggregateRepository repository, MeterRegistry registry) {
        this.repository          = repository;
        this.aggregatesConsumed  = Counter.builder("consumer.aggregates.consumed.total")
            .description("Total EventAggregate records consumed from Kafka")
            .register(registry);
        this.alertsConsumed      = Counter.builder("consumer.alerts.consumed.total")
            .description("Total Alert records consumed from Kafka")
            .register(registry);
        this.consumerErrors      = Counter.builder("consumer.errors.total")
            .description("Total deserialization/processing errors")
            .register(registry);

        // Gauge: current number of tracked event types
        Gauge.builder("consumer.aggregates.tracked",
            repository, EventAggregateRepository::getAggregateCount)
            .description("Number of distinct event types tracked")
            .register(registry);
    }

    @KafkaListener(
        topics         = "events-processed",
        groupId        = "event-consumer-group",
        containerFactory = "aggregateListenerContainerFactory"
    )
    public void consumeAggregate(
            @Payload EventAggregate aggregate,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset) {
        try {
            log.info("Consumed aggregate: type={} count={} partition={} offset={}",
                aggregate.getEventType(), aggregate.getCount(), partition, offset);
            repository.saveAggregate(aggregate);
            aggregatesConsumed.increment();
        } catch (Exception e) {
            consumerErrors.increment();
            log.error("Error processing aggregate from partition={} offset={}: {}",
                partition, offset, e.getMessage());
        }
    }

    @KafkaListener(
        topics         = "events-alerts",
        groupId        = "alert-consumer-group",
        containerFactory = "alertListenerContainerFactory"
    )
    public void consumeAlert(
            @Payload Alert alert,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset) {
        try {
            log.warn("ALERT received: type={} count={} severity={} partition={} offset={}",
                alert.getEventType(), alert.getCount(), alert.getSeverity(), partition, offset);
            repository.saveAlert(alert);
            alertsConsumed.increment();
        } catch (Exception e) {
            consumerErrors.increment();
            log.error("Error processing alert from partition={} offset={}: {}",
                partition, offset, e.getMessage());
        }
    }
}
