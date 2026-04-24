package com.eventplatform.producer.controller;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.eventplatform.common.model.Event;
import com.eventplatform.producer.dto.EventRequest;
import com.eventplatform.producer.service.EventPublisherService;

import jakarta.validation.Valid;

/**
 * REST API for publishing events to the Kafka events-raw topic.
 *
 * POST /api/v1/events        - publish single event
 * POST /api/v1/events/batch  - publish up to 500 events
 * GET  /api/v1/events/health - service health
 */
@RestController
@RequestMapping("/api/v1/events")
public class EventController {

    private static final Logger log = LoggerFactory.getLogger(EventController.class);

    private final EventPublisherService publisherService;

    public EventController(EventPublisherService publisherService) {
        this.publisherService = publisherService;
    }

    @PostMapping
    public CompletableFuture<ResponseEntity<Map<String, Object>>> publish(
            @RequestBody @Valid EventRequest request) {

        Event event = new Event(request.getType(), request.getSource(), request.getPayload());

        return publisherService.publish(event)
            .thenApply(result -> {
                Map<String, Object> body = new HashMap<>();
                body.put("id",        event.getId());
                body.put("type",      event.getType());
                body.put("status",    "published");
                body.put("topic",     EventPublisherService.TOPIC);
                body.put("partition", result.getRecordMetadata().partition());
                body.put("offset",    result.getRecordMetadata().offset());
                return ResponseEntity.<Map<String, Object>>ok(body);
            })
            .exceptionally(ex -> {
                Map<String, Object> err = new HashMap<>();
                err.put("error",   "publish_failed");
                err.put("message", ex.getMessage());
                return ResponseEntity.<Map<String, Object>>internalServerError().body(err);
            });
    }

    @PostMapping("/batch")
    public ResponseEntity<Map<String, Object>> publishBatch(
            @RequestBody @Valid List<EventRequest> requests) {

        if (requests.isEmpty() || requests.size() > 500) {
            Map<String, Object> err = new HashMap<>();
            err.put("error", "Batch size must be between 1 and 500");
            return ResponseEntity.badRequest().body(err);
        }

        requests.forEach(req -> {
            Event event = new Event(req.getType(), req.getSource(), req.getPayload());
            publisherService.publish(event);
        });

        log.info("Batch published {} events", requests.size());
        Map<String, Object> body = new HashMap<>();
        body.put("published", requests.size());
        body.put("topic",     EventPublisherService.TOPIC);
        body.put("status",    "accepted");
        return ResponseEntity.ok(body);
    }

    @GetMapping("/health")
    public ResponseEntity<Map<String, String>> health() {
        Map<String, String> body = new HashMap<>();
        body.put("status",  "UP");
        body.put("service", "event-producer");
        return ResponseEntity.ok(body);
    }
}