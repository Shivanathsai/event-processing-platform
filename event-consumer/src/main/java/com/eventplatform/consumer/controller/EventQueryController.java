package com.eventplatform.consumer.controller;

import com.eventplatform.common.model.Alert;
import com.eventplatform.common.model.EventAggregate;
import com.eventplatform.consumer.repository.EventAggregateRepository;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

/**
 * Query API for processed event data.
 *
 * GET /api/v1/aggregates           — all latest aggregates
 * GET /api/v1/aggregates/{type}    — aggregate for specific event type
 * GET /api/v1/alerts               — recent alerts (default: last 50)
 * GET /api/v1/alerts/{type}        — alerts for specific event type
 * GET /api/v1/stats                — platform stats
 */
@RestController
@RequestMapping("/api/v1")
public class EventQueryController {

    private final EventAggregateRepository repository;

    public EventQueryController(EventAggregateRepository repository) {
        this.repository = repository;
    }

    @GetMapping("/aggregates")
    public ResponseEntity<List<EventAggregate>> getAllAggregates() {
        return ResponseEntity.ok(repository.findAll());
    }

    @GetMapping("/aggregates/{eventType}")
    public ResponseEntity<?> getAggregate(@PathVariable String eventType) {
        return repository.findByType(eventType)
            .map(ResponseEntity::ok)
            .orElse(ResponseEntity.notFound().build());
    }

    @GetMapping("/alerts")
    public ResponseEntity<List<Alert>> getAlerts(
            @RequestParam(defaultValue = "50") int limit) {
        return ResponseEntity.ok(repository.findAlerts(Math.min(limit, 500)));
    }

    @GetMapping("/alerts/{eventType}")
    public ResponseEntity<List<Alert>> getAlertsByType(@PathVariable String eventType) {
        return ResponseEntity.ok(repository.findAlertsByType(eventType));
    }

    @GetMapping("/stats")
    public ResponseEntity<Map<String, Object>> getStats() {
        return ResponseEntity.ok(Map.of(
            "trackedEventTypes", repository.getAggregateCount(),
            "totalAlerts",       repository.getAlertCount(),
            "status",            "running"
        ));
    }

    @GetMapping("/health")
    public ResponseEntity<Map<String, String>> health() {
        return ResponseEntity.ok(Map.of("status", "UP", "service", "event-consumer"));
    }
}
