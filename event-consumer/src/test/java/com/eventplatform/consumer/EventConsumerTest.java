package com.eventplatform.consumer;

import com.eventplatform.common.model.Alert;
import com.eventplatform.common.model.EventAggregate;
import com.eventplatform.consumer.controller.EventQueryController;
import com.eventplatform.consumer.repository.EventAggregateRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class EventConsumerTest {

    private EventAggregateRepository repository;
    private EventQueryController      controller;

    @BeforeEach
    void setUp() {
        repository = new EventAggregateRepository();
        controller = new EventQueryController(repository);
    }

    @Test
    void saveAndRetrieveAggregate() {
        EventAggregate agg = new EventAggregate(
            "user.login", 42L,
            Instant.now().minusSeconds(60), Instant.now()
        );
        repository.saveAggregate(agg);

        var result = controller.getAggregate("user.login");
        assertEquals(200, result.getStatusCode().value());
        EventAggregate body = (EventAggregate) result.getBody();
        assertNotNull(body);
        assertEquals(42L, body.getCount());
    }

    @Test
    void missingAggregate_returns404() {
        var result = controller.getAggregate("nonexistent.type");
        assertEquals(404, result.getStatusCode().value());
    }

    @Test
    void getAllAggregates_returnsAll() {
        repository.saveAggregate(new EventAggregate("type.a", 1L, Instant.now(), Instant.now()));
        repository.saveAggregate(new EventAggregate("type.b", 2L, Instant.now(), Instant.now()));

        var result = controller.getAllAggregates();
        assertEquals(200, result.getStatusCode().value());
        assertEquals(2, result.getBody().size());
    }

    @Test
    void saveAndRetrieveAlert() {
        Alert alert = new Alert("suspicious.event", 150L, 100L);
        repository.saveAlert(alert);

        var result = controller.getAlerts(10);
        assertEquals(200, result.getStatusCode().value());
        assertEquals(1, result.getBody().size());
        assertEquals("suspicious.event", result.getBody().get(0).getEventType());
    }

    @Test
    void alertSeverity_criticalWhenCountExceeds3xThreshold() {
        Alert alert = new Alert("ddos.attempt", 400L, 100L);
        assertEquals(Alert.Severity.CRITICAL, alert.getSeverity());
    }

    @Test
    void alertSeverity_highWhenCountExceeds2xThreshold() {
        Alert alert = new Alert("spam.event", 250L, 100L);
        assertEquals(Alert.Severity.HIGH, alert.getSeverity());
    }

    @Test
    void alertSeverity_mediumWhenCountExceeds1_5xThreshold() {
        Alert alert = new Alert("normal.spike", 160L, 100L);
        assertEquals(Alert.Severity.MEDIUM, alert.getSeverity());
    }

    @Test
    void getStats_returnsCorrectCounts() {
        repository.saveAggregate(new EventAggregate("type.x", 5L, Instant.now(), Instant.now()));
        repository.saveAlert(new Alert("type.x", 200L, 100L));

        var result = controller.getStats();
        assertEquals(200, result.getStatusCode().value());
        assertEquals(1, result.getBody().get("trackedEventTypes"));
        assertEquals(1, result.getBody().get("totalAlerts"));
    }

    @Test
    void health_returnsUp() {
        var result = controller.health();
        assertEquals(200, result.getStatusCode().value());
        assertEquals("UP", result.getBody().get("status"));
    }

    @Test
    void alertsByType_filtersCorrectly() {
        repository.saveAlert(new Alert("type.a", 200L, 100L));
        repository.saveAlert(new Alert("type.b", 300L, 100L));
        repository.saveAlert(new Alert("type.a", 250L, 100L));

        List<Alert> alerts = controller.getAlertsByType("type.a").getBody();
        assertNotNull(alerts);
        assertEquals(2, alerts.size());
        assertTrue(alerts.stream().allMatch(a -> "type.a".equals(a.getEventType())));
    }

    @Test
    void repository_capsAlertsAt1000() {
        for (int i = 0; i < 1010; i++) {
            repository.saveAlert(new Alert("spam", 200L, 100L));
        }
        assertTrue(repository.getAlertCount() <= 1000);
    }

    @Test
    void latestAggregate_overwritesPrevious() {
        repository.saveAggregate(new EventAggregate("event.x", 10L, Instant.now(), Instant.now()));
        repository.saveAggregate(new EventAggregate("event.x", 20L, Instant.now(), Instant.now()));

        var agg = repository.findByType("event.x");
        assertTrue(agg.isPresent());
        assertEquals(20L, agg.get().getCount());
    }
}
