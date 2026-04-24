package com.eventplatform.consumer.repository;

import com.eventplatform.common.model.Alert;
import com.eventplatform.common.model.EventAggregate;
import org.springframework.stereotype.Repository;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;

/**
 * In-memory store for processed event aggregates and alerts.
 * Thread-safe — written by Kafka listener, read by REST controller.
 *
 * In production: replace with a time-series DB (InfluxDB, TimescaleDB)
 * or a Redis sorted set for TTL-based retention.
 */
@Repository
public class EventAggregateRepository {

    // Latest aggregate per event type
    private final Map<String, EventAggregate> latestAggregates = new ConcurrentHashMap<>();

    // Recent alerts (capped at 1000)
    private final List<Alert> recentAlerts = new CopyOnWriteArrayList<>();

    private static final int MAX_ALERTS = 1000;

    public void saveAggregate(EventAggregate aggregate) {
        latestAggregates.put(aggregate.getEventType(), aggregate);
    }

    public Optional<EventAggregate> findByType(String eventType) {
        return Optional.ofNullable(latestAggregates.get(eventType));
    }

    public List<EventAggregate> findAll() {
        return new ArrayList<>(latestAggregates.values());
    }

    public void saveAlert(Alert alert) {
        if (recentAlerts.size() >= MAX_ALERTS) {
            recentAlerts.remove(0);
        }
        recentAlerts.add(alert);
    }

    public List<Alert> findAlerts(int limit) {
        List<Alert> all = new ArrayList<>(recentAlerts);
        Collections.reverse(all);
        return all.stream().limit(limit).collect(Collectors.toList());
    }

    public List<Alert> findAlertsByType(String eventType) {
        return recentAlerts.stream()
            .filter(a -> eventType.equals(a.getEventType()))
            .collect(Collectors.toList());
    }

    public int getAggregateCount() {
        return latestAggregates.size();
    }

    public int getAlertCount() {
        return recentAlerts.size();
    }

    public void clear() {
        latestAggregates.clear();
        recentAlerts.clear();
    }
}
