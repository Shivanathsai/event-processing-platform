# Real-Time Event Processing Platform

Java-based microservices platform using **Kafka Streams** on **AWS EKS** with **Grafana** dashboards for end-to-end monitoring and alerting.

---

## Architecture

```
REST Client
    │
    ▼
event-producer (Spring Boot, port 8081)
    │  POST /api/v1/events → publishes to Kafka
    │
    ▼
Kafka Topic: events-raw (3 partitions)
    │
    ▼
event-processor (Kafka Streams)
    │  Tumbling window (60s) — count by event type
    │  Exactly-once semantics (PROCESSING_GUARANTEE=exactly_once_v2)
    │  Dead-letter queue for poison pills
    │
    ├──▶ events-processed  (EventAggregate per closed window)
    ├──▶ events-alerts      (Alert when count > threshold)
    └──▶ events-dlq         (null/malformed events)
    │
    ▼
event-consumer (Spring Boot, port 8083)
    │  @KafkaListener → in-memory store
    │  GET /api/v1/aggregates, /api/v1/alerts
    │
    ▼
Prometheus → Grafana (port 3000)
    │  Service availability, publish latency, consumer lag
    │  Alert rules → Alertmanager
```

---

## Services

| Service | Port | Responsibility |
|---|---|---|
| event-producer | 8081 | REST API → Kafka publisher |
| event-processor | 8082 | Kafka Streams topology |
| event-consumer | 8083 | Kafka consumer + query API |
| Prometheus | 9090 | Metrics collection |
| Grafana | 3000 | Dashboards + alerting |
| Alertmanager | 9093 | Alert routing |

---

## Quick Start

```bash
# Build all modules
mvn clean package -DskipTests

# Start everything (Kafka + 3 services + monitoring)
docker compose up -d

# Wait for services to be healthy (~30s)
curl http://localhost:8081/api/v1/events/health
curl http://localhost:8082/actuator/health
curl http://localhost:8083/api/v1/health

# Publish an event
curl -X POST http://localhost:8081/api/v1/events \
  -H "Content-Type: application/json" \
  -d '{"type":"user.login","source":"demo","payload":{"userId":"123"}}'

# Publish a batch
curl -X POST http://localhost:8081/api/v1/events/batch \
  -H "Content-Type: application/json" \
  -d '[{"type":"order.placed","source":"demo","payload":{"orderId":"1"}},
       {"type":"order.placed","source":"demo","payload":{"orderId":"2"}}]'

# Query aggregates (after window closes ~60s)
curl http://localhost:8083/api/v1/aggregates
curl http://localhost:8083/api/v1/aggregates/user.login

# Query alerts
curl http://localhost:8083/api/v1/alerts

# Open Grafana
open http://localhost:3000  # admin/admin
```

---

## Run Tests

```bash
# Unit tests — no Kafka broker needed (TopologyTestDriver)
mvn test -pl event-processor

# Consumer tests
mvn test -pl event-consumer

# All modules
mvn test
```

---

## Kafka Streams Topology Details

**Window**: Tumbling, 60 seconds, no grace period
**Suppression**: `untilWindowCloses` — output fires exactly once per window close
**Exactly-once**: `PROCESSING_GUARANTEE=exactly_once_v2` (requires Kafka 2.5+)
**State store**: `event-count-store` — queryable via Interactive Queries
**DLQ**: Events with null/blank type → `events-dlq` topic
**Alert threshold**: Configurable via `ALERT_THRESHOLD` env var (default: 100)

---

## Grafana Dashboard

Import `monitoring/grafana/dashboards/event-platform.json` or it auto-provisions.

Panels:
- Service availability (UP/DOWN)
- Events published/sec + failure rate
- Kafka publish latency p50/p95/p99
- Aggregates consumed/sec
- Alerts fired/sec
- Consumer errors/sec
- JVM heap usage per service

---

## AWS EKS Deployment

```bash
# Apply all manifests
kubectl apply -f k8s/deployment.yaml

# Check rollout
kubectl rollout status deployment/event-producer  -n event-platform
kubectl rollout status deployment/event-processor -n event-platform
kubectl rollout status deployment/event-consumer  -n event-platform
```
