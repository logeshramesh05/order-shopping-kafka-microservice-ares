# Order–Shipping–ARES Platform: Full Technical Documentation


Detailed technical documentation for the Order–Shipping–ARES platform. For a quick start, see [`README.md`](./README.md).

## Table of Contents

1. [Overview](#1-overview)
2. [Architecture](#2-architecture)
3. [Services in Detail](#3-services-in-detail)
4. [Data Flow](#4-data-flow)
5. [API Reference](#5-api-reference)
6. [Configuration Reference](#6-configuration-reference)
7. [Deployment](#7-deployment)
8. [Adding OpenAPI/Swagger Docs](#8-adding-openapiswagger-docs)
9. [Testing](#9-testing)
10. [Design Decisions & Tradeoffs](#10-design-decisions--tradeoffs)
11. [Known Gaps / Roadmap](#11-known-gaps--roadmap)

---

## 1. Overview

This is a small, event-driven e-commerce backend made of independent Spring Boot microservices communicating over **Apache Kafka**, persisting to **MySQL**, and deployed together with **Docker Compose**.

| Service | Port | Responsibility |
|---|---|---|
| `order` | 8081 | Accepts new orders, saves them, publishes them to Kafka |
| `shipping` | 8082 | Consumes orders, tracks shipping status, marks orders shipped, exposes a chaos-engineering module |
| `ares-service` | 8083 | Observes the platform's health (Docker, Kafka, service health, host metrics) and provides AI-assisted incident analysis |

The goal of `ares-service` is to add **observability** and **resilience testing** on top of a normal microservice pipeline, without changing how `order` and `shipping` work.

---

## 2. Architecture

```
                 ┌─────────────┐        order_topic        ┌────────────────┐
   HTTP POST --> │ order-service│ ───────────────────────▶ │ shipping-service│
  /api/orders/   │  (8081)      │                            │  (8082)         │
   produce       │  MySQL:orders│                            │  MySQL:shipping │
                 └─────────────┘                            └────────┬────────┘
                                                                       │ shipped_order_topic
                                                                       ▼
                                                              (no consumer yet —
                                                               see Known Gaps)

                 ┌───────────────────────────────────────────────────────────┐
                 │                       ares-service (8083)                  │
                 │  Observation Engine        Intelligence Engine             │
                 │  - Docker CLI               - Groq (OpenAI-compatible) LLM │
                 │  - Kafka AdminClient        - Root-cause / severity output │
                 │  - Actuator health checks                                  │
                 │  - JVM/host metrics                                        │
                 │  - H2 (or MySQL) history                                   │
                 └───────────────────────────────────────────────────────────┘
                              ▲ periodically polls actuator/health of
                              └── order-service & shipping-service

                 ┌───────────────────────────────────────────────────────────┐
                 │   shipping-service :: ares.resilience package              │
                 │   Chaos experiments (stop/restart containers, simulate     │
                 │   latency / packet loss / Kafka or DB failure) via the     │
                 │   Docker CLI, with results persisted and queryable.        │
                 └───────────────────────────────────────────────────────────┘
```

**Kafka topics:**
- `order_topic` — produced by `order-service` on every new order; consumed by `shipping-service`.
- `shipped_order_topic` — produced by `shipping-service` when an order is marked shipped (no consumer currently implemented).

---

## 3. Services in Detail

### 3.1 `order` service
**Package:** `com.example.kafka.order`

| Class | Responsibility |
|---|---|
| `Order` | JPA entity (`orders` table): `id`, `customerName`, `totalCost`, `address`, `shipped` |
| `OrderRepository` | Spring Data JPA repository |
| `OrderProducer` | Serializes an `Order` to JSON, publishes to `order_topic` via `KafkaTemplate<String, String>` |
| `OrderController` | `POST /api/orders/produce` — saves the order, then publishes it |
| `AppConstants` | `ORDER_TOPIC = "order_topic"` |

### 3.2 `shipping` service
**Package:** `com.example.kafka.shipping`

| Class | Responsibility |
|---|---|
| `Shipping` | JPA entity (`shipping` table): `Id`, `customerName`, `totalCost`, `address`, `shipping` (boolean) |
| `ShippingRepository` | Spring Data JPA repository |
| `ShippingConsumer` | `@KafkaListener` on `order_topic`; deserializes and saves incoming orders |
| `ShippedOrderIdProducer` | Publishes a `Long` order ID to `shipped_order_topic` |
| `ShippingController` | `DELETE /api/shipping/{orderId}` (mark shipped) and `GET /api/shipping` (list pending) |
| `AppConstants` | `ORDER_TOPIC`, `SHIPPED_ORDER_TOPIC` |

#### 3.2.1 Embedded chaos-engineering module: `com.example.kafka.shipping.ares.resilience`

| Class | Responsibility |
|---|---|
| `ResilienceController` | `POST /run`, `POST /stop`, `GET /history` under `/api/ares/resilience` |
| `ResilienceEngineService` | Records an experiment, delegates execution to `DockerOperationsPort`, updates status |
| `LocalDockerOperationsPort` | Shells out to the Docker CLI: `STOP_CONTAINER` → `docker stop`, `RESTART_SERVICE` → `docker restart`. `INJECT_LATENCY`, `SIMULATE_PACKET_LOSS`, `SIMULATE_KAFKA_FAILURE`, `SIMULATE_DATABASE_FAILURE` are **simulated only** — they return a descriptive result without touching real infrastructure |
| `ResilienceExperimentRecord` | Entity (`ares_resilience_experiments` table) tracking each experiment |
| `ExperimentStatus` | Enum: `RUNNING`, `COMPLETED`, `FAILED`, `STOPPED` |

### 3.3 `ares-service`
**Package:** `com.example.ares` — split into two ports-and-adapters modules.

#### Observation module (`observation`)
Collects a point-in-time health/resource snapshot of the whole platform.

| Component | Purpose |
|---|---|
| `ObservationSnapshot`, `ObservedComponent`, `ResourceSample` | Domain records describing a snapshot |
| `DockerCliObservationAdapter` | Observes running containers via the Docker CLI |
| `KafkaAdminObservationAdapter` | Checks Kafka cluster/broker connectivity via `AdminClient` |
| `ActuatorHealthRestAdapter` | Polls `/actuator/health` on `order-service` and `shipping-service` |
| `JvmSystemMetricsAdapter` | Reads CPU/memory/disk/thread stats from the local JVM |
| `JpaObservationHistoryAdapter` | Persists/loads snapshot history (table `ares_observation_snapshots`) |
| `ObservationService` | Orchestrates collection, computes overall health (`DOWN` > `DEGRADED` > all-`UNKNOWN` > `UP`), runs on a schedule (default every 30s) |
| `ObservationController` | `POST /collect`, `GET /latest`, `GET /history?limit=` under `/api/operations/observations` |

#### Intelligence module (`intelligence`)
Sends incident context to an LLM and returns a structured root-cause analysis.

| Component | Purpose |
|---|---|
| `AIAnalysisRequest` / `AIAnalysisResult` | Domain records for the request/response |
| `AnalysisSeverity` | Enum: `LOW`, `MEDIUM`, `HIGH`, `CRITICAL`, `UNKNOWN` |
| `AIAnalysisPort` | Provider-neutral interface |
| `GroqAdapter` | Implementation against Groq's OpenAI-compatible `/chat/completions` API; parses JSON response into `rootCause`, `severity`, `businessImpact`, `recommendation`, `confidence`, `summary`; degrades gracefully if no API key or on failure |
| `IntelligenceController` | `POST /analyze` under `/api/operations/intelligence` |

#### Cross-cutting
- `ObservationProperties`, `IntelligenceProperties` — `@ConfigurationProperties` for `ares.observation.*` / `ares.intelligence.*`
- `RestClientConfiguration` — shared `RestTemplate` bean
- `GlobalExceptionHandler` — common REST error handling
- Exposes `/actuator/prometheus` via `micrometer-registry-prometheus`

---

## 4. Data Flow

1. `POST /api/orders/produce` → order saved to MySQL → published to `order_topic`.
2. `shipping-service` consumes from `order_topic` → saves a `Shipping` record.
3. `DELETE /api/shipping/{orderId}` → record removed from `shipping` table → order ID published to `shipped_order_topic` (currently unconsumed).
4. `ares-service` polls Docker, Kafka, and both services' `/actuator/health` every 30s (or on-demand), aggregates overall health, and stores the snapshot.
5. `POST /api/operations/intelligence/analyze` sends incident context (logs/metrics/health) to Groq and returns root cause, severity, business impact, and recommendation.
6. `POST /api/ares/resilience/run` (on `shipping-service`) starts a chaos experiment; results are queryable via `/history`.

---

## 5. API Reference

### order-service (`:8081`)
| Method | Path | Body | Description |
|---|---|---|---|
| POST | `/api/orders/produce` | `{customerName, totalCost, address, shipped}` | Create and publish an order |

### shipping-service (`:8082`)
| Method | Path | Description |
|---|---|---|
| GET | `/api/shipping` | List pending shipping records |
| DELETE | `/api/shipping/{orderId}` | Mark an order shipped |
| POST | `/api/ares/resilience/run` | Start a chaos experiment |
| POST | `/api/ares/resilience/stop` | Stop a chaos experiment |
| GET | `/api/ares/resilience/history` | List past experiments |

### ares-service (`:8083`)
| Method | Path | Description |
|---|---|---|
| POST | `/api/operations/observations/collect` | Force an observation snapshot |
| GET | `/api/operations/observations/latest` | Alias for collect |
| GET | `/api/operations/observations/history?limit=20` | Recent snapshots (1–100) |
| POST | `/api/operations/intelligence/analyze` | AI-assisted incident analysis |

### Actuator (all services)
`GET /actuator/health`, `/actuator/info`, `/actuator/metrics` (ares-service also exposes `/actuator/prometheus`).

---

## 6. Configuration Reference

| Variable | Service | Default | Purpose |
|---|---|---|---|
| `SERVER_PORT` | all | 8081/8082/8083 | HTTP port |
| `ORDER_SPRING_DATASOURCE_URL` | order | TiDB Cloud URL | MySQL connection |
| `SHIPPING_SPRING_DATASOURCE_URL` | shipping | TiDB Cloud URL | MySQL connection |
| `SPRING_DATASOURCE_USERNAME` / `PASSWORD` | order, shipping | sample creds | MySQL credentials — **replace in real use** |
| `SPRING_KAFKA_PRODUCER_BOOTSTRAP_SERVERS` | order | `localhost:9092` (`kafka:29092` in Docker) | Kafka producer endpoint |
| `SPRING_KAFKA_CONSUMER_BOOTSTRAP_SERVERS` | shipping | `localhost:9092` (`kafka:29092` in Docker) | Kafka consumer endpoint |
| `ARES_SPRING_DATASOURCE_URL` | ares-service | in-memory H2 | ares datastore |
| `ARES_OBSERVATION_COLLECTION_INTERVAL_MS` | ares-service | 30000 | Scheduled snapshot cadence |
| `ARES_OBSERVATION_DOCKER_ENABLED` | ares-service | true | Toggle Docker observation |
| `ARES_ORDER_ACTUATOR_HEALTH_URL` | ares-service | `http://order-service:8081/actuator/health` | Health polling target |
| `ARES_SHIPPING_ACTUATOR_HEALTH_URL` | ares-service | `http://shipping-service:8082/actuator/health` | Health polling target |
| `ARES_AI_PROVIDER` | ares-service | `groq` | AI provider selector |
| `GROQ_BASE_URL` / `GROQ_MODEL` / `GROQ_API_KEY` | ares-service | Groq endpoint / `openai/gpt-oss-120b` / (empty) | LLM connectivity |

> ⚠️ **Security note:** default DB credentials shipped in `application.properties` for `order` and `shipping` should be rotated and replaced with environment-variable-based secrets before any real deployment.

---

## 7. Deployment

### Full stack (recommended)
```bash
docker compose up --build
```
This starts `zookeeper`, `kafka`, `order-service`, `shipping-service`, and `ares-service` on a shared `kafka-network`. `ares-service` mounts `/var/run/docker.sock` so it can observe/control sibling containers.

Create a `.env` file first with at least:
```env
ORDER_SPRING_DATASOURCE_URL=jdbc:mysql://<host>:<port>/orders
SPRING_DATASOURCE_USERNAME=<db-user>
SPRING_DATASOURCE_PASSWORD=<db-password>
SHIPPING_SPRING_DATASOURCE_URL=jdbc:mysql://<host>:<port>/shipping
GROQ_API_KEY=<your-groq-api-key>
```

### Independent service
`order/` ships its own standalone `docker-compose.yml` (with a local MySQL container):
```bash
cd order && docker compose up --build
```
`shipping` and `ares-service` expect Kafka (and, for `ares-service`, the other two services) to already be reachable, so they're best run via the root compose file.

### Cloud deployment
Each service has its own multi-stage `Dockerfile` (Maven build → `eclipse-temurin:21-jre-jammy` runtime) and can be pushed independently to any container platform (ECS, Cloud Run, AKS/EKS/GKE). Swap Kafka/MySQL for managed equivalents (MSK/Confluent Cloud, RDS/TiDB Cloud) and pass the same environment variables.

---

## 8. Testing

| Service | Tests |
|---|---|
| `order` | `OrderApplicationTests` — Spring context load |
| `shipping` | `ShippingApplicationTests` — Spring context load |
| `ares-service` | Unit tests for `ObservationService`, `ObservationController`, `IntelligenceService`, `GroqAdapter`, `IntelligenceController`; `ObservationPersistenceIntegrationTest` (integration, H2/Testcontainers) |

---

## 9. Design Decisions & Tradeoffs

- **Kafka instead of direct REST calls between `order` and `shipping`** — decouples the services and makes messages durable if a consumer is temporarily down, at the cost of added infrastructure complexity.
- **Ports-and-adapters structure in `ares-service`** — each observation source (Docker, Kafka, Actuator, JVM metrics) is behind its own interface, making them independently testable and swappable, at the cost of more boilerplate than a single monolithic collector.
- **Simulated fault types in the resilience module** — `STOP_CONTAINER`/`RESTART_SERVICE` are real Docker operations, but latency/packet-loss/Kafka/DB-failure faults are recorded as simulated events rather than executed against real infrastructure. This is a deliberate safety tradeoff to keep chaos experiments low-risk while still exercising the operational workflow.
- **AI as an advisor, not an actor** — `GroqAdapter` returns analysis and recommendations only; nothing in the platform lets the AI take automated remediation actions. This trades speed for safety and human oversight.

---

## 10. Known Gaps / Roadmap

- No consumer exists yet for `shipped_order_topic` — the event is published but not acted on.
- Simulated (non-executed) chaos-fault types listed above.
- Hard-coded default DB credentials in `order`/`shipping` `application.properties` — replace before production use.
- `ares-service` defaults to in-memory H2, so observation history doesn't persist across restarts unless pointed at MySQL.
- The root `docker-compose.yml` grants `ares-service` access to the host's Docker socket — appropriate for a lab/demo, but a significant privilege in production; consider a scoped alternative (e.g., a sidecar with restricted permissions) for real deployments.
