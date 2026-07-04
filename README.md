# Order–Shipping–ARES Platform: Full Technical Documentation


**Stack:** Java 21+, Spring Boot 4.0.6, Apache Kafka, MySQL/TiDB Cloud, H2, Docker

---

## 1. What this system does

Three independently deployable Spring Boot services, wired together with Kafka + Docker Compose:

| Service | Port | Role |
|---|---|---|
| `order-service` | 8081 | Accepts an order, saves it, publishes it to Kafka |
| `shipping-service` | 8082 | Consumes orders from Kafka, saves a shipping record, marks orders shipped, publishes that fact back to Kafka. Also hosts the ARES **Resilience Engine** (chaos experiments) |
| `ares-service` | 8083 | Watches the health of the other two services + Docker + Kafka, stores snapshots, and calls an LLM (Groq) to explain incidents in plain English |

### 1.1 End-to-end flow

```
Client
  │  POST /api/orders/produce  { customerName, totalCost, address }
  ▼
order-service ──save──▶ MySQL/TiDB "orders" table
  │  produces JSON string
  ▼
Kafka topic: order_topic
  │  @KafkaListener consumes
  ▼
shipping-service ──save──▶ MySQL/TiDB "shipping" table
  │  (later) DELETE /api/shipping/{orderId}
  ▼
shipping-service ──delete row, produce Long orderId──▶ Kafka topic: shipped_order_topic
```

```
ares-service (loop, every 30s)
  ├─ Docker CLI          → `docker ps`
  ├─ Kafka AdminClient   → describeCluster()
  ├─ Actuator health     → GET order-service:8081/actuator/health, shipping-service:8082/...
  └─ JVM metrics         → CPU / memory / disk / thread count
  → merged into one snapshot, health state computed, saved to DB

ares-service (on demand)
  POST /api/operations/intelligence/analyze → Groq LLM → rootCause / severity / recommendation

shipping-service (on demand, chaos engineering)
  POST /api/ares/resilience/run → shells out to `docker stop/restart <container>`
```

---

## 2. Service-by-service breakdown

### 2.1 `order-service`
- `Order` entity: `id, customerName, totalCost, address, shipped`
- One controller method does two things with no link between them: save to DB, then publish to Kafka
- No DTO layer — the JPA entity is also the request body, response body, and Kafka payload

### 2.2 `shipping-service`
- `Shipping` entity duplicates `customerName/totalCost/address` but is a **separate, independently defined** entity from `Order`
- Kafka listener deserializes the incoming JSON string directly into `Shipping`, relying on matching field names — not a shared contract
- `DELETE /api/shipping/{orderId}` = "mark as shipped." It **deletes the row** (doesn't flip a status flag) and publishes the order ID to `shipped_order_topic`
- `GET /api/shipping` lists what's still pending (shipped rows no longer exist in this table)
- Hosts the **ARES Resilience Engine**: runs Docker-based chaos experiments (stop/restart a container, simulated latency/packet-loss/Kafka-outage/DB-outage) and logs every run to a `ares_resilience_experiments` table

### 2.3 `ares-service`
Built with **ports-and-adapters (hexagonal) architecture** — different style from `order`/`shipping`'s flat controller→service→repository shape.

**Observation module** (read-only platform telemetry):
- Domain objects (`ObservationSnapshot`, `ObservedComponent`, etc.) are immutable Java records
- Each data source (Docker, Kafka, Actuator, JVM, DB history) sits behind its own interface ("port"), with one real implementation ("adapter") per port
- `ObservationService` fans out to all ports, merges results, decides overall health (`DOWN` > `DEGRADED` > `UNKNOWN` > `UP`), and saves the snapshot
- Runs automatically every 30s (configurable) and is also callable on demand via REST

**Intelligence module** (AI-assisted analysis):
- `AIAnalysisPort` is provider-neutral; `GroqAdapter` is the only implementation today
- Builds a prompt from incident id + logs + metrics + health, asks the LLM for JSON-only output, parses it defensively (strips markdown fences, falls back gracefully on parse failure)
- Never throws — a missing API key or a failed call still returns a normal 200 response with `severity: UNKNOWN` and an explanation

---

## 3. Infrastructure & deployment

- `docker-compose.yml` chains: Zookeeper → Kafka → `order-service` → `shipping-service` → `ares-service`
- `order`/`shipping` point at a **shared external TiDB Cloud cluster** (two schemas: `orders`, `shipping`)
- `ares-service` defaults to **in-memory H2** — overridable via env var
- `ares-service` mounts the host's Docker socket so it can run `docker ps` from inside a container
- `shipping-service` does **not** get the same socket mount, even though its Resilience Engine also needs to run `docker stop/restart` — a mismatch (see §4.5)

---

## 4. Tradeoffs — what was chosen, why, and what it costs

Each item is a design choice that makes sense for a learning/portfolio project, paired with what it would cost in production. This is the section worth knowing cold for an interview.

### 4.1 Order saved to DB and published to Kafka in one method, no safety net
- **What:** `save()` then `send()`, sequentially, with no compensating logic if one succeeds and the other fails
- **Why it's fine here:** simplest possible flow, easy to build and demo
- **Cost at scale:** if the process crashes right after the DB commit but before Kafka receives the message, the order is saved but *never reaches shipping* — silently lost, no retry, no dead-letter
- **Production fix:** transactional outbox pattern, or CDC (e.g. Debezium) reading the `orders` table instead of an app-level Kafka call

### 4.2 `Order` and `Shipping` are two separate entities with no shared contract
- **What:** `shipping-service` deserializes the Kafka payload straight into its own `Shipping` class, trusting that field names happen to match
- **Why it's fine here:** zero shared library needed, each service stays fully independent
- **Cost at scale:** no compile-time or schema check — rename a field in `order-service` and `shipping-service` silently gets `null`, not a build error
- **Production fix:** shared DTO module, or Avro/Protobuf + a schema registry

### 4.3 "Shipped" = row deleted, not a status flag
- **What:** marking an order shipped deletes it from the `shipping` table instead of setting `shipping = true`
- **Why it's fine here:** makes "what's pending" a trivial query with no `WHERE` clause
- **Cost at scale:** kills the audit trail permanently — no way to ever query "orders shipped last week" from this table again
- **Production fix:** add a `status` enum + `shipped_at` timestamp column instead of deleting

### 4.4 Raw JSON/`Long` on Kafka, no schema registry, wildcard trusted packages
- **What:** `order_topic` carries a hand-built JSON string, `shipped_order_topic` carries a raw `Long`; consumer config trusts `*` packages for deserialization
- **Why it's fine here:** no Avro/Protobuf tooling needed, anyone can read the topic with a plain console consumer
- **Cost at scale:** no schema evolution safety net, and `trusted.packages=*` is flagged by most security scanners as a deserialization risk
- **Production fix:** Avro/Protobuf + schema registry, and scope trusted packages down to `com.example.kafka.*`

### 4.5 Hardcoded live database credentials — **highest priority fix**
- **What:** both `order` and `shipping` `application.properties` files hardcode the *real* TiDB username and password as the fallback value inside `${SPRING_DATASOURCE_PASSWORD:actualPasswordHere}`
- **Why it happened:** convenient for running locally without needing a `.env` file every time
- **Cost:** this is a genuine leaked secret the moment the repo is pushed anywhere shared or public
- **Fix, in order:**
  1. Rotate the TiDB password now — treat the committed one as burned
  2. Change the defaults to empty placeholders so the app fails loudly instead of silently using a real/dead credential
  3. Keep actual secrets only in `.env` (already used for containers) or a secrets manager
  4. Add secret-scanning or a pre-commit hook to prevent this happening again
- **Worth noting:** `ares-service` gets this right — its H2 default is harmless (throwaway in-memory DB) and its Groq key default is empty, which is the correct pattern

### 4.6 Two different architectural styles in one codebase
- **What:** `order`/`shipping` use a flat controller→service→repository shape; `ares-service` uses ports-and-adapters with separate domain/persistence models
- **Why it's fine here:** ARES's style makes swapping `GroqAdapter` for a different LLM provider trivial, and makes `ObservationService` fully unit-testable without Spring
- **Cost:** a reviewer has to learn two different conventions in the same repo — fine if framed as "ARES is the showcase-architecture module," confusing if left unexplained

### 4.7 Health checks are polled, not pushed
- **What:** ARES actively polls Docker/Kafka/Actuator every 30 seconds rather than services pushing events
- **Why it's fine here:** zero changes needed in `order`/`shipping` for ARES to observe them
- **Cost:** up to 30-second blind spots; a crash-and-restart inside one interval is invisible; health checks currently run one at a time, not in parallel, so this won't scale past a handful of services without changes
- **Production fix:** parallelize the checks (Java 21 virtual threads make this cheap), or move to push-based metrics via the Micrometer/Prometheus dependency already in the project but not yet wired up

### 4.8 AI analysis always returns 200, even on failure
- **What:** missing API key, network failure, or unparsable AI response all become a valid-looking result with `severity: UNKNOWN` instead of an HTTP error
- **Why it's fine here:** a UI can always render the response with no special-casing
- **Cost:** a careless caller could treat a "the AI is unavailable" result as if it were a real diagnosis
- **Fix:** add an explicit `providerAvailable: true/false` field so callers can't miss the difference

### 4.9 Chaos experiments have no allow-list, no auth
- **What:** the resilience endpoint takes an arbitrary `affectedService` string and runs `docker stop`/`docker restart` on it directly — no whitelist, no confirmation, no auth check
- **Why it's fine here:** clean port/adapter split means swapping in real latency-injection tooling (Toxiproxy, `tc netem`) later won't touch the controller at all
- **Cost:** on any shared or internal network, this endpoint could stop *any* container on the host Docker daemon, not just the intended targets
- **Fix:** allow-list of experiment-eligible containers, plus auth, before exposing this beyond localhost

### 4.10 No auth, no gateway, anywhere
- **What:** every REST endpoint across all three services is open, unauthenticated
- **Why it's fine here:** keeps the project scoped to demonstrating Kafka + observability + AI patterns, not auth infrastructure
- **Cost:** not deployable as-is without a gateway or Spring Security layer in front
- **Worth stating explicitly** in any writeup as "not done, by design, for scope" rather than letting it look like an oversight

---

## 5. Other gaps worth knowing

- `order`/`shipping` don't have `spring-boot-starter-actuator` in their `pom.xml` — meaning ARES's health polling against `/actuator/health` on those services will currently fail until it's added
- Docker socket is mounted for `ares-service` but not `shipping-service`, even though the Resilience Engine needing it lives in `shipping-service`
- No dead-letter topic or retry policy on the `shipping-service` Kafka consumer — a bad message on `order_topic` will loop by default instead of being quarantined
- `INJECT_LATENCY` / `SIMULATE_PACKET_LOSS` / `SIMULATE_KAFKA_FAILURE` / `SIMULATE_DATABASE_FAILURE` experiment types are simulated in name only right now — they return a canned string, they don't actually touch the network or DB

---

## 6. How to frame this in an interview

- **Order + Shipping** = core event-driven microservice fundamentals (Kafka producer/consumer, per-service DB ownership), deliberately kept simple to isolate the messaging pattern
- **ARES** = the "level-up" module — same platform, but built so Docker/Kafka/Actuator/the LLM provider are all swappable behind stable interfaces, adding observability + AI incident analysis + chaos engineering
- The known gaps (hardcoded creds, no outbox, no schema registry, no auth) make a stronger "what I'd do differently" answer than pretending the project is production-ready

---

## 7. Suggested next steps, in priority order

1. Rotate the TiDB credentials; replace hardcoded defaults with empty placeholders
2. Add `spring-boot-starter-actuator` to `order`/`shipping` so ARES's health polling works end-to-end
3. Replace delete-as-completion with a `status` enum on `shipping`
4. Introduce a shared schema/DTO module (or Avro + Schema Registry) between `order` and `shipping`
5. Parallelize ARES's Actuator health checks
6. Add an allow-list + auth to the Resilience Engine, and fix the Docker socket mount mismatch
7. Wire Micrometer → Prometheus → Grafana instead of only exposing metrics via REST/H2
